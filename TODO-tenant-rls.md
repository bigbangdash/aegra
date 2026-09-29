# Tenant RLS — 引き継ぎメモ (2026-09-29)

ブランチ `feat/tenant-rls-poc`。フラグ `AEGRA_TENANT_RLS_ENABLED`。

## 決定事項（再確認不要）

- 目的: 監査向けに DB レベルのテナント分離を説明できること。脅威モデルは「アプリのバグによるテナント越境」（侵害されたプロセスは対象外）
- 対象: メタデータテーブル + checkpoints/store (LangGraph pool) + Redis。新規デプロイ前提なので `tenant_id NOT NULL`、backfill なし
- テナント約100、1ユーザー = 1テナント。`tenant_id` は JWT の `org_id` から取り、テナントレジストリで照合
- fail-closed: DB スコープ未宣言はエラー。バイパスは `system_scope(reason)` のみ
- assistants: テナント行 + 共有 system 行（`tenant_id NULL AND user_id='system'`、読み取り専用、CHECK 制約）
- Redis: ElastiCache Serverless。ACL ユーザーではなく、テナントごとのデータ鍵を1つの KMS 鍵で包む（AAD に tenant_id）。RBAC はユーザーグループあたり100ユーザー上限のため
- 未決: upstream に出すか（案: 汎用フックは upstream、KMS/レジストリは別パッケージ）

## 完了

- Phase 1〜3: RLS ポリシー、スコープ機構、全経路のスコープ分類、assistants の分離
- Phase 4（今回）: `core/tenant_store.py` が store の namespace に `("aegra_tenant", tenant_id)` を透過的に前置し、読み出し時に剥がす
  - 理由: `store` の主キー `(prefix, key)` がテナント共通なので、同じ namespace/key を使うと他テナントの不可視行と衝突し書き込みが失敗、キーの存在も漏れる
  - HTTP API とグラフは同じ store インスタンスなので両方に効く
  - sync ノードの `store.put/get` は `batch()` でスコープを event loop 側へ引き継ぐ（`db_scope.bind_scope`）
  - system scope はパススルー（物理レイアウトが見える）
  - `store_vectors`（semantic search 有効時のみ存在）も、存在すれば RLS 対象（`OPTIONAL_TENANT_TABLES`）
  - E2E 用プローブグラフ `examples/tenant_store_probe/`（`aegra.tenant-rls.json` に登録済み）

## 検証状況（Phase 4）

- unit: `tests/unit/test_core` 317 passed。unit+integration 全体で失敗1件（`test_assistant_large_config_db`、下記）
- 実 DB E2E `test_langgraph_tenant_rls.py`: 15 passed（rls-probe, 55432）
- サーバー E2E `test_tenant_rls_api_e2e.py`（dev モード、フラグ ON）: 9 passed
- 物理行が `aegra_tenant.<tenant>.…` で `tenant_id` 付きになることを確認済み

## 残タスク

### Phase 4 の仕上げ
- [ ] **prod モード（Redis worker）で `test_tenant_rls_api_e2e.py` を実行**（未実施）
- [ ] **E2E 全体をフラグ OFF のベースラインと比較**（dev/prod とも）。途中まで dev・フラグ ON で流した結果:
  - 失敗の大半は `AuthenticationError: execution failed`（LLM API キー無し）。ベースラインにも出るはずだが未確認
  - `test_store.py::test_org_prefix_without_org_membership_is_forbidden` が DID NOT RAISE。tenant header auth が匿名ユーザーにも `org_id` を付けているのが原因と推測（未確認）。フラグ OFF でも同じなら RLS 起因ではない
- [ ] `test_assistant_large_config_db` は既定の localhost:5432（他プロジェクトの DB）に向かって `database "aegra" does not exist` で失敗。環境問題で今回の変更とは無関係

### Phase 5: Redis 暗号化とテナントレジストリ
- [ ] テナントごとのデータ鍵によるエンベロープ暗号化（KMS 鍵1つ、AAD に tenant_id）
- [ ] テナントレジストリ（`org_id` を照合。現状 `core/tenant.py::resolve_tenant_id` は `org_id` の有無しか見ていない）
- [ ] tenant_id の形式検証をレジストリ側でも行う（store 側は `.` を拒否している）

### Phase 6: ドキュメントと CLI
- [ ] CLI enable コマンド（現状は `aegra_api.core.tenant_rls.enable_tenant_rls` を手で呼ぶ。初回起動後に一度実行、要 autocommit 接続）
- [ ] docs、両 `.env.example` への `AEGRA_TENANT_*` 追加（CLAUDE.md のルール）
- [ ] 運用上の注意の明文化: RLS 有効化は LangGraph の `setup()` 後、semantic search を後から有効にしたら enable を再実行（`store_vectors`）

## 環境メモ（重要）

- aegra リポジトリに `.env` を置かない（ホスト側 pytest が他プロジェクトの 5432 に向かう）。compose 起動時だけ `.env.example` をコピーし、`up` 直後に削除する
- 5432/6379 の他プロジェクトのコンテナ（`prj-jfee-*`、`m10-*`）には触らない
- DB に触る E2E は必ず env で向き先を指定する:
  `POSTGRES_HOST=localhost POSTGRES_PORT=5434 POSTGRES_USER=user POSTGRES_PASSWORD=password POSTGRES_DB=aegra`
- RLS スタック起動:
  `AEGRA_TENANT_RLS_ENABLED=true POSTGRES_PORT=5434 docker compose -f docker-compose.yml -f docker-compose.dev.yml -f docker-compose.tenant-rls.yml up -d`
  （prod モードは `docker-compose.dev.yml` を外す）。起動後に enable を実行
- サーバー E2E: `AEGRA_E2E_TENANT_RLS=1 uv run --package aegra-api pytest libs/aegra-api/tests/e2e/test_tenant_rls/ -v`
- 実 DB 単体 E2E 用の使い捨てコンテナ: `rls-probe`（pgvector/pgvector:pg18、55432、postgres/postgres）
