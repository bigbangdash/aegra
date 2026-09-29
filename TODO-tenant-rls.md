# Tenant RLS — 引き継ぎメモ (2026-09-29)

ブランチ `feat/tenant-rls-poc`。フラグ `AEGRA_TENANT_RLS_ENABLED`。

## 決定事項（再確認不要）

- 目的: 監査向けに DB レベルのテナント分離を説明できること。脅威モデルは「アプリのバグによるテナント越境」（侵害されたプロセスは対象外）
- 対象: メタデータテーブル + checkpoints/store (LangGraph pool) + Redis。新規デプロイ前提なので `tenant_id NOT NULL`、backfill なし
- テナント約100、1ユーザー = 1テナント。`tenant_id` は JWT の `org_id` から取り、テナントレジストリで照合
- fail-closed: DB スコープ未宣言はエラー。バイパスは `system_scope(reason)` のみ
- assistants: テナント行 + 共有 system 行（`tenant_id NULL AND user_id='system'`、読み取り専用、CHECK 制約）
- Redis: ElastiCache Serverless。ACL ユーザーではなく、テナントごとのデータ鍵を1つの KMS 鍵で包む（AAD に tenant_id）。RBAC はユーザーグループあたり100ユーザー上限のため
- Phase 5 の決定（2026-09-29）: Redis で平文に残すのは `event_id` と end フラグだけ（イベントの種類も暗号文に入れる）／dev と E2E の鍵は環境変数のマスター鍵から HKDF で派生（StaticKeyProvider）。KMS 版は別パッケージ／org_id の形式は `^[A-Za-z0-9_-]{1,64}$` で仮置き（IdP の実際の形式が分かったら直す）
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
- [x] **prod モード（Redis worker）で `test_tenant_rls_api_e2e.py` を実行** → 9 passed（dev も 9 passed）
- [x] **E2E 全体をフラグ OFF のベースラインと比較**（2026-09-29、dev/prod とも。manual_auth_tests・multi_instance・test_tenant_rls は除外）

  | スタック | dev 失敗/総数 | prod 失敗/総数 |
  |---|---|---|
  | 標準 compose（noop auth）・フラグ OFF | 23 / 141 | 26 / 158 |
  | tenant-rls compose（header auth）・フラグ OFF | 24 / 141 | 35 / 158 |
  | tenant-rls compose（header auth）・フラグ ON | 24 / 141 | 37 / 158 |

  - noop auth の失敗はすべて LLM を呼ぶグラフのもの（OpenAI 401。API キー無し）で、どの組み合わせでも同じ
  - noop auth → header auth で増えた分は auth の違いによるもので、RLS とは関係ない:
    - `test_store::test_org_prefix_without_org_membership_is_forbidden`（DID NOT RAISE）: header auth が匿名にも `org_id="e2e-tenant"` を付ける。推測どおり
    - `test_run_reconciliation_e2e` 8件（prod のみ。404 / Timeout）: テストが DB に `user_id="anonymous"` の行を直接入れるが、header auth の identity は `e2e-user`
  - フラグ OFF → ON で増えたのは prod の2件だけ（`test_retry_exhaustion_marks_run_and_thread_error`、`test_worker_timeout_cannot_overwrite_reconciled_interruption`）。テストが `tenant_id` なしで assistant を直接 INSERT し、CHECK 制約 `aegra_tenant_shared_rows_are_system` に弾かれた＝意図どおり処理を止める側に倒れた動き
  - dev・prod とも、フラグ ON のサーバーログに `DbScopeMissingError` と RLS の権限エラーは0件
  - 必要なら別タスク: `test_run_reconciliation_e2e` の `_seed_run` を、フラグ ON のとき `tenant_id` を付けて tenant header に合わせる（今は RLS スタックで回すことを想定していないテスト）
- [x] `test_assistant_large_config_db` は環境の問題で確定。5434 に向けて `alembic upgrade head` 後に実行 → 1 passed

### Phase 5: Redis 暗号化とテナントレジストリ
- [x] テナントごとのデータ鍵によるエンベロープ暗号化（2026-09-29。KMS 版の鍵の提供元は別パッケージで、未実装）
  - `core/tenant_crypto.py`: AES-256-GCM、AAD = `tenant_id|run_id|event_id|key_version`、形式 `v1.<version>.<b64(nonce+ct)>`。`TenantKeyProvider` Protocol ＋ `configure_key_provider()`（KMS 版の差し込み口）、既定は `StaticKeyProvider`（`AEGRA_TENANT_REDIS_MASTER_KEY` から HKDF で派生）
  - `RedisRunBroker`: フラグ ON のとき、Redis の message は `{event_id, end, sealed}` だけ（cache と pub/sub の両方）。復号は読む側の `current_db_scope()` のテナントで行う。system スコープ・スコープ未宣言・未暗号化のメッセージは例外にする。フラグ OFF のときは従来の形式のまま
  - cancel を受け取る側（スコープ無し）は `core/active_runs.active_run_tenants`（`execute_run` が登録・削除）を見て、その run 自身のテナントで end を書く。見つからなければ警告ログを出して書かない（run 自身の終了処理が end を出す）
    - `Task.get_context()` は使えない: worker は子タスクで実行し、外側のタスクは system スコープのため
  - 起動時: フラグ ON かつ Redis broker のときに鍵が無ければ起動しない。`cryptography` を直接依存に追加
  - `docker-compose.tenant-rls.yml` にテスト専用の固定鍵（公開文字列の base64）
  - 検証: unit 1884 passed（新規: `test_tenant_crypto.py`、`test_redis_broker_tenant_encryption.py`、run_executor の登録テスト）。E2E は暗号化を入れる前と差分ゼロ（prod-on 37/158、prod-off 35/158、dev-on 24/141）。prod の `test_tenant_rls_api_e2e.py` は 10 passed（新規 `test_redis_event_buffer_holds_only_sealed_payloads`: Redis に平文が無いこと・持ち主は再送を受け取れること・別テナントは 404）
  - 注意: 完了した run の `/stream` は Last-Event-ID が無いと end しか返さない（再送させるには存在しない ID を渡す）
- [ ] テナントレジストリ（`org_id` を照合。現状 `core/tenant.py::resolve_tenant_id` は `org_id` の有無しか見ていない）
- [x] tenant_id の形式検証（レジストリ側での検証は、レジストリを作るときに同じ `is_valid_tenant_id` を使う）
  - 実装: `db_scope.tenant_scope()` が `^[A-Za-z0-9_-]{1,64}$` 以外を ValueError、`resolve_tenant_id` は 403。cron は形式に合わない tenant_id の行だけを飛ばしてエラーログを出す（以前は1件でバッチ全体が止まった。外部レビューの指摘）
  - 同じ回に外部レビューの指摘で直したもの: `open_sealed` が 12 バイト以下の本体を `TenantPayloadError` にする（以前は `ValueError`）、`configure_key_provider()` は import 時に呼ぶとコメントに明記
  - 検証（2026-09-29、Docker スタック）: unit 1900 passed。E2E は前回と同じ件数（prod-on 37/158、dev-on 24/141、prod-off 35/158）。失敗の内訳も前回と同じ種類だけ（LLM・auth の違い・CHECK 制約）。`test_tenant_rls_api_e2e.py` は prod 10 passed、dev 9 passed（1件は prod_only で対象外）。3つのサーバーログとも `DbScopeMissingError`・`TenantPayloadError`・形式エラーは0件

### 未検証の前提（2026-09-30 に設計書を書いていて判明）
- [x] superuser ではないテーブル所有者でログインする構成での E2E（2026-09-30 に確認済み）
  - 構成: `aegra_app`（NOSUPERUSER・NOBYPASSRLS）で起動し、テーブルは `aegra_app` の所有。enable は superuser で `--app-role aegra_app`
  - 結果: E2E 37/158（superuser のときと同じ内訳。権限エラーは0）、`test_tenant_rls_api_e2e.py` 10 passed。直接確認 8/8（所有者は13社分が見える、テナント用のロールでテナント未指定なら0件、cron が自動で run を作成、lease reaper が期限切れの run を回収、TTL の掃除がスレッドを削除）。サーバーログのスコープ・暗号・reaper・cron・TTL のエラーは0
  - 以下は確認前に書いたメモ:system スコープはロールを切り替えず、「所有者には RLS がかからない（FORCE なし）」ことに頼っている。E2E の `user` は superuser なので RLS を常にすり抜けており、この経路は未検証。所有者でも BYPASSRLS でもないロールでは、system スコープの処理（reaper・TTL・cron の claim）から行が見えなくなる

### Phase 6: ドキュメントと CLI
- [x] CLI enable コマンド: `aegra db enable-tenant-rls [--app-role ROLE]`（`libs/aegra-cli/src/aegra_cli/commands/db.py`）
  - `AEGRA_TENANT_RLS_ENABLED` が true でなければ接続する前にエラー（DB だけ RLS にするとフラグ OFF のサーバーの書き込みが全部失敗するため）。既定では接続中のロールにテナント用のロールを付与。テーブルが無ければ「先にサーバーを一度起動」と表示。何度流しても同じ結果
  - 検証: CLI unit 198 passed（新規 `tests/test_db_tenant_rls.py` 4件）。E2E の enable 手順を CLI に置き換えて prod-on を実行 → `Tenant RLS enabled. Tenant role aegra_tenant granted to user.`、E2E 37/158（前回と同じ内訳）、`test_tenant_rls_api_e2e.py` 10 passed
- [x] docs、両 `.env.example` への `AEGRA_TENANT_*` 追加: 新しいガイド `docs/guides/tenant-isolation.mdx`（`docs.json` の Configuration に追加）、`docs/reference/environment-variables.mdx` に表、`docs/guides/deployment.mdx` にコマンド1行。両方の `.env.example` はコメントアウトで3変数（中身は揃っている）
- [x] 運用上の注意の明文化: `tenant-isolation.mdx` の Setup と Operational notes に記載（起動してから enable、semantic search を後から有効にしたらやり直し、有効化後にフラグを OFF に戻さない、切り替えるときの 10 分の待ち、鍵の提供元は import 時に設定、cron の扱い）
  - フラグを切り替えるときは、実行中の run が0件になってから 600 秒（再送用バッファの保持時間）待つ。切り替えより前に書かれたイベントは、フラグ ON なら「未暗号化」、OFF なら「暗号化済み」として `TenantPayloadError` になり、ストリームが止まる（漏れる向きではなく止まる向き）。移行期間だけ平文を受け入れる案は採らない（新規デプロイ前提のため）
  - KMS 版の鍵の提供元は import 時に `configure_key_provider()` する（Aegra の lifespan が利用側の lifespan より先に鍵を確かめるため）

## 環境メモ（重要）

- aegra リポジトリに `.env` を置かない（ホスト側 pytest が他プロジェクトの 5432 に向かう）。compose 起動時だけ `.env.example` をコピーし、`up` 直後に削除する
- 5432/6379 の他プロジェクトのコンテナ（`prj-jfee-*`、`m10-*`）には触らない
- DB に触る E2E は必ず env で向き先を指定する:
  `POSTGRES_HOST=localhost POSTGRES_PORT=5434 POSTGRES_USER=user POSTGRES_PASSWORD=password POSTGRES_DB=aegra`
- RLS スタック起動:
  `AEGRA_TENANT_RLS_ENABLED=true POSTGRES_PORT=5434 docker compose -f docker-compose.yml -f docker-compose.dev.yml -f docker-compose.tenant-rls.yml up -d`
  （prod モードは `docker-compose.dev.yml` を外す）。起動後に下の CLI で enable を実行
- RLS の有効化（スタック起動後）: `AEGRA_TENANT_RLS_ENABLED=true POSTGRES_HOST=localhost POSTGRES_PORT=5434 POSTGRES_USER=user POSTGRES_PASSWORD=password POSTGRES_DB=aegra uv run --package aegra-cli aegra db enable-tenant-rls`
- サーバー E2E: `AEGRA_E2E_TENANT_RLS=1 uv run --package aegra-api pytest libs/aegra-api/tests/e2e/test_tenant_rls/ -v`
- 実 DB 単体 E2E 用の使い捨てコンテナ: `rls-probe`（pgvector/pgvector:pg18、55432、postgres/postgres）
