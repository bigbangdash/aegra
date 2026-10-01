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
- 決定（2026-09-30）: テナントレジストリと KMS 版の鍵は aegra 本体に入れず、aegra を動かす側のプロジェクト（auth handler と同じ場所）で `configure_tenant_resolver()`・`configure_key_provider()` に差し込む

## 完了

- Phase 1〜3: RLS ポリシー、スコープ機構、全経路のスコープ分類、assistants の分離
- Phase 4（今回）: `core/tenancy/store.py` が store の namespace に `("aegra_tenant", tenant_id)` を透過的に前置し、読み出し時に剥がす
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
  - `core/tenancy/crypto.py`: AES-256-GCM、AAD = `tenant_id|run_id|event_id|key_version`、形式 `v1.<version>.<b64(nonce+ct)>`。`TenantKeyProvider` Protocol ＋ `configure_key_provider()`（KMS 版の差し込み口）、既定は `StaticKeyProvider`（`AEGRA_TENANT_REDIS_MASTER_KEY` から HKDF で派生）
  - `RedisRunBroker`: フラグ ON のとき、Redis の message は `{event_id, end, sealed}` だけ（cache と pub/sub の両方）。復号は読む側の `current_db_scope()` のテナントで行う。system スコープ・スコープ未宣言・未暗号化のメッセージは例外にする。フラグ OFF のときは従来の形式のまま
  - cancel を受け取る側（スコープ無し）は `core/active_runs.active_run_tenants`（`execute_run` が登録・削除）を見て、その run 自身のテナントで end を書く。見つからなければ警告ログを出して書かない（run 自身の終了処理が end を出す）
    - `Task.get_context()` は使えない: worker は子タスクで実行し、外側のタスクは system スコープのため
  - 起動時: フラグ ON かつ Redis broker のときに鍵が無ければ起動しない。`cryptography` を直接依存に追加
  - `docker-compose.tenant-rls.yml` にテスト専用の固定鍵（公開文字列の base64）
  - 検証: unit 1884 passed（新規: `test_tenancy/test_crypto.py`、`test_redis_broker_tenant_encryption.py`、run_executor の登録テスト）。E2E は暗号化を入れる前と差分ゼロ（prod-on 37/158、prod-off 35/158、dev-on 24/141）。prod の `test_tenant_rls_api_e2e.py` は 10 passed（新規 `test_redis_event_buffer_holds_only_sealed_payloads`: Redis に平文が無いこと・持ち主は再送を受け取れること・別テナントは 404）
  - 注意: 完了した run の `/stream` は Last-Event-ID が無いと end しか返さない（再送させるには存在しない ID を渡す）
- [x] テナントレジストリの差し込み口（2026-09-30、ラルフループ項目4）: `configure_tenant_resolver()`。レジストリの本体は別パッケージ（下の「本番化の残タスク」）
- [x] tenant_id の形式検証（レジストリ側での検証は、レジストリを作るときに同じ `is_valid_tenant_id` を使う）
  - 実装: `tenancy.scope.tenant_scope()` が `^[A-Za-z0-9_-]{1,64}$` 以外を ValueError、`resolve_tenant_id` は 403。cron は形式に合わない tenant_id の行だけを飛ばしてエラーログを出す（以前は1件でバッチ全体が止まった。外部レビューの指摘）
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

### ラルフループ（`RALPH-tenant-rls.md`、2026-09-30〜）
- [x] 1. E2E ノイズ整理: `tests/e2e/_utils.py` に `on_tenant_rls_stack()`（`AEGRA_E2E_TENANT_RLS=1` = RLS compose スタック、フラグは問わない）と `e2e_owner()`（RLS スタックなら `("e2e-user", "e2e-tenant")`、それ以外は `("anonymous", None)`）。`test_run_reconciliation_e2e._seed_run` と `finalize_run` の user_id/tenant_id をこれに合わせた。`test_store::test_org_prefix_without_org_membership_is_forbidden` は RLS スタックで skip
  - 検証（prod・フラグ ON・CLI enable 済み、manual_auth_tests・multi_instance 除外、test_tenant_rls 含む）: 25 failed / 168 passed / 5 skipped。失敗 25 件はすべて LLM グラフ（OpenAI 401。cancel 系2件も `agent` グラフが 401 で先に error になるもの）。以前 ON で増えていた2件と reconciliation 8件は通過。サーバーログの scope・暗号・RLS エラー 0
- [x] 2. schema 対応（#327 と両立）: `enable_tenant_rls(..., schema=None) -> str`。schema 未指定なら DDL 接続の `current_schema()`。全テーブルを `"schema"."table"` で扱い（子テーブルのポリシーの親参照も）、`GRANT USAGE ON SCHEMA` をテナント用ロールに付与。optional テーブルの有無は `pg_tables`（schema 指定）で判定。CLI に `--schema`、完了メッセージに対象 schema を表示
  - 検証: unit `test_tenancy/test_rls.py` 16 passed（新規3: 接続の schema を使う／明示指定が優先／親テーブルの修飾）、CLI 199 passed（新規 `--schema`）。実 DB E2E `test_langgraph_tenant_rls.py` を `public`/`aegra_custom`（DSN の search_path）でパラメタ化 → metadata と合わせて 45 passed。USAGE 付与を外すと custom 側 10 件が失敗することを確認（テストが効いている）。integration は RLS 未適用の DB（5434 の `aegra_plain`、alembic head）で 452 passed（`test_thread_metadata_merge_db` も実行された）。RLS 済みの `aegra` DB では 7 件が NOT NULL で落ちる＝環境の問題。サーバー E2E `test_tenant_rls_api_e2e.py` 10 passed（CLI enable を再実行後）。lint OK、type-check 57 件は HEAD と同数（増加 0）
  - 環境メモ追記: ホスト側 E2E の Redis は `REDIS_URL=redis://localhost:6380/0`（`REDIS_PORT` は無い）
- [x] 3. FORCE RLS ＋ system の明示化: 全テナント表（通常・共有・子）に `FORCE ROW LEVEL SECURITY` と `aegra_system_access` ポリシー（`TO <ログインロール>`、`current_setting('aegra.system', true) = 'on'`）。system_scope は GUC `aegra.system` を立てる（SQLAlchemy は after_begin でトランザクション内、LangGraph pool は checkout 中だけセッション単位で立てて返却時に消す。setup() の CONCURRENTLY があるのでトランザクションにしない）。テナント checkout も念のため local で消す。alembic のマイグレーション接続でも立てる（データ移行が 0 行にならないように）。BYPASSRLS ロール案は不採用（理由は RALPH のメモ）
  - 検証: unit/integration 2361 passed（新規: FORCE とポリシーの対象ロール、pool/session の GUC、後始末失敗でも checkout は成功）、CLI 199 passed、lint OK、type-check 57（HEAD と同数）。実 DB E2E 52 passed（新規 `test_force_rls_owner_e2e.py` 7件: 非 superuser 所有者で、スコープ無しの生接続は 0 行、system は両経路で全テナント読み書き、テナントは自分だけ、漏れた system フラグでもテナントは広がらない、プール返却で消える、FORCE 後も setup() が動く）。FORCE を外すと「0 行」テストが落ちることを確認
  - サーバー（prod、`aegra_app` = NOSUPERUSER・NOBYPASSRLS・所有者でログイン、superuser から `--app-role aegra_app` で enable、FORCE 後に再起動）: E2E 25 failed / 190 passed / 5 skipped。失敗は項目1と完全に同じ 25 件（LLM）。サーバーログのスコープ・暗号・権限・制約エラー 0。lease reaper が回収、cron が発火（run は `tenant_id=tenant-sys`、next_run 前進）、TTL 掃除が削除（`CRON_POLL_INTERVAL_SECONDS=5`、`sweep_interval_minutes=0.1` で直接確認）。生きている DB で `aegra_app` がスコープ無しで見える件数: thread/runs/checkpoints 0、assistant 9（共有 system 行、設計どおり）
  - docs: `tenant-isolation.mdx` の Note と enable 手順（`--schema` も）、arch §1・§4・§5・§9・§10・§11
- [x] 4. tenant resolver の口: `core/tenancy/resolver.py` に `configure_tenant_resolver(async fn)`・`TenantRejectedError`・既定の `org_id_tenant_resolver`。`resolve_tenant_id` は async になり、結果の形式を中央で検証。tenant を決めるのは入口3か所だけ（HTTP dependency・`execute_run`・cron 発火）。`tenant_id_for` は廃止し、`threads.py`・`crons.py`・`assistant_service.py`・`run_preparation.py` は `scoped_tenant_id()`（今のスコープ）を読む。拒否時: HTTP 403／キュー済み run は system スコープで `error` に確定（stream の signal はテナント鍵が要るので出さない、done キーと cleanup はする）／cron はその回だけ飛ばして next_run を進め、有効のまま（resolver が別テナントを返した場合も飛ばす）
  - 検証: unit/integration 2374 passed（新規: resolver の差し替え・拒否・出力検証・None で既定に戻る・scoped_tenant_id・403 変換、execute_run の拒否で run を error にし実行しない・resolver で決まるテナント、cron の拒否／別テナント／受理）。lint OK、type-check 57（HEAD と同数）
  - サーバー（`aegra_app` 所有者ログイン、FORCE 済み）: `examples/tenant_header_auth_example.py` に「`e2e-inactive` を拒否する」仮レジストリを入れ、E2E `test_tenant_rejected_by_the_configured_resolver_gets_403` を追加 → テナント RLS E2E 63 passed。全体 E2E は LLM の 25 件のみ失敗（項目1と同じ集合）。途中 1 件（`test_thread_ttl` keep_latest）が落ちたのは確認用 override に残した 6 秒ごとの TTL 掃除のせいで、外すと 2 passed
  - docs: `tenant-isolation.mdx` に resolver の設定例と拒否時の振る舞い、arch §3（入口の表・scoped_tenant_id・フック）と §9（レジストリ本体は別パッケージ）
- [x] 5. checkpoint 性能計測: `scripts/bench_tenant_rls_checkpoint.py`（使い捨て DB を OFF/ON で作り、非 superuser 所有者でログイン、背景 100 テナント × 2000 checkpoint = 20 万行、ON は FORCE 済み。`--variants` で対策候補も測る）
  - 結果（5434 のローカル Docker、400 回、ms、p50 / p95、括弧は p95 の ON/OFF 比）:

    | 操作 | OFF | ON | ON＋1文化 |
    |---|---|---|---|
    | checkpoint aput | 0.44 / 0.58 | 1.21 / 1.81 (3.1x) | 0.89 / 1.12 (1.9x) |
    | aget_tuple（最新、100 件のスレッド） | 0.69 / 0.79 | 2.06 / 2.68 (3.4x) | 2.11 / 2.47 (3.1x) |
    | aget_tuple（最新、2000 件のスレッド） | 0.83 / 1.22 | 1.75 / 1.95 (1.6x) | 1.34 / 1.71 (1.4x) |
    | alist（スレッド、10 件） | 1.43 / 2.48 | 3.38 / 5.12 (2.1x) | 2.75 / 4.40 (1.8x) |
    | alist（スレッド指定なし、10 件） | 29.74 / 34.04 | 4.72 / 8.54 (0.25x) | 3.62 / 4.29 (0.13x) |
    | store aput | 0.48 / 0.82 | 1.97 / 2.93 (3.6x) | 1.42 / 2.08 (2.5x) |
    | store aget | 0.87 / 1.59 | 2.82 / 3.25 (2.0x) | 2.10 / 3.98 (2.5x) |

  - 原因: 増えるのは checkout ごとの固定の往復（BEGIN・SET LOCAL ROLE・set_config×2・COMMIT の 5 回）で、1 操作あたり +0.5〜1.5 ms。小さい操作ほど比が大きく見える。絶対値は p95 で数 ms 以内で、LLM 呼び出し（数百 ms〜秒）に比べ小さい。本番（RDS、往復 0.5〜1 ms）では往復の回数がそのまま効く
  - EXPLAIN: 短いスレッド（〜500 行）では `thread_id` と `tenant_id` の BitmapAnd＋ソートになるが、2000 行のスレッドでは主キーの逆順スキャン（1 行）に切り替わる＝スレッドが長くなっても悪化しない。スレッド指定なしの一覧は `idx_checkpoints_tenant_id` で絞れるので OFF より速い
  - 対策: (1) ロールと設定を 1 文に（`set_config('role', …, true)`、PostgREST と同じ）→ 往復 5→3、p50 −0.3〜0.8 ms → 項目 5b で実装。(2) 複合インデックス `(tenant_id, thread_id, checkpoint_ns, checkpoint_id)` は短いスレッドの計画が良くなるだけで効果が小さく、書き込みのコストが増えるので採らない
- [x] 5b. スコープ適用を1往復に: LangGraph pool のテナント checkout と SQLAlchemy の after_begin を `SELECT set_config('role', …, true), set_config('aegra.tenant_id', …, true), set_config('aegra.system', '', true)` の1文に（ロール名はバインド値になり、識別子のクォートが不要）。ベンチの `one-statement` 変種は削除
  - 検証: unit/integration 2374 passed（pool/session のテストを1文・バインド値に更新）、CLI 199、lint OK、type-check 57。実 DB E2E 52 passed、テナント checkout の `current_user` が `aegra_tenant`・`is_superuser=off` になることを確認。サーバー: テナント RLS E2E 63 passed、全体 E2E は LLM の 25 件のみ（集合は前回と同じ）、ログのエラー 0
  - ベンチ（p50、前回 ON → 今回 ON）: aput 1.21→0.92、alist(スレッド) 3.38→2.70、store aput 1.97→1.38、store aget 2.82→2.02 ms。aget_tuple（短いスレッド）は 2.06→2.12 で変わらず（計画のソートが支配的）
- [x] 6. 既存環境の移行: `enable_tenant_rls(..., assign_existing_to=None)` と CLI `--assign-existing-to TENANT`。最初に未割り当て行（列がまだ無ければ全行、共有 system assistant は除く）を表ごとに数え、オプション無しなら何も変えずに `UntaggedRowsError`（CLI は件数とオプションを表示）。オプション有りなら1トランザクションで列追加→UPDATE→store は `aegra_tenant.<T>.` の下へコピー・`store_vectors` の prefix 付け替え（FK に ON UPDATE が無い）・旧行削除、の後にいつもの DDL。DDL 接続でも `aegra.system` を立てる（FORCE 後に所有者で再実行しても数え漏れない）。テナント ID の形式は接続前に検証
  - 検証: unit `test_tenancy/test_rls.py` 26 passed（新規6: 拒否時に変更ゼロ・共有 assistant を数えない・割当てと store 移動の順序・全行タグ済みなら何もしない・不正 ID は接続前に拒否・system フラグ）、CLI 202 passed（新規3）。実 DB E2E `test_assign_existing_e2e.py` 4 passed（semantic search あり: 拒否でポリシー 0 のまま／割当て後は legacy が thread・run・assistant・checkpoint・store の get と**ベクトル検索**まで見え、他テナントは共有 assistant だけ／物理 prefix が `aegra_tenant.legacy.notes`／再実行は何もしない）。store 移動を外すと 2 件落ちることを確認。CLI を実 DB で「拒否→割当て→再実行」。unit/integration 2380、lint OK、type-check 57、サーバー用 DB で enable 再実行後のテナント RLS E2E 67 passed
  - docs: `tenant-isolation.mdx` に「既存環境のアップグレード」節、arch §8・§9
- [x] 7. ドキュメント仕上げ: 脅威モデル（arch §2、ガイドの Warning）に「グラフのコードは同じプロセスで動き、DB の資格情報と鍵を読める（#312）」。arch §2 に「テナントは持ち主ではない」＝RLS は会社、`user_id` の述語は会社の中の持ち主、#489 の owner は内側だけ、互いに導出しない。`docs/reference/cli.mdx` に `aegra db enable-tenant-rls` の節（`--app-role`・`--schema`・`--assign-existing-to`）、環境変数の説明を resolver に合わせた。ガイドの「semantic search を後から有効化」に、再実行までの挙動を追記
  - 追記の前に実 DB で確認: enable 後に semantic search を有効化 → 再実行前のテナントの埋め込み付き書き込みは `permission denied for table store_vectors` で、store 行も書かれない（止まる向き・原子的）。再実行後は書いたテナントの検索で見つかり、別テナントでは見つからない
- [x] 最終検証（2026-09-30）: unit/integration 2380 passed（RLS 未適用 DB）、CLI 202 passed、lint・format OK、type-check 57（HEAD と同数）、サーバー無しの実 DB E2E 56 passed
  - サーバー E2E（prod・Redis worker）: フラグ ON（`aegra_app` 所有者ログイン・FORCE・仮レジストリ入り header auth）25 failed / 195 passed / 5 skipped、フラグ OFF（標準 compose・noop auth・`aegra.json`・新しい DB）25 failed / 185 passed / 15 skipped。失敗は両方とも同じ 25 件で、すべて LLM グラフ（OpenAI 401）。フラグ ON のサーバーログにスコープ・暗号・権限・制約のエラーは 0

## 外のプロジェクトからの確認（`~/SampleCode/aegra-rls-sample`、2026-10-01）

このブランチをローカルパスで使う別リポジトリ。aegra 側の修正は不要だった。
- `make demo`（12 節・70 項目、生の HTTP）: 2 テナントの分離、トークンとレジストリ、DB・Redis の中身、同じユーザー名、書き込みの越境、store API、assistant、ストリーミング、cron、8 テナント × 20 本の同時実行。記録は `RALPH-sample.md`
- `make e2e`（34 件、アプリ＝BFF 越し）: BFF 自身はテナントも持ち主もチェックせず、ログインしたユーザーとして SDK で呼ぶだけ。他テナントのチャット・メモ・ストリームに届かない、持ち主の層とテナントの層が別々に効く、cookie 改ざん・テナント指定は無効、停止中テナントは 403、4 人同時でも混ざらない。記録は `RALPH-bff.md`
- 分かったこと（KH に入れるときにも効く）: aegra は auth handler にヘッダーを1つの dict で渡す（`authenticate(headers)`）／`metadata.owner` は作成時に上書きされる／他人のスレッドの run 一覧は 200 `[]`（存在しない ID と同じ応答、main と同じ仕様）

## 本番化の残タスク（2026-10-01 時点）

aegra のこのリポジトリ:
- [ ] コミットを分けて PR にする（Draft のうちにセルフレビュー）。upstream に出すかは未決（上の「未決」）
- [ ] `manual_auth_tests`（JWT の auth）と `multi_instance` の E2E を RLS 構成で流す（今回すべての回で除外していた）

aegra を動かす側のプロジェクト（KH の copilot agent・Python/Aegra 版を想定。確認待ち）:
- [ ] 本物の auth handler: IdP の JWT を検証して `org_id` を入れる。IdP の `org_id` の形式が分かったら `TENANT_ID_PATTERN`（仮置き `^[A-Za-z0-9_-]{1,64}$`）を合わせる
- [ ] テナントレジストリの本体: 「有効なテナント」の正本（IdP か契約管理か）を決め、キャッシュ付きで `configure_tenant_resolver()` に差し込む（毎リクエスト・毎 run・毎 cron で呼ばれる）。サンプルの `src/sample/tenancy.py` が形の見本
- [ ] KMS 版の鍵の提供元: `TenantKeyProvider` を実装し `configure_key_provider()`（`http.app` のモジュールから import 時に。auth のモジュールは最初のリクエストまで読まれないので遅い）。データ鍵のキャッシュとローテーション（`key_version`）。サンプルの `FakeKmsKeyProvider` が形の見本

本番環境（RDS・ElastiCache）:
- [ ] サーバーのログインロールをテーブル所有者・NOSUPERUSER・NOBYPASSRLS にし、enable は superuser から `--app-role`。デプロイ手順に enable の再実行（semantic search を有効にした後など）を入れる
- [ ] 往復が長い環境での性能（`scripts/bench_tenant_rls_checkpoint.py` を RDS 相当で）
- [ ] PgBouncer / RDS Proxy（トランザクションプーリング）を使う場合の確認: LangGraph pool の system checkout はセッション単位で `aegra.system` を立てる。ポリシーはログインロールにしか効かないので他テナントへは漏れない設計だが、実構成で未確認

