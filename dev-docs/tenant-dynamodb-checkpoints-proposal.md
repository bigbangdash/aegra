# テナント別 DynamoDB チェックポイント — 仕様案（B 案）

状態: **実装済み（Draft PR、ブランチ `feat/tenant-dynamodb-checkpoints`、2026-10-02）**。採否は未決。
前提は `tenant-rls-architecture.md`（以下「RLS 設計書」）。
ユーザー向けではない。採否が決まるまで `docs/` には環境変数の参照（`docs/reference/environment-variables.mdx`）以外書かない。
実 AWS（STS・IAM）では未確認。DynamoDB Local での E2E まで（§7）。

---

## 1. 一段落で

LangGraph のチェックポイント（`checkpoints` / `checkpoint_blobs` / `checkpoint_writes`）
だけを Postgres から外し、**テナントごとに別の DynamoDB テーブル**へ保存する。
threads・runs・assistants・crons・store・thread_ttl は今まで通り Postgres + RLS に残す。
切り替えは **env だけ**で行う（`AEGRA_CHECKPOINT_BACKEND=dynamodb`）。
DynamoDB 版は Aegra に同梱し、任意依存（extra）として入れる。アプリ側のコードは変えなくてよい。
Aegra の外に残すのは、テーブルと IAM ロールの用意（IaC）だけ。

```
            ┌──────────── Postgres（RLS、今のまま）────────────┐
request ──▶ │ thread / runs / assistant / crons / store / ttl │
  │         └─────────────────────────────────────────────────┘
  │ tenant_scope(t)
  ▼
TenantRoutingCheckpointer ──for_tenant(t)──▶ DynamoDBSaver(table = {PREFIX}{t},
                                                           creds = STS(tag tenant_id=t))
```

テナントレジストリと KMS 鍵は、コードで差し込むフックにした（RLS 設計書 §9）。
チェックポイントの保存先はそれと違う扱いにする。中身が業務ごとに変わるものではなく、
テーブル名の規則とロールの ARN だけで決まるので、設定値で足りる。

---

## 2. 目的と、目的にしないこと

この案が足すもの（Postgres の RLS ではできないもの）:

- 会話の状態（チェックポイント）の**物理的な分離**: テナント A のテーブルとテナント B のテーブルは別リソース
- テナントごとの暗号鍵: テーブル単位で KMS の CMK を分けられる
- 削除の証明: 解約時は「テーブルを削除した」で済む
- テナントごとのバックアップと復元（PITR）

この案が**足さない**もの:

- アプリのバグによる越境への強さは、RLS と同じ。テナントはどちらも `current_db_scope()` から決まる。
  スコープが正しければどちらも守れ、スコープ自体を取り違えるバグはどちらも防げない。
  IAM が追加で防ぐのは「スコープは正しいのに、別テナントのテーブル名を組み立ててしまう」種類のバグだけ
- 侵害されたプロセスへの対策は、今の脅威モデル（RLS 設計書 §2）と同じく対象外。
  サーバーはどのテナントのロールでも引き受けられるので
- Postgres 側の RLS は不要にならない。threads などが残るため、分離の仕組みが2系統になる

採用してよい条件: 物理分離・テナント別の鍵・削除証明のどれかが**顧客要件として出ている**こと。
出ていないなら RLS 設計書の構成（A 案）のままにする。

---

## 3. 設定

`settings.py` に `CheckpointSettings(EnvBase)` を足す。

| 変数（案） | 既定 | 意味 |
|---|---|---|
| `AEGRA_CHECKPOINT_BACKEND` | `postgres` | `postgres` / `dynamodb` |
| `AEGRA_DYNAMODB_TABLE_PREFIX` | `aegra-ckpt-` | テーブル名 = 接頭辞 + `tenant_id` |
| `AEGRA_DYNAMODB_REGION` | （必須） | リージョン |
| `AEGRA_DYNAMODB_TENANT_ROLE_ARN` | （必須） | テナント用に引き受ける IAM ロール（§6.1） |
| `AEGRA_DYNAMODB_S3_BUCKET` | 未設定 | 350KB を超える状態の退避先。`AEGRA_DYNAMODB_ENDPOINT_URL` が無い（実 AWS）ときは必須。無いと 400KB を超える状態の保存が `ValidationException` で失敗する（§8） |
| `AEGRA_DYNAMODB_TTL_SECONDS` | 未設定 | 設定するとチェックポイントに DynamoDB の TTL を付ける |
| `AEGRA_DYNAMODB_ENDPOINT_URL` | 未設定 | DynamoDB Local 用。設定中は STS を使わず、既定の認証情報でつなぐ |

起動時の確認（どれかに当たれば起動を拒否する）:

- `dynamodb` なのに `AEGRA_TENANT_RLS_ENABLED=false`（テナントが無いと振り分けられない）
- `dynamodb` なのに extra（`aegra-api[dynamodb]`）が入っていない（import は `try/except ImportError`）
- 必須の変数が足りない（`AEGRA_DYNAMODB_S3_BUCKET` は実 AWS のときだけ必須。DynamoDB Local には S3 が無い）
- `AEGRA_DYNAMODB_ENDPOINT_URL` と `AEGRA_DYNAMODB_TENANT_ROLE_ARN` が両方設定されている（本番で Local を向く事故を防ぐ）

その他:

- 新規デプロイ専用。Postgres から DynamoDB への移行と、その逆は作らない（RLS 設計書 §9 と同じ扱い）
- 一度 `dynamodb` で動かした後に `postgres` へ戻すと、状態が見えなくなる。戻さない（RLS のフラグと同じ）
- 変数を足したら `.env.example` と `libs/aegra-cli/src/aegra_cli/templates/env.example.template` の両方に書く（リポジトリの CLAUDE.md の規則）

---

## 4. Aegra 本体に入れるもの

### 4.1 テナントごとの saver を作る部分

```python
# core/tenancy/checkpointer.py（Protocol・振り分け・起動時の確認。extra を import しない）
class TenantCheckpointerProvider(Protocol):
    async def for_tenant(self, tenant_id: str) -> BaseCheckpointSaver: ...
    def for_tenant_sync(self, tenant_id: str) -> BaseCheckpointSaver: ...   # Pregel の同期経路用
    async def health(self) -> None: ...          # 例外 = 不健康

# core/tenancy/dynamodb.py（extra を先頭で import する。build_tenant_checkpointer() からだけ読み込む）
class DynamoDBCheckpointerProvider:              # §3 の env から組み立てる。同梱の実装はこれだけ
    ...
class PrunableDynamoDBSaver(DynamoDBSaver):     # prune/aprune（keep_latest・delete）を足す。§4.2
    ...
```

- `Protocol` は内部の境目として置くだけで、外から差し込む API（`configure_*`）は作らない。
  別の保存先が要るようになったら、`AEGRA_CHECKPOINT_BACKEND` の値を増やす
- `for_tenant` は saver をテナントごとにキャッシュする。STS の認証情報の期限が近づいたら作り直す
- テーブルが無い、または認証情報が取れないテナントは**例外**にする。その場でテーブルを作らない
  （サーバーに `CreateTable` の権限を持たせないため）。saver を作るときに `DescribeTable` を1回呼んで確かめ、
  無ければ `TenantCheckpointTableMissingError`、STS に断られたら `TenantCheckpointCredentialsError`
  （どちらも `TenantCheckpointerError`）。HTTP では **403**（`forbidden`、本文にテーブル名は出さない。
  レジストリに断られたテナントの 403 と同じ扱い）。run の中で起きれば run は `error` で終わる
- 依存: `langgraph-checkpoint-aws` を extra `dynamodb` に入れる。既定のインストールには入れない

### 4.2 振り分けチェックポインタ

`TenantRoutingCheckpointer(BaseCheckpointSaver)`。
langgraph-checkpoint 4.1（`uv.lock` は 4.1.1）のメソッドを、すべて同じ手順で委譲する。対象は
`aget_tuple` / `alist` / `aput` / `aput_writes` / `adelete_thread` / `adelete_for_runs` /
`acopy_thread` / `aprune` / `aget_delta_channel_history`、それと同期版。

```
1. scope = current_db_scope()          # スコープ未宣言 → DbScopeMissingError（今と同じ fail-closed）
2. tenant スコープ → saver = await provider.for_tenant(scope.tenant_id)
   system スコープ → エラー（§5.2 の明示 API 以外）
3. saver.<同じメソッド>(...)
```

- 手順1を**委譲の前に**済ませる。プロバイダが内部で `run_in_executor` を使うと
  ContextVar が引き継がれないため（`asyncio.to_thread` は引き継ぐが、`run_in_executor` は引き継がない）
- `get_next_version` とシリアライザ（`serde`）は振り分け先と一致させる。
  テナントごとに違う値を返されると版の比較が壊れるので、プロバイダには同じ設定を要求する。
  `DynamoDBSaver` は基底クラスの整数版（1, 2, 3…）をそのまま使う（§8）
- `aprune` は `DynamoDBSaver` にも `AsyncPostgresSaver` にも無い（どちらも基底の `NotImplementedError`）。
  DynamoDB 版は Aegra 側の saver サブクラスで `keep_latest` / `delete` を足す。名前空間ごとに
  最大の checkpoint_id を残し、他のチェックポイントとその writes を消す（今の Postgres の生 SQL と同じ意味）
- `setup()` は何もしない。テーブルの用意はテナント追加の手順に含める（§6）

### 4.3 呼び出し箇所の変更

| 箇所 | スコープ | 変更 |
|---|---|---|
| `core/database.py` で `AsyncPostgresSaver` を作る所 | 起動時 | `dynamodb` なら `build_tenant_checkpointer(settings.checkpoint)`（振り分けチェックポインタ）。`setup()` は store だけ |
| `main.py` の lifespan 先頭 | 起動時 | `ensure_checkpoint_backend_available()`（§3 の RLS フラグ・extra の確認）。`TenantCheckpointerError` → 403 のハンドラ |
| `services/langgraph_service.py` でグラフに渡す所 | tenant | 変更なし（振り分けチェックポインタがそのまま渡る） |
| `api/threads.py` の `adelete_thread` | tenant | 変更なし。state/history 系7箇所の `except Exception`（500 に包む）だけ `TenantCheckpointerError` を素通しにした |
| `services/run_cleanup.py` の `adelete_thread` | 呼び出し元から継承（バックグラウンドの後片付け） | 変更なし。tenant スコープを継承することをテストで固定（`test_run_cleanup_scope.py`） |
| `services/thread_ttl.py` の `adelete_thread` | **system**（テナントをまたぐ掃除） | §5.2。`dynamodb` のときだけ1件ごとに `tenant_scope` に入り直す |
| `services/thread_ttl.py` `_prune_checkpoint_history`（生 SQL） | **system** | `dynamodb` のときは `aprune([thread_id], strategy="keep_latest")`。`postgres` は生 SQL のまま（動きを変えない） |
| `core/health.py` の `aget_tuple("health-check")` | system | 振り分けなら `provider.health()`（失敗 = unhealthy / `/ready` 503）。`postgres` は今までどおり |
| `core/tenancy/rls.py` で RLS をかけるテーブルの一覧 | 有効化の CLI | `enable_tenant_rls(tables=None)` の既定が `isolated_tables_for_settings()`。`dynamodb` ではチェックポイントの3テーブルを外す（そもそも作られない） |

---

## 5. スコープの扱い

### 5.1 原則は RLS 設計書 §3 のまま

テナントを決めるのは入口だけ（HTTP の依存関係、`execute_run`、cron の発火）。
振り分けチェックポインタはスコープを読むだけで、自分ではテナントを決めない。

### 5.2 system スコープでテナントをまたぐ処理

スレッド TTL の掃除は system スコープで、全テナントの期限切れスレッドを処理する。
Postgres なら RLS の system ポリシーで1本の SQL で済むが、テーブルがテナントごとに分かれるとそうはいかない。

- 期限切れのスレッドを取る SQL（`_expired_claim_stmt`）で `thread.tenant_id` も取る（バックエンドによらず）
- `dynamodb` のときだけ、1件ごとに `with tenant_scope(row.tenant_id):` に入り直してから `adelete_thread` / `aprune` を呼ぶ。
  cron の発火（RLS 設計書 §3）と同じ形。`postgres` は今までどおり system スコープのまま
  （claim のトランザクションは SQLAlchemy 側で system のまま進む。スコープを変えるのはチェックポイント側だけ）
- `tenant_id` が無い行は、その1件だけ失敗として数え、次の claim へ進む
- system スコープのまま振り分けチェックポインタを呼んだらエラーにする。
  「system なら全テーブルを見る」という抜け道は作らない

### 5.3 2つの DB をまたぐ順序

Postgres と DynamoDB を1つのトランザクションにはできない。今のコードの順序
（「チェックポイントを先に消し、その後でスレッドの行を消す」。`thread_ttl.py:186` のコメント）を守る。

| 失敗の位置 | 残る状態 | 扱い |
|---|---|---|
| チェックポイント削除の途中 | スレッドの行も、一部のチェックポイントも残る | 再実行で消える（`adelete_thread` は冪等であることをプロバイダに要求） |
| チェックポイント削除の後、行の削除の前 | スレッドの行だけ残る（状態は空） | 再実行で消える |
| run の作成後、最初のチェックポイントの前 | チェックポイントが無い | 今と同じ |

スレッドの行が無いのにチェックポイントだけが残る「孤児」は、上の順序なら生じない。
念のため、テナントごとにテーブルをスキャンして Postgres に無い `thread_id` を報告する
突き合わせジョブを別に用意する（自動では消さない。§9 の未決事項）。

---

## 6. DynamoDB 版の中身と、Aegra の外に置く運用

Aegra に入るのは §4.1 の `DynamoDBCheckpointerProvider` まで。テーブルと IAM ロールは IaC で用意する。
`aegra-rls-sample` には、DynamoDB Local で `AEGRA_CHECKPOINT_BACKEND=dynamodb` を試す docker 構成を足す。

### 6.1 `DynamoDBCheckpointerProvider`

- 中身は `langgraph-checkpoint-aws` の `DynamoDBSaver`（1.2.3）。1テーブルに文字列キー `PK`（HASH）と `SK`（RANGE）、
  TTL 属性は `ttl`。チェックポイントは `PK=CHECKPOINT_{thread_id}` / `SK={ns}#{checkpoint_id}`、writes は
  `PK=WRITES_{thread_id}#{ns}#{id}` / `SK={task_id}#{idx}`、本体は `PK=CHUNK_…` / `SK=CHUNK` の別アイテム。
  gzip 圧縮（任意）と、350KB を超える本体の S3 退避（任意）がある
  （[AWS ドキュメント](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ddb-langgraph-checkpoint.html)）
- テナントごとにテーブル名 `{AEGRA_DYNAMODB_TABLE_PREFIX}{tenant_id}` と、そのテナントの boto3 セッションで1インスタンス作る。
  テーブルごとに分けるので、キー設計は変えずに済む
- 認証情報: `AEGRA_DYNAMODB_TENANT_ROLE_ARN` のロール1本を `sts:AssumeRole` + `TagSession`（`tenant_id=<t>`）で引き受ける。
  サーバー自身の認証情報（ECS のタスクロールなど）には、このロールを引き受ける権限だけを持たせる
- ロールのポリシー（IaC 側）は `Resource` を `arn:aws:dynamodb:<region>:<acct>:table/<prefix>${aws:PrincipalTag/tenant_id}` に限る。
  S3 退避のオブジェクトのキーも `${aws:PrincipalTag/tenant_id}/` で始まるものに限る
- 認証情報は期限の手前（5分前）で更新し、テナントごとにキャッシュする。テナント数が約100なら件数は問題にならない。
  `RoleSessionName` は `aegra-{tenant_id}`（64文字で切る）、`DurationSeconds` は 3600
- ロールに要る権限（IaC 側）: 自テーブルへの読み書きに加えて **`dynamodb:DescribeTable`**（saver 生成時の確認）。
  `AEGRA_DYNAMODB_TTL_SECONDS` を設定すると `DynamoDBSaver` が S3 バケットのライフサイクル設定を読み書きしようとする
  （`s3:GetLifecycleConfiguration` / `PutLifecycleConfiguration`。無くても警告ログだけで動く）
- `health()` は、直近に使ったテナントの saver で `DescribeTable` を1回。まだどのテナントも使っていなければ
  実 AWS では `sts:GetCallerIdentity`、Local では `ListTables(Limit=1)`。全テナントのテーブルは見ない。
  直近のテナントのテーブルが消えていても（解約、§6.2）バックエンドは応答しているので健康とみなし、
  その saver をキャッシュから外す（次の要求は作り直し → `TenantCheckpointTableMissingError`）

### 6.2 テナントのライフサイクル

| 操作 | 手順 |
|---|---|
| 追加 | テナントレジストリへの登録と同時に、IaC でテーブルを作る（PK/SK、TTL、PITR、SSE。テナント別の CMK は任意）。テーブルができるまでレジストリ上は `active` にしない |
| 停止 | レジストリで止める（今のまま）。テーブルは残す |
| 削除 | Postgres のスレッドの行を消した後、テーブルを削除する。S3 退避分も同じテナントのプレフィックスごと消す |

### 6.3 制約

- 1アイテムは 400KB まで。S3 退避を必須にする
- 1アカウント・1リージョンあたりのテーブル数に上限がある。具体値は採用前に Service Quotas で確認する
- 状態の履歴（`aget_state_history`）は DynamoDB の Query になる。`alist` の `filter`（メタデータでの絞り込み）は
  全件を読んでクライアント側で比べる（§8）。履歴の長いスレッドでは読み取り量がそのまま増える

---

## 7. テスト

| レベル | 内容 | 場所 |
|---|---|---|
| unit | 設定の検証4条件と起動時の確認 | `tests/unit/test_settings.py`（`TestCheckpointSettings`）、`test_core/test_tenancy/test_checkpointer_startup.py`、`test_main.py` |
| unit | provider: テナントごとのキャッシュ、STS のタグ・期限前の作り直し、endpoint 時は STS 無し、テーブル無し/認証不可は例外で作らない、`health()`、prune の keep_latest | `test_core/test_tenancy/test_dynamodb_provider.py`（boto3 は偽セッション） |
| unit | 振り分け: tenant スコープ → そのテナントの saver、system → `SystemScopeCheckpointerError`、スコープ無し → `DbScopeMissingError`。非同期9・同期9の全メソッド（基底クラスの公開メソッドを網羅していることも検査）、`run_in_executor` の中でも委譲前のテナント | `test_core/test_tenancy/test_routing_checkpointer.py` |
| unit | TTL の掃除が `dynamodb` では1件ごとに tenant スコープへ入り直し `aprune` を呼ぶ（生 SQL 無し）、`postgres` は system のまま。`run_cleanup` のスコープ継承。RLS の対象テーブル。`database.py` の分岐 | `test_services/test_thread_ttl.py`、`test_core/test_tenancy/test_run_cleanup_scope.py`、`test_core/test_tenancy/test_rls.py`、`test_core/test_database_manager.py` |
| integration | `/health`・`/ready` が provider 経由、`TenantCheckpointerError` → 403（delete・state・history） | `tests/integration/test_health_checkpoint_backend.py`、`test_api/test_threads_tenant_checkpointer_error.py` |
| E2E（DynamoDB Local） | `docker-compose.tenant-dynamodb.yml` ＋ host のサーバー（手順はファイル先頭）。2テナントで run → 相手の state/history/delete は 404、自分のテーブルにだけアイテム、削除・`/threads/prune`（delete・keep_latest）でそのテーブルからだけ消える、テーブル無しテナントは run が `error`・state/history/delete が 403 | `tests/e2e/test_tenant_rls/test_tenant_dynamodb_e2e.py`（`AEGRA_E2E_TENANT_DYNAMODB=1`） |
| E2E（実 AWS、手動・未実施） | テナント A の認証情報で B のテーブルを読むと `AccessDenied`。DynamoDB Local は IAM を評価しないので、ここは実環境でしか確かめられない | — |

---

## 8. 採用前に確かめること（2026-10-01 に `langgraph-checkpoint-aws` 1.2.3 を読んで確認）

- 非同期版は**同期 boto3 を `langchain_core.runnables.run_in_executor` で既定のスレッドプールに逃がしているだけ**
  （`aget_tuple` / `aput` / `aput_writes` / `adelete_thread`。`alist` は1件ずつスレッドで `next()`）。
  同時実行はスレッドプールの既定（`min(32, cpu+4)`）で頭打ちになる。1インスタンス30 run なら同じ桁。
  `run_in_executor` は `copy_context().run` で包むので ContextVar は引き継がれるが、§4.2 の「委譲の前にスコープを読む」は変えない
- 実装があるのは `get_tuple` / `put` / `put_writes` / `list(filter・before・limit 対応、filter はクライアント側)` /
  `delete_thread` とその非同期版だけ。`prune` / `delete_for_runs` / `copy_thread` / `get_delta_channel_history` は
  基底クラスのまま（`get_delta_channel_history` は基底の既定実装が `get_tuple` を辿るので動く。他は `NotImplementedError`）。
  `AsyncPostgresSaver`（langgraph-checkpoint-postgres 3.0.4）も `prune` / `delete_for_runs` / `copy_thread` は未実装で、
  Aegra はどれも呼んでいない。TTL の `keep_latest` だけは Aegra 側で足す（§4.2）
- `get_next_version` は基底の整数（`AsyncPostgresSaver` は `"{n:032}.{乱数}"` の文字列）。Aegra に版の形式を前提にしたコードは無い。
  振り分けチェックポインタは委譲先と同じ整数版を返す
- S3 退避は任意（`s3_offload_config` を渡さなければ全部 DynamoDB に書く）。DynamoDB Local だけで動く。
  ただし本体が 400KB を超えると `ValidationException: Item size has exceeded the maximum allowed size` で
  その run が失敗する（途中まで書いた小さいアイテムは残る）。実 AWS では S3 を必須にする（§3）
- テーブルが無いテナント: 最初の読み書きで `ClientError`（`ResourceNotFoundException`）。saver の生成時には何も問い合わせない
- 1チェックポイントあたりの書き込みコスト（DynamoDB のオンデマンド課金）は未確認（§9 に残す）。
  1チェックポイント = メタ1 + 本体1 + writes（タスクごとにメタ1 + 本体1）のアイテム数になる

---

## 9. 未決事項・残作業

- **実 AWS での確認（未実施）**: `AssumeRole`＋`TagSession` のロールポリシー（`${aws:PrincipalTag/tenant_id}`）で
  テナント A の認証情報から B のテーブルが `AccessDenied` になること。`DescribeTable`・S3 ライフサイクルの権限（§6.1）
- 孤児の突き合わせジョブ（§5.3）。見つけたものを自動で消すか、報告だけにするか
- 1チェックポイントあたりの DynamoDB の書き込みコスト（オンデマンド課金）が run の頻度で許容範囲か（§8 のアイテム数から見積もる）。
  性能（同期 boto3 のスレッドプール、`alist(filter=)` の全件読み）も未計測
- `keep_latest` の prune は `DeltaChannel` を使うグラフでは履歴の鎖を切る（langgraph-checkpoint の `prune` の注意書き）。
  今の Postgres の生 SQL も同じ性質なので据え置き。`DeltaChannel` を使い始めるときに見直す
- キャッシュ済みの saver があるテナントのテーブルを消すと、`health()` が気づくまでは読み書きが boto3 の
  `ResourceNotFoundException` のまま上がる（403 にならない）。解約したテナントはレジストリ（resolver）で先に止める前提
- Docker イメージ（`deployments/docker/Dockerfile`）は `uv export --no-emit-project` で extra を入れないので、
  `dynamodb` で動かすイメージには extra の追加が要る（今回は host で起動して E2E）
- 孤児の突き合わせジョブで、見つけたものを自動で消すか、報告だけにするか
- upstream に出すか。出すなら、振り分けの仕組み（§4.2）と `AEGRA_CHECKPOINT_BACKEND` を先に出し、
  DynamoDB 版は extra として別 PR にする
- env で済まない保存先（独自のストレージなど）が出てきたら、そのときにコードで差し込むフックを足すか決める。
  今は作らない
- store（長期メモリ）も同じ形で分けるか。`langgraph-checkpoint-aws` には `DynamoDBStore` もあるが、
  今の `TenantScopedPostgresStore` の namespace 前置き（RLS 設計書 §6）と意味検索の扱いをどうするかが別に要る。本案の範囲外
