# テナント別 DynamoDB チェックポイント — 仕様案（B 案）

状態: **提案・未実装**（2026-10-01）。前提は `tenant-rls-architecture.md`（以下「RLS 設計書」）。
ユーザー向けではない。採否が決まるまで `docs/` には何も書かない。

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
# core/tenant_checkpointer.py（新規）
class TenantCheckpointerProvider(Protocol):
    async def for_tenant(self, tenant_id: str) -> BaseCheckpointSaver: ...
    async def health(self) -> None: ...          # 例外 = 不健康

class DynamoDBCheckpointerProvider:              # §3 の env から組み立てる。同梱の実装はこれだけ
    ...
```

- `Protocol` は内部の境目として置くだけで、外から差し込む API（`configure_*`）は作らない。
  別の保存先が要るようになったら、`AEGRA_CHECKPOINT_BACKEND` の値を増やす
- `for_tenant` は saver をテナントごとにキャッシュする。STS の認証情報の期限が近づいたら作り直す
- テーブルが無い、または認証情報が取れないテナントは**例外**にする。その場でテーブルを作らない
  （サーバーに `CreateTable` の権限を持たせないため）
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
| `core/database.py:85` で `AsyncPostgresSaver` を作る所 | 起動時 | `dynamodb` なら `TenantRoutingCheckpointer(DynamoDBCheckpointerProvider(settings))` を作る。`setup()` は store だけにする |
| `services/langgraph_service.py:355` でグラフに渡す所 | tenant | 変更なし（振り分けチェックポインタがそのまま渡る） |
| `api/threads.py:936` の `adelete_thread` | tenant | 変更なし |
| `services/run_cleanup.py:70` の `adelete_thread` | 呼び出し元から継承（バックグラウンドの後片付け） | 実装時に tenant スコープを継承していることをテストで固定する |
| `services/thread_ttl.py:188` の `adelete_thread` | **system**（テナントをまたぐ掃除） | §5.2 |
| `services/thread_ttl.py:150` `_prune_checkpoint_history`（チェックポイントのテーブルを直接 SQL で消す） | **system** | §5.2。生 SQL をやめ `aprune(..., strategy="keep_latest")` に置き換える |
| `core/health.py:94,141` の `aget_tuple("health-check")` | system | `provider.health()` に置き換える |
| `core/tenancy/rls.py:31-33` で RLS をかけるテーブルの一覧 | 有効化の CLI | `dynamodb` のときはチェックポイントの3テーブルを外す（そもそも作られない） |

---

## 5. スコープの扱い

### 5.1 原則は RLS 設計書 §3 のまま

テナントを決めるのは入口だけ（HTTP の依存関係、`execute_run`、cron の発火）。
振り分けチェックポインタはスコープを読むだけで、自分ではテナントを決めない。

### 5.2 system スコープでテナントをまたぐ処理

スレッド TTL の掃除は system スコープで、全テナントの期限切れスレッドを処理する。
Postgres なら RLS の system ポリシーで1本の SQL で済むが、テーブルがテナントごとに分かれるとそうはいかない。

- 期限切れのスレッドを取る SQL（`_expired_claim_stmt`）で `thread.tenant_id` も取る
- 1件ごとに `with tenant_scope(row.tenant_id):` に入り直してから `adelete_thread` / `aprune` を呼ぶ。
  cron の発火（RLS 設計書 §3）と同じ形
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
- 認証情報は期限の手前で更新し、テナントごとにキャッシュする。テナント数が約100なら件数は問題にならない
- `health()` は、引き受けたロールで DynamoDB に1回問い合わせる程度にする。全テナントのテーブルは見ない

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

| レベル | 内容 |
|---|---|
| unit | 振り分け: tenant スコープ → そのテナントの saver、system スコープ → エラー、スコープ無し → `DbScopeMissingError`。全メソッドを対象にする |
| unit | `run_in_executor` の中から呼ばれても、委譲前に決めたテナントが使われる |
| unit | TTL の掃除が1件ごとに tenant スコープへ入り直す。生 SQL が呼ばれない |
| E2E（DynamoDB Local） | `aegra-rls-sample` の `make demo` 2項目め（他テナントの state は 404）がそのまま通る。テーブルが2つでき、相手のテーブルに行が無い |
| E2E（実 AWS、手動） | テナント A の認証情報で B のテーブルを読むと `AccessDenied`。DynamoDB Local は IAM を評価しないので、ここは実環境でしか確かめられない |

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

## 9. 未決事項

- 1チェックポイントあたりの DynamoDB の書き込みコスト（オンデマンド課金）が run の頻度で許容範囲か（§8 のアイテム数から見積もる）
- 孤児の突き合わせジョブで、見つけたものを自動で消すか、報告だけにするか
- upstream に出すか。出すなら、振り分けの仕組み（§4.2）と `AEGRA_CHECKPOINT_BACKEND` を先に出し、
  DynamoDB 版は extra として別 PR にする
- env で済まない保存先（独自のストレージなど）が出てきたら、そのときにコードで差し込むフックを足すか決める。
  今は作らない
- store（長期メモリ）も同じ形で分けるか。`langgraph-checkpoint-aws` には `DynamoDBStore` もあるが、
  今の `TenantScopedPostgresStore` の namespace 前置き（RLS 設計書 §6）と意味検索の扱いをどうするかが別に要る。本案の範囲外
