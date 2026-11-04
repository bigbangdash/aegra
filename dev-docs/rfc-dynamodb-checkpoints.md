# RFC: 保存先を切り替えられるようにする（DynamoDB・テナント別テーブル、MongoDB）

- 状態: **下書き（社内レビュー前）**。本家に出すときは英語にして、aegra/aegra#331 に Discussion か doc PR で出す
- 対象の issue: aegra/aegra#331（#324 MongoDB、#270 MySQL を統合したもの）
- 実装: フォーク `bigbangdash/aegra` のブランチ `feat/tenant-dynamodb-checkpoints`
  （8コミット。`feat/tenant-rls-poc` の上に積んである。本家 main `c7dd3ee` に載せ直し済み）。
  実装したのは §5.2 のテナント別テーブルだけ。MongoDB は未着手
- 設計の詳細: `dev-docs/tenant-dynamodb-checkpoints-proposal.md`（以下「DynamoDB 設計書」）
- 書いた人: （記入）／ 実装と保守の担当: （記入。§9）
- 関係する RFC: `dev-docs/rfc-tenant-rls.md`（以下「RLS RFC」）。§5.2 はそのテナントの仕組みを前提にする

---

## 1. 一段落で

aegra が保存しているものを4つの役割に分け、役割ごとに保存先を設定で選べるようにする。
Postgres は既定のまま残し、設定しなければ今と何も変わらない。
最初に切り替えられるようにするのは、すでにライブラリがある **チェックポイントと store** で、
DynamoDB と MongoDB を選べるようにする（Phase 1）。
DynamoDB のチェックポイントは、RLS RFC のテナント分離を有効にしていれば**テナントごとに別のテーブル**に振り分けられる。
thread・run などの管理情報（メタデータ）は、インターフェースを決めてから MongoDB 版を足す（Phase 2）。
DynamoDB でメタデータを持つことは、検索の要件に合わないので目標にしない。

```
                       ┌──────────── 設定で選ぶ ────────────┐
  HTTP / ワーカー ──▶  │ ① チェックポイント  postgres | dynamodb | mongodb │ Phase 1
                       │ ② store（長期メモリ） postgres | dynamodb* | mongodb │ Phase 1
                       │ ③ メタデータ        postgres | mongodb            │ Phase 2
                       │ ④ 調停（キュー・リース・掃除） ③ と同じ DB に従う  │ Phase 2
                       └────────────────────────────────────┘
   * DynamoDB の store は意味検索（ベクトル検索）なし

   ① を dynamodb にして、テナント分離（RLS RFC）も有効なら:
   tenant_scope(t) ──▶ TenantRoutingCheckpointer ──▶ テーブル {接頭辞}{t}（STS で t 専用の認証情報）
```

---

## 2. なぜやるか

- AWS で運用している組織は、会話の状態（チェックポイント）を DynamoDB に置きたい。
  サーバーレスで容量の管理が要らない、テーブル単位で暗号鍵やバックアップを分けられる、などの理由
- 複数の会社で1つの aegra を共有する場合、会社ごとにテーブルを分けると、Postgres の RLS ではできない次のことができる（§5.2）
  - 会話の状態の物理的な分離（会社 A と B のテーブルは別リソース）
  - 会社ごとの暗号鍵（テーブル単位で KMS の鍵を分ける）
  - 解約時の削除の証明（「テーブルを消した」で済む）
  - 会社ごとのバックアップと復元
- 社内の標準 DB が MongoDB の組織が、aegra のためだけに Postgres を運用したくない（#331、#324。5組織ほどが要望）
- 今は「別の DB を使いたい」なら aegra をフォークするしかない。メンテナーも「フォークされるより議論したい」と書いている

---

## 3. 目的にしないこと

- 保存先の間のデータ移行（Postgres → DynamoDB など）。新しく立てる環境だけを対象にする
- 動いている環境で保存先を変えること。変えると今までのデータが見えなくなる
- MySQL・Cloudflare D1 などの対応。メンテナーの方針どおり、インターフェースが固まってから別の実装として扱う
- Redis（SSE の pub/sub とジョブキュー）の置き換え。今のまま
- 1つの環境の中で、役割ごとにバラバラな組み合わせを自由に選べること。対応する組み合わせは §5.4 の表に限る
- テナント別テーブルで、アプリのバグによる越境への強さを RLS より上げること（§5.2）

---

## 4. 今の aegra は何をどこに保存しているか

| 役割 | 中身 | 今の実装 | Postgres 固有の機能への依存 |
|---|---|---|---|
| ① チェックポイント | 会話の途中状態（`checkpoints` / `checkpoint_blobs` / `checkpoint_writes`） | `AsyncPostgresSaver`（`core/database.py`。langgraph-checkpoint-postgres 3.2.0） | 小さい。スレッド TTL の履歴削除は #707 で saver の `aprune(strategy="keep_latest")` 経由になった。ヘルスチェックは `aget_tuple` を直接呼ぶ |
| ② store | 長期メモリ。意味検索あり | `AsyncPostgresStore`（`core/database.py`） | 意味検索に pgvector |
| ③ メタデータ | `assistant` / `assistant_versions` / `thread` / `runs` / `crons` / `thread_ttl` の6テーブル | SQLAlchemy。16ファイルから約140か所でセッションを直接使う。Alembic のマイグレーション17本 | 大きい。JSONB の包含検索（`@>`）、`ON CONFLICT`、`md5(config::text)` の一意インデックス、`gen_random_uuid()` |
| ④ 調停 | run の割り当てとリース、クラッシュ後の回収、cron の発火、スレッド TTL の掃除 | ③ の `runs` などの行に対する更新 | 大きい。`SELECT … FOR UPDATE SKIP LOCKED`（`lease_reaper.py`・`cron_service.py`・`thread_ttl.py`）、マイグレーション時の advisory lock |

メンテナーが #331 で挙げた難所は、③と④に集中している。

- 認可フィルタが JSONB の包含条件に変換されていて、ユーザー分離を SQL で強制している。
  別の DB でここを少しでも間違えると、バグでは済まずデータ漏えいになる
- リースを使ったクラッシュ回収が、行ロックの性質に頼っている

①と②は、LangGraph の公式インターフェース（`BaseCheckpointSaver` / `BaseStore`）の実装を差し替えるだけで済む。
DynamoDB 用（`langgraph-checkpoint-aws`）と MongoDB 用（`langgraph-checkpoint-mongodb`、`langgraph-store-mongodb`）は
それぞれ別の団体が保守している。

---

## 5. 提案

### 5.1 Phase 1: チェックポイントと store の切り替え

**設定**

| 変数 | 既定 | 値 |
|---|---|---|
| `AEGRA_CHECKPOINT_BACKEND` | `postgres` | `postgres` / `dynamodb` / `mongodb` |
| `AEGRA_STORE_BACKEND` | `postgres` | `postgres` / `dynamodb` / `mongodb` |
| `AEGRA_DYNAMODB_*`、`MONGODB_URI` ほか | — | 保存先ごとの接続設定。使う保存先の分だけ必須。DynamoDB の分は §5.2 |

新しい変数は、プロジェクトの規則どおり `.env.example`・CLI のテンプレート・`docs/guides/deployment.mdx` に書く。

**本体に入れるもの**

- `core/persistence/` に、設定から `BaseCheckpointSaver` と `BaseStore` を作る関数を1つずつ置く。
  `core/database.py` は、今 `AsyncPostgresSaver` と `AsyncPostgresStore` を直接作っている所でこの関数を呼ぶ
- DynamoDB 版と MongoDB 版は追加インストール（`aegra-api[dynamodb]`、`aegra-api[mongodb]`）にする。
  既定のインストールには入れない。選んだのに入っていなければ、起動時に止める
- 本体に残っている「Postgres 前提」を外す:
  - スレッド TTL の履歴削除は、本家ではすでに `aprune` 経由（#707）。ただし DynamoDB 版・MongoDB 版の saver は
    `aprune` を実装していない（DynamoDB 版は `langgraph-checkpoint-aws` 1.2.6 まで確認。MongoDB 版は要確認）ので、
    Aegra 側のサブクラスで足す。DeltaChannel を使う thread を飛ばす今の扱い（#707）も同じにする
  - ヘルスチェック（`core/health.py`）が `aget_tuple` を直接呼んでいる所を、保存先ごとの確認にする
  - store の意味検索（`aegra.json` の `store.index`）を、対応していない保存先で設定したら起動時に止める

**ユーザー分離は今と変わらない**

チェックポイントは `thread_id` をキーに保存される。誰の thread かはメタデータ（Postgres のまま）で確かめてから
チェックポイントに触るので、分離の仕組みは今と同じ。
store も、Aegra が namespace の先頭にユーザーの区切りを付けている今の仕組みのまま。
ただし、どちらも「Aegra 側が正しいキーを渡す」ことが前提なので、§7 の共通テストで保存先ごとに確かめる。

**Phase 1 だけで得られるもの**

会話の状態という、いちばん量が多く増え続けるデータを DynamoDB や MongoDB に置ける。
Postgres に残るのは管理情報だけになり、小さなインスタンスで済む。
「Postgres を完全に無くしたい」という要望には、Phase 2 まで応えられない。

### 5.2 DynamoDB のテナント別テーブル（RLS RFC が前提）

テナント分離（`AEGRA_TENANT_RLS_ENABLED=true`）を有効にして `AEGRA_CHECKPOINT_BACKEND=dynamodb` にすると、
チェックポイントを**テナントごとに別の DynamoDB テーブル**へ保存する。フォークで実装・E2E 済みなのはこの形。
thread・runs・assistant・crons・store・thread_ttl は Postgres（RLS）に残る。

**採用してよい条件**: 物理分離・テナント別の鍵・削除の証明のどれかが、**顧客の要件として出ている**こと。
出ていなければ RLS だけ（チェックポイントも Postgres）にする。分離の仕組みが2系統になるので、要らなければ増やさない。

**仕組み**

- `TenantRoutingCheckpointer`（`BaseCheckpointSaver`）を1つだけグラフに渡す。どのメソッドも次の手順で委譲する
  1. `current_db_scope()` を読む。スコープが無ければエラー（RLS RFC と同じく、素通しにしない）
  2. tenant スコープならそのテナントの saver を取る。system スコープならエラー（「system なら全テーブル」という抜け道は作らない）
  3. 同じメソッドを呼ぶ
- 手順1は委譲の前に済ませる。DynamoDB 版は中で同期の boto3 をスレッドに逃がしているので、そこで ContextVar に頼らないため
- テナントの saver は `DynamoDBCheckpointerProvider` が作ってキャッシュする。テーブル名は `{AEGRA_DYNAMODB_TABLE_PREFIX}{tenant_id}`
- 認証情報: ロール1本を `sts:AssumeRole` と `TagSession`（`tenant_id=<t>`）で引き受ける。
  ロールのポリシーは `Resource` を `table/<prefix>${aws:PrincipalTag/tenant_id}` に限る（IaC 側）。
  期限の5分前に取り直す
- テーブルが無い、認証情報が取れないテナントは例外にし、HTTP では 403、run は `error` で終わる。
  その場でテーブルを作らない（サーバーに `CreateTable` の権限を持たせないため）

**設定**（DynamoDB 設計書 §3）

| 変数 | 意味 |
|---|---|
| `AEGRA_DYNAMODB_TABLE_PREFIX` | テーブル名の接頭辞（既定 `aegra-ckpt-`） |
| `AEGRA_DYNAMODB_REGION` | リージョン（必須） |
| `AEGRA_DYNAMODB_TENANT_ROLE_ARN` | テナント用に引き受けるロール（必須） |
| `AEGRA_DYNAMODB_S3_BUCKET` | 350KB を超える状態の退避先。実 AWS では必須（DynamoDB の1アイテムは 400KB まで） |
| `AEGRA_DYNAMODB_TTL_SECONDS` | チェックポイントに DynamoDB の TTL を付ける |
| `AEGRA_DYNAMODB_ENDPOINT_URL` | DynamoDB Local 用。ロールの ARN と同時に設定したら起動しない（本番で Local を向く事故を防ぐ） |

**会社をまたぐ掃除**

スレッド TTL の掃除は system スコープで全社の期限切れスレッドを処理する。テーブルが会社ごとに分かれるので、
期限切れの行を取るときに `thread.tenant_id` も取り、1件ごとに `tenant_scope` に入り直してから `adelete_thread` / `aprune` を呼ぶ
（cron の発火と同じ形）。

**2つの DB をまたぐ順序**

Postgres と DynamoDB は1つのトランザクションにできない。今のコードの順序（チェックポイントを先に消し、スレッドの行は後）を守る。
途中で落ちても、再実行すれば消える。スレッドの行が無いのにチェックポイントだけ残る「孤児」は、この順序なら生じない。

**この形が守らないもの**

- アプリのバグによる越境への強さは RLS と同じ。どちらもテナントはスコープから決まる。
  IAM が追加で防ぐのは「スコープは正しいのに、別のテナントのテーブル名を組み立てた」種類のバグだけ
- 侵害されたプロセスへの対策は対象外。サーバーはどのテナントのロールも引き受けられる

### 5.3 Phase 2: メタデータと調停の切り替え（MongoDB）

**インターフェースの境目**

ルートやサービスから SQLAlchemy のセッションを直接使うのをやめ、リソースごとのリポジトリを通す。

```python
class ThreadRepository(Protocol):
    async def create(self, owner: OwnerScope, thread: NewThread, *, if_exists: IfExists) -> Thread: ...
    async def get(self, owner: OwnerScope, thread_id: str) -> Thread | None: ...
    async def search(self, owner: OwnerScope, query: ThreadQuery) -> Page[Thread]: ...
    async def update_metadata(self, owner: OwnerScope, thread_id: str, patch: MetadataPatch) -> Thread | None: ...
    async def delete(self, owner: OwnerScope, thread_id: str) -> bool: ...
# 同じ形で AssistantRepository / RunRepository / CronRepository / ThreadTTLRepository
```

- **すべてのメソッドが `owner` を必須の引数に取る。** 持ち主を指定しない読み書きはインターフェース上できない。
  今の「各クエリに `WHERE user_id == …` を書き足す」作りでは、書き忘れがそのまま漏えいになっていた
  （GHSA-m98r-6667-4wq7、#424、#431 はどれもこの種類）。境目を型で強制する
- 持ち主の値を何から決めるか（今の `user.identity`、#489 の `owner` 案）はリポジトリの外で決める。
  リポジトリは渡された `OwnerScope` に従うだけにする
- テナントをまたいで処理する内部の仕事（掃除・回収）だけは、別の `SystemScope` を受け取るメソッドに分ける。
  リクエストの経路からは呼べない場所に置く
- RLS RFC のテナント分離は Postgres の機能なので、MongoDB のメタデータでは使えない。
  MongoDB でテナントを分けるなら、`OwnerScope` にテナントを含めてリポジトリが条件に足す（§10）

**認可フィルタ**

`core/auth_filters.py` は今、認可処理が返したフィルタ（`{"owner": "u1"}`、`{"tags": {"$contains": "x"}}` など）を
JSONB の `@>` に直接変換している。これを2段にする。

1. フィルタを保存先に依存しない形（等しい・配列が含む、の2種類と AND）に変換する。対応していない書き方はここで拒否する
2. 保存先ごとに、それを Postgres の `@>` か MongoDB の `{"metadata.owner": "u1"}` / `{"metadata.tags": {"$all": [...]}}` に変換する

1段目と2段目の対応は §7 の共通テストで、同じ入力から同じ行が返ることを確かめる。

**調停（キュー・リース・掃除）**

行ロックを前提にしたコードを、「条件付きで1件を取る」操作に言い換えてリポジトリに入れる。

| 今の処理 | Postgres（今の実装を移すだけ） | MongoDB |
|---|---|---|
| run を取る・リースを延長する | `UPDATE … WHERE claimed_by IS NULL` | `findOneAndUpdate`（条件に `status` と `claimed_by` を入れる） |
| リース切れの run を回収する | `FOR UPDATE SKIP LOCKED` | `findOneAndUpdate` を1件ずつ。条件に `lease_expires_at < now` |
| cron の発火 | `FOR UPDATE SKIP LOCKED` | `findOneAndUpdate` で `next_run_date` を進めた側だけが発火する |
| スレッド TTL の掃除 | `FOR UPDATE SKIP LOCKED` | 掃除中の印を付ける `findOneAndUpdate`（印に期限を付けて、落ちたら別のインスタンスが拾う） |

- MongoDB は複数件を一度に取る `SKIP LOCKED` 相当が無いので、1件ずつ取る。大量の回収では Postgres より遅くなる
- 「1つの run を2つのワーカーが同時に実行しない」「落ちたワーカーの run は回収される」という保証は変えない。
  §7 の競合テストで、保存先ごとに確かめる
- MongoDB は **レプリカセット必須** にする（複数ドキュメントのトランザクションに要るため）。単体構成なら起動時に止める

**スキーマ**

- Postgres は今の Alembic のまま
- MongoDB はマイグレーションの代わりに、起動時にコレクションとインデックスを作る。版を記録するドキュメントを置き、
  複数インスタンスが同時に起動したときは1つだけが作る（Postgres の advisory lock の代わり）
- 一意制約（`assistant` の `user_id + graph_id + config` など）は、MongoDB では config のハッシュをフィールドに持って一意インデックスにする

### 5.4 対応する組み合わせ

起動時に、下の表に無い組み合わせは拒否する。

| メタデータ・調停 | チェックポイント | store | 段階 |
|---|---|---|---|
| postgres | postgres | postgres | 今と同じ（既定） |
| postgres | dynamodb / mongodb | postgres / dynamodb / mongodb | Phase 1 |
| postgres（RLS あり） | dynamodb（テナント別テーブル） | postgres（RLS あり） | §5.2。フォークで実装済み |
| mongodb | mongodb | mongodb | Phase 2。Postgres 不要 |
| mongodb | dynamodb | mongodb / dynamodb | Phase 2 |

DynamoDB でメタデータを持たない理由: thread や assistant の検索は「metadata に任意のキーと値が含まれるもの」を
並び替えてページ送りする。DynamoDB では、これを全件読み込み（Scan）以外で実現できない。

---

## 6. 他の案と、選ばなかった理由

- **SQLAlchemy の方言で吸収する（MySQL などを足す）**: SQL の DB 同士なら近いが、DynamoDB と MongoDB には使えない。
  JSONB の包含検索も方言ごとに違うので、結局フィルタの変換は要る
- **全部を一度に抽象化する**: 変更が大きすぎて、レビューもテストも追いつかない。
  メンテナーも「範囲を絞った提案のほうが通る」と書いている（#331）
- **本体の外のプラグインだけにする（entry point で外部パッケージを読み込む）**: 本体の保守は軽くなるが、
  ユーザー分離を保証するテストが本体の CI で回らない。分離は譲れない条件なので、少なくとも共通テストは本体に置く（§9）
- **テナント別テーブルではなく、1つの DynamoDB テーブルのキーにテナントを入れる**: テーブルが1つで済むが、
  IAM で会社ごとに区切れず（キーの先頭一致の条件は書けるが、鍵とバックアップは分けられない）、§2 の利点が無くなる

---

## 7. テストの方針

**共通テスト（契約テスト）を1つ書き、すべての保存先で同じものを回す。**

| 区分 | 中身 |
|---|---|
| 基本操作 | 作成・取得・更新・削除・検索・ページ送り・並び順 |
| フィルタ | 認可フィルタと検索フィルタが、どの保存先でも同じ行を返す |
| **ユーザー分離** | 他人の ID を指定した読み書き・削除・検索がすべて「無い」扱いになる。GHSA-m98r-6667-4wq7、#424、#431 の手口（本文の値で別人のデータに行き先を変える）を、保存先ごとにテストとして固定する |
| 調停 | 2つのワーカーが同じ run を取り合うと片方だけが取れる。リースが切れた run が回収される。cron が1回だけ発火する |
| チェックポイント・store | LangGraph が配っている saver / store の共通テストに加えて、Aegra が使う `aprune` と削除の動き |
| テナント別テーブル（§5.2） | 2社で run を流し、相手の state/history/削除は 404、自分のテーブルにだけアイテムがある。テーブルの無い会社は 403。system スコープで振り分けを呼ぶとエラー |

**CI**

- Postgres: 今のテストを全部（変更なし）
- DynamoDB Local・MongoDB（レプリカセット1台のコンテナ）: 共通テストと、run を1本流す E2E
- リポジトリのインターフェースを変える PR は、すべての保存先で共通テストが通らないとマージできない

**保存先ごとにしか確かめられないこと**

- DynamoDB の IAM（会社 A の認証情報で B のテーブルが `AccessDenied` になること）は、DynamoDB Local では評価されない。
  実際の AWS での確認は手動になる（フォークでも未実施）
- MongoDB Atlas の Vector Search は、ローカルのコンテナでは動かない。意味検索は Atlas に対する手動確認になる

---

## 8. 段階ごとの PR の分け方

1. リファクタリング: チェックポイントと store を作る所を関数1つにまとめる。ヘルスチェックを保存先ごとの確認にする。動きは変えない
2. Phase 1（DynamoDB）: `AEGRA_CHECKPOINT_BACKEND` と追加インストール `aegra-api[dynamodb]`、`aprune` を足したサブクラス。共通テスト（チェックポイントの分）
3. テナント別テーブル（§5.2）: 振り分けチェックポインタと STS。RLS RFC の PR が入った後
4. Phase 1（MongoDB）と `AEGRA_STORE_BACKEND`
5. リファクタリング: メタデータのリポジトリを入れ、Postgres の実装に今のクエリを移す。動きは変えない。ここが一番大きい
6. 認可フィルタの2段化
7. Phase 2: MongoDB のメタデータと調停。共通テスト（メタデータ・分離・調停）

1〜4 は 5〜7 を待たずに出せる。5 と 6 は MongoDB が入らなくても、
「持ち主の指定を型で強制する」という分離の強化として単独で意味がある。

---

## 9. 保守（誰が直すか）

メンテナーが #331 で最も気にしている点（「入れた時点では動くが、そのうち半分壊れたまま放置される」）への答え。

- Postgres 以外の実装ごとに担当者を決め、CODEOWNERS に書く。担当: （記入）
- 共通テストが main で落ちたまま次のリリースを迎えた保存先は、「実験的」と表示し、
  その次のマイナーリリースまでに直らなければ外す。この取り決めを docs に書く
- 担当者が抜けるときは、その保存先を「実験的」に下げる

---

## 10. 未決事項

- 担当者（§9）。社内で決める
- テナント分離なしで `dynamodb` を選んだときの形。フォークの実装は RLS を必須にしている。
  本家に出すなら、RLS なしでは1つのテーブル、ありならテナント別、とするのが自然
- 1チェックポイントあたりの DynamoDB の書き込み量（オンデマンド課金）と性能。
  DynamoDB 版は同期の boto3 をスレッドプールで回すので、同時実行がスレッド数で頭打ちになる。未計測
- 孤児の突き合わせジョブ（§5.2）で、見つけたものを自動で消すか、報告だけにするか
- Docker イメージは追加インストールを入れないので、`dynamodb` で動かすイメージには追加が要る
- DynamoDB の store を Phase 1 に含めるか。意味検索が無いので、需要を確かめてから
- MongoDB の意味検索を、Atlas Vector Search 前提にするか、意味検索なしで始めるか
- MongoDB のメタデータでテナントを分けるときの形（§5.3）
- 1件ずつ取る回収（§5.3）が、大量の run を抱える環境で間に合うか。性能の確認が要る
- `assistant_versions` と `assistant` の更新を、MongoDB でも同じトランザクションにまとめられるか
- #312（グラフの実行を別プロセスに分ける案）が入ったとき、チェックポイントを持つのが本体か実行側か。
  Phase 1 の関数は「saver を作る」だけにしておき、どちらにも置けるようにする

---

## 付録: 関連

- aegra/aegra#331 この RFC が答える issue
- aegra/aegra#312 グラフ実行の差し替え（保存先とは別の軸）
- aegra/aegra#327 `POSTGRES_SCHEMA`（Postgres の中でのスキーマ分け）
- aegra/aegra#489 持ち主を `identity` と別に決める案（§5.3 の `OwnerScope` の値の決め方）
- aegra/aegra#431、#424、GHSA-m98r-6667-4wq7 持ち主チェックの漏れによる脆弱性（§7 の分離テストの元）
- aegra/aegra#707、#709 スレッド TTL の履歴削除が `aprune` 経由になった
- `dev-docs/rfc-tenant-rls.md` テナント分離の RFC（§5.2 の前提）
- `dev-docs/tenant-dynamodb-checkpoints-proposal.md` テナント別テーブルの設計の詳細
