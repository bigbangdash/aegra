# Tenant RLS — Maintainer / Debug Guide

Internal engineering notes for `AEGRA_TENANT_RLS_ENABLED`. NOT user-facing; the
user guide is `docs/guides/tenant-isolation.mdx`. Read this before touching
anything that opens a DB connection, writes to Redis, or spawns a background task.

---

## 1. One-paragraph model

Every unit of work declares a **DB scope** in a contextvar: one tenant, or the
system with a written reason. The scope is resolved at the edge (HTTP dependency,
`execute_run`, each cron fire) and every connection checkout reads it. A tenant
scope switches the connection to the `aegra_tenant` role and sets the GUC
`aegra.tenant_id`; PostgreSQL RLS policies compare rows against that GUC. A
system scope keeps the login role, which owns the tables and so bypasses RLS.
**No scope is an error, never a bypass.** Redis, which RLS cannot reach, gets the
same boundary by encrypting event payloads with a per-tenant key. With the flag
off none of this runs: no scope is required and every format is unchanged.

---

## 2. Threat model (and what is out of scope)

In scope: **an application bug that crosses tenants** — a missing `WHERE`, a
wrong id from a path param, a background job that forgets which tenant it
serves. The failure mode we want is "no rows / 404 / decrypt error", never
another tenant's data.

Out of scope: a compromised server process. It holds the DB login (table
owner) and the Redis master key. Also out of scope: isolating tenants from the
operator.

Design consequence: every guard **fails closed** and every bypass is named.

---

## 3. Scope: who declares it

```
core/db_scope.py
  tenant_scope(tenant_id)   validates ^[A-Za-z0-9_-]{1,64}$ else ValueError
  system_scope(reason)      reason mandatory → greppable audit trail
  bind_scope(scope)         re-enter a captured scope (thread → loop hop)
  current_db_scope()        raises DbScopeMissingError when nothing declared
```

Entry points (the ONLY places a tenant is chosen):

| Where | Tenant comes from |
|-------|-------------------|
| `core/tenant.py::tenant_db_scope` — router dependency on threads, runs, stateless runs, assistants, crons, store, event streaming | `user.org_id` from the auth handler. Missing or malformed → 403 |
| `services/run_executor.py::execute_run` | `job.user.org_id` (same resolver). Overrides the worker loop's system scope |
| `services/cron_scheduler.py::_tick` per cron | `cron.tenant_id` stored at create time. Malformed legacy value → that cron is skipped and logged, batch continues |

System scopes (`grep 'system_scope("'`):

```
LangGraph schema setup at startup
startup: sync default assistants from aegra.json
health probes
cron scheduler: claim due crons; each fire re-scopes to its tenant
worker loop: queue and lease management
worker shutdown: requeue drained runs
lease reaper: cross-tenant crash recovery
thread TTL sweeper: cross-tenant expiry
```

Contextvar gotchas that already bit us:

- The FastAPI dependency **must be async**. A sync generator dependency runs in
  a worker thread and its contextvar never reaches the endpoint.
- Streaming works because FastAPI (0.138) runs yield-dependency teardown after
  the response is sent, so the SSE generator still sees the tenant scope.
- Sync graph nodes call the store from a worker thread → `TenantScopedPostgresStore.batch`
  captures the scope and `bind_scope`s it on the event loop.
- Background tasks inherit the scope that was active at `create_task`. Loops
  are created inside their `system_scope`, and `execute_run` re-scopes per job.

---

## 4. How the scope reaches PostgreSQL

Two pools, two mechanisms, same effect:

```
SQLAlchemy (asyncpg)                        LangGraph pool (psycopg, autocommit)
core/tenant_session.py                      core/tenant_pool.py
  after_begin on every transaction:           connection():
    SET LOCAL ROLE aegra_tenant                 scope resolved BEFORE checkout
    set_config('aegra.tenant_id', t, true)      tenant → wrap the whole checkout in
                                                  one transaction, SET LOCAL ROLE,
                                                  set_config(..., true)
```

- `SET LOCAL` dies at COMMIT and AsyncSession autobegins after every commit,
  hence `after_begin` instead of once per request.
- The LangGraph pool is autocommit, so there is no transaction to hang
  `SET LOCAL` on; the checkout opens one explicitly. Commit/rollback clears role
  and GUC before the connection returns to the pool — no leakage between checkouts.
- System scope: no role switch, no GUC. The login role owns the tables, and
  RLS without `FORCE` does not apply to the owner. **Requirement: the server's
  login role must own the tables (or have BYPASSRLS).** If it does not, every
  system-scope job (reaper, TTL sweeper, cron claim) silently sees zero rows.
  The regular E2E stack logs in as a superuser (postgres image default). The
  non-superuser-owner path was verified manually (2026-09-30): full E2E plus
  cron firing, lease reaper recovery and TTL sweeping, all passing.
- Policies read `NULLIF(current_setting('aegra.tenant_id', true), '')`. The
  NULLIF matters: a pooled connection that ever set the GUC reports `''`
  afterwards, and `'' = ''` would match untagged rows.

---

## 5. Schema and policies

Alembic adds nullable `tenant_id` columns and indexes only. Roles, NOT NULL,
defaults and policies live in `core/tenant_rls.py::enable_tenant_rls`, run by the
operator (`aegra db enable-tenant-rls`), because LangGraph creates its tables in
`setup()` after alembic runs, and because flag-off installs must stay unaffected.

| Table group | Policy |
|-------------|--------|
| `thread`, `runs`, `crons`, `checkpoints`, `checkpoint_blobs`, `checkpoint_writes`, `store` | `tenant_id = current tenant` for read and write. Column `NOT NULL`, default = current tenant, so writers never pass it explicitly |
| `store_vectors` (only with semantic search) | Same, applied only if the table exists → re-run enable after enabling semantic search |
| `assistant` (shared) | Read: own tenant OR `tenant_id IS NULL AND user_id = 'system'`. Write: own tenant only. CHECK `tenant_id IS NOT NULL OR user_id = 'system'` stops a tenant row from becoming shared |
| `assistant_versions` (child) | Visible/writable when the parent assistant is |
| `thread_ttl` | GRANT only; touched inside tenant transactions, holds no tenant content |

Shared assistants are the graphs from `aegra.json`, synced at startup under
system scope. Every tenant can run them; none can edit them.

Keys that span tenants:

- Assistant uniqueness index is now `(COALESCE(tenant_id, ''), user_id, graph_id, md5(config))`.
  Otherwise the same user id in two tenants collided on a row RLS hides.
- `thread_pkey` is global while reads are RLS-scoped: another tenant's thread
  looks absent. Thread upsert uses `INSERT ... ON CONFLICT DO NOTHING RETURNING`
  and returns 404 when the key belongs to someone invisible, instead of 500.

---

## 6. Store: the hidden namespace prefix

`store`'s primary key is `(prefix, key)` across tenants. Two tenants writing
the same namespace/key would collide on a row RLS hides — the write fails and
reveals that the key exists. `core/tenant_store.py` stores tenant ops under
`("aegra_tenant", tenant_id, *namespace)` and strips it on read. Graphs and the
HTTP API use the same store instance, so both see only their own namespace.

- System scope passes through and sees the physical layout.
- Upstream batches ops from many callers on one background task (would mix
  scopes). We run every op inline and serialize with a lock: `abatch` holds one
  checkout while `_cursor` takes a second, and unbounded concurrency deadlocks the pool.
- Tenant ids must not contain `.`: the store joins namespace labels with dots.
  The global id format already excludes it.

---

## 7. Redis: per-tenant encryption

RLS cannot see Redis, so run events (the replay list `cache:{run_id}` and the
pub/sub channel) are encrypted. Only `services/redis_broker.py` changes; the
in-memory broker (dev mode) is in-process and untouched.

```
flag on, message on the wire:
  {"event_id": "...", "end": false, "sealed": "v1.<keyver>.<b64(nonce ‖ ct)>"}
  AES-256-GCM, AAD = tenant_id \x1f run_id \x1f event_id \x1f keyver
```

- Plaintext stays: `event_id` (dedupe, Last-Event-ID) and `end` (the broker
  stops and `_check_end_in_buffer` works without a key). The event type is inside
  the ciphertext. Counters, done keys, the queue and cancel messages hold only ids.
- **The key is chosen by the caller's own scope** (`tenant_crypto.encryption_tenant()`),
  never by anything read from Redis or the run row. A bug that hands a reader
  another tenant's run_id gets `TenantPayloadError`, not data.
- System scope or no scope → error, for both read and write. No system path
  reads event payloads.
- Flag on + unsealed message, or flag off + sealed message → error, not skip.
- The cancel listener has no scope. It writes the `end` event under the run's
  tenant from `core/active_runs.active_run_tenants`, which `execute_run`
  fills and clears. `Task.get_context()` does not work: the worker runs the job in
  a child task and the outer task holds the worker loop's system scope. If the
  tenant is unknown, the listener logs and skips; the run's own cancel path still
  emits `end` under its tenant.

Keys: `TenantKeyProvider.get_key(tenant, version)`. Default `StaticKeyProvider`
derives per-tenant keys from `AEGRA_TENANT_REDIS_MASTER_KEY` with HKDF (version
`s1`). `configure_key_provider()` is the hook for a KMS-backed provider (planned
as a separate package). It must be called at import time: `main.py` checks the
key at startup, before any user lifespan runs. Flag on + Redis broker + no key →
the server refuses to start.

---

## 8. Enabling and switching

```
1. set AEGRA_TENANT_RLS_ENABLED=true (+ master key if REDIS_BROKER_ENABLED)
2. start the server once          → migrations + LangGraph setup() create tables
3. aegra db enable-tenant-rls     → role, grants, NOT NULL, defaults, policies
```

- Step 3 refuses to run with the flag off. After step 3 a flag-off server fails
  every write (NOT NULL `tenant_id` it cannot fill). Do not flip the flag back.
- Step 3 is idempotent (DROP ... IF EXISTS before each CREATE). Re-run it after
  enabling semantic search.
- New databases only. There is no backfill for rows without `tenant_id`.
- Switching the flag with Redis on: replay buffers live 600 s. Events written
  before the switch fail to decode after it. Drain runs, wait 600 s, then switch.

---

## 9. Deliberately NOT implemented (don't chase these as bugs)

- **Tenant registry.** `resolve_tenant_id` checks presence and format of
  `org_id`, not that the tenant exists or is active. Open question: core table
  vs. a resolver hook with the registry in a separate package.
- **KMS key provider.** Only the hook exists.
- **FORCE ROW LEVEL SECURITY.** System scope relies on owner bypass (§4).
- **Backfill / migrating an existing install.**
- **Plaintext tolerance during a flag switch.** Rejected: new deployments only.
- **Isolation in the in-memory broker.** Single process, never leaves memory.

---

## 10. Debug cheat-sheet

| Symptom | Meaning |
|---------|---------|
| `DbScopeMissingError` | A code path opened a connection (or touched Redis events) without a scope. Find the entry point; add `tenant_scope` or a named `system_scope`. Never add a silent default |
| `TenantPayloadError: ... does not belong to tenant X` | Reader's tenant differs from the writer's. A real cross-tenant attempt or a wrong scope upstream |
| `TenantPayloadError: unsealed event ... while tenant RLS is on` | Buffer written before the flag switch (§8) |
| `run events cannot be accessed from system scope` | A system job touched event payloads. Should never happen; see §7 |
| `violates check constraint "aegra_tenant_shared_rows_are_system"` | Insert without `tenant_id` into `assistant` outside the system user — usually a test seeding rows directly |
| `null value in column "tenant_id"` | Flag off after enable (§8), or a write under system scope into a tenant table |
| 404 for a thread id you know exists | It belongs to another tenant (by design) |
| System jobs see nothing | Login role does not own the tables (§4) |
| `Skipping cron with a malformed tenant_id` | Legacy row from before id validation; fix or delete the row |

---

## 11. Test map

| Level | Files |
|-------|-------|
| Unit — scope and resolver | `tests/unit/test_core/test_db_scope.py`, `test_tenant.py`, `test_background_db_scopes.py` |
| Unit — DB plumbing | `test_tenant_session.py`, `test_tenant_pool.py`, `test_tenant_rls.py`, `test_database_manager.py`, `test_tenant_store.py` |
| Unit — Redis | `test_tenant_crypto.py`, `tests/unit/test_services/test_redis_broker_tenant_encryption.py`, `test_run_executor.py` (tenant registry) |
| Unit — cron | `tests/unit/test_services/test_cron_scheduler.py` (malformed tenant skip) |
| Unit — CLI | `libs/aegra-cli/tests/test_db_tenant_rls.py` |
| Integration | `tests/integration/test_api/test_threads_tenant_scope.py` |
| E2E real DB, no server | `tests/e2e/test_tenant_rls/test_metadata_tenant_rls.py`, `test_langgraph_tenant_rls.py` (throwaway Postgres) |
| E2E server | `tests/e2e/test_tenant_rls/test_tenant_rls_api_e2e.py` with `docker-compose.tenant-rls.yml` (header-trusting test auth, test-only Redis key) and `AEGRA_E2E_TENANT_RLS=1`; the Redis test is `prod_only` |

Known E2E noise on the RLS stack, not regressions: `test_store::test_org_prefix_without_org_membership_is_forbidden`
and `test_run_reconciliation_e2e` assume noop auth and seed rows without
`tenant_id`; LLM-backed tests fail without API keys.
