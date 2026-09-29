"""AsyncPostgresStore variant that keeps each call in its caller's DB scope.

AsyncBatchedBaseStore funnels every caller through one long-lived background
task and may merge ops from several callers into a single abatch() on one
connection. Under tenant RLS that would drop the caller's scope or mix
tenants, so every async op runs inline via BaseStore's abatch() path instead.

Tenant-scoped ops are also stored under [TENANT_NAMESPACE_ROOT, tenant_id, ...].
The store's primary key is (prefix, key) across all tenants, so without the
prefix two tenants using the same namespace and key would collide on a row
RLS hides from one of them: the write fails and leaks that the key exists.
Callers (graphs and the HTTP API alike) never see the prefix.
"""

import asyncio
from collections.abc import Iterable
from typing import Any

from langgraph.store.base import (
    BaseStore,
    GetOp,
    Item,
    ListNamespacesOp,
    MatchCondition,
    Op,
    PutOp,
    Result,
    SearchOp,
)
from langgraph.store.postgres.aio import AsyncPostgresStore

from aegra_api.core.db_scope import DbScope, bind_scope, current_db_scope

TENANT_NAMESPACE_ROOT = "aegra_tenant"


def tenant_namespace_prefix(tenant_id: str) -> tuple[str, str]:
    # The Postgres store joins namespace labels with '.', so a dot in the
    # tenant id would split into extra labels and break stripping on read.
    if "." in tenant_id:
        raise ValueError(f"tenant_id must not contain '.': {tenant_id!r}")
    return (TENANT_NAMESPACE_ROOT, tenant_id)


def _prefix_op(op: Op, head: tuple[str, ...]) -> Op:
    if isinstance(op, (GetOp, PutOp)):
        return op._replace(namespace=(*head, *op.namespace))
    if isinstance(op, SearchOp):
        return op._replace(namespace_prefix=(*head, *op.namespace_prefix))
    if isinstance(op, ListNamespacesOp):
        conditions = list(op.match_conditions or ())
        prefixed = [
            MatchCondition(match_type="prefix", path=(*head, *c.path)) for c in conditions if c.match_type == "prefix"
        ]
        others = [c for c in conditions if c.match_type != "prefix"]
        if not prefixed:
            prefixed = [MatchCondition(match_type="prefix", path=head)]
        max_depth = op.max_depth + len(head) if op.max_depth is not None else None
        return op._replace(match_conditions=tuple(prefixed + others), max_depth=max_depth)
    raise TypeError(f"Unknown store op: {type(op).__name__}")


def _strip(namespace: tuple[str, ...], head: tuple[str, ...]) -> tuple[str, ...]:
    # RLS already hides other tenants' rows; reaching this means the prefix
    # and the DB scope disagree, which must never be papered over.
    if namespace[: len(head)] != head:
        raise RuntimeError("Store returned a namespace outside the current tenant prefix")
    return namespace[len(head) :]


def _strip_result(result: Result, head: tuple[str, ...]) -> Result:
    if isinstance(result, Item):
        result.namespace = _strip(result.namespace, head)
        return result
    if isinstance(result, list):
        stripped: list[Any] = []
        for entry in result:
            if isinstance(entry, Item):
                entry.namespace = _strip(entry.namespace, head)
                stripped.append(entry)
            else:
                stripped.append(_strip(entry, head))
        return stripped
    return result


class TenantScopedPostgresStore(AsyncPostgresStore):
    aget = BaseStore.aget
    asearch = BaseStore.asearch
    aput = BaseStore.aput
    adelete = BaseStore.adelete
    alist_namespaces = BaseStore.alist_namespaces
    # Sync methods must funnel through batch() below, which carries the scope.
    get = BaseStore.get
    search = BaseStore.search
    put = BaseStore.put
    delete = BaseStore.delete
    list_namespaces = BaseStore.list_namespaces

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._abatch_lock = asyncio.Lock()

    async def abatch(self, ops: Iterable[Op]) -> list[Result]:
        tenant_id = current_db_scope().tenant_id
        if tenant_id is None:
            # System scope sees the physical layout; RLS and NOT NULL tenant_id
            # still stop it from writing rows no tenant owns.
            return await self._locked_abatch(ops)
        head = tenant_namespace_prefix(tenant_id)
        results = await self._locked_abatch([_prefix_op(op, head) for op in ops])
        return [_strip_result(result, head) for result in results]

    def batch(self, ops: Iterable[Op]) -> list[Result]:
        # Sync graph nodes call from a worker thread; the loop would run the
        # coroutine without the caller's contextvars, so carry the scope over.
        scope = current_db_scope()
        ops = list(ops)
        return asyncio.run_coroutine_threadsafe(self._abatch_in_scope(ops, scope), self.loop).result()

    async def _abatch_in_scope(self, ops: list[Op], scope: DbScope) -> list[Result]:
        with bind_scope(scope):
            return await self.abatch(ops)

    async def _locked_abatch(self, ops: Iterable[Op]) -> list[Result]:
        # abatch holds one pool checkout while _cursor takes a second; unbounded
        # concurrency deadlocks the pool. Upstream's single batch task serializes too.
        async with self._abatch_lock:
            return await super().abatch(ops)
