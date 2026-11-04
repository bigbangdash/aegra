"""Store probe graph for the tenant RLS E2E: writes and reads back via the injected store.

One async and one sync node, so both store call paths run inside a real run.
"""

from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph.store.base import BaseStore


class State(TypedDict, total=False):
    namespace: list[str]
    key: str
    value: str
    async_read: str | None
    sync_read: str | None


async def write_async(state: State, *, store: BaseStore) -> dict[str, Any]:
    namespace = tuple(state["namespace"])
    await store.aput(namespace, state["key"], {"value": state["value"]})
    item = await store.aget(namespace, state["key"])
    return {"async_read": item.value["value"] if item else None}


def write_sync(state: State, *, store: BaseStore) -> dict[str, Any]:
    namespace = (*state["namespace"], "sync")
    store.put(namespace, state["key"], {"value": state["value"]})
    item = store.get(namespace, state["key"])
    return {"sync_read": item.value["value"] if item else None}


builder = StateGraph(State)
builder.add_node("write_async", write_async)
builder.add_node("write_sync", write_sync)
builder.add_edge(START, "write_async")
builder.add_edge("write_async", "write_sync")
builder.add_edge("write_sync", END)
graph = builder.compile()
