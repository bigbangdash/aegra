"""Cross-tenant isolation through a real server with AEGRA_TENANT_RLS_ENABLED.

Needs the stack from docker-compose.tenant-rls.yml started with the flag on and
the enable step applied; set AEGRA_E2E_TENANT_RLS=1 to run. Uses the stress_test
graph (no LLM) so it runs without API keys, in both dev and prod mode.

The same user id is reused across tenants on purpose: the existing
user_id == identity filters would let those requests through, so any
isolation observed here comes from the tenant boundary.
"""

import json
import os
import uuid
from typing import Any

import httpx
import pytest
import redis.asyncio as aioredis
from langgraph_sdk import get_client

from aegra_api.settings import settings

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.skipif(
        os.getenv("AEGRA_E2E_TENANT_RLS") != "1",
        reason="needs a tenant-RLS server (docker-compose.tenant-rls.yml) and AEGRA_E2E_TENANT_RLS=1",
    ),
]

SHARED_USER = "shared-user"
RUN_INPUT = {"messages": [{"role": "user", "content": json.dumps({"delay": 0.1, "steps": 2})}]}


def _client(tenant: str, user: str = SHARED_USER) -> Any:
    return get_client(url=settings.app.SERVER_URL, headers={"x-tenant-id": tenant, "x-user-id": user})


def _tenant() -> str:
    return f"tenant-{uuid.uuid4().hex[:8]}"


async def _thread_with_completed_run(client: Any) -> str:
    assistant = await client.assistants.create(graph_id="stress_test", if_exists="do_nothing")
    thread = await client.threads.create()
    await client.runs.wait(thread["thread_id"], assistant["assistant_id"], input=RUN_INPUT)
    return thread["thread_id"]


async def _status_of(call: Any) -> int:
    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        await call
    return exc_info.value.response.status_code


@pytest.mark.asyncio
async def test_run_executes_and_checkpoints_under_tenant_scope() -> None:
    client = _client(_tenant())

    thread_id = await _thread_with_completed_run(client)
    state = await client.threads.get_state(thread_id)

    ai_content = json.loads(state["values"]["messages"][-1]["content"])
    assert ai_content["status"] == "completed"


@pytest.mark.asyncio
async def test_same_user_id_in_another_tenant_cannot_reach_thread() -> None:
    owner = _client(_tenant())
    intruder = _client(_tenant())
    thread_id = await _thread_with_completed_run(owner)

    assert await _status_of(intruder.threads.get(thread_id)) == 404
    assert await _status_of(intruder.threads.get_state(thread_id)) == 404
    assert await _status_of(intruder.threads.get_history(thread_id)) == 404
    # list_runs answers an unknown thread with [] rather than 404; with the same
    # user id, only the tenant boundary keeps the owner's run out of it.
    assert await intruder.runs.list(thread_id) == []
    found = await intruder.threads.search(limit=100)
    assert thread_id not in {t["thread_id"] for t in found}


@pytest.mark.asyncio
async def test_run_on_another_tenants_thread_id_is_not_found() -> None:
    owner = _client(_tenant())
    intruder = _client(_tenant())
    thread_id = await _thread_with_completed_run(owner)
    assistant = await intruder.assistants.create(graph_id="stress_test", if_exists="do_nothing")

    status = await _status_of(intruder.runs.create(thread_id, assistant["assistant_id"], input=RUN_INPUT))

    assert status == 404
    owner_runs = await owner.runs.list(thread_id)
    assert len(owner_runs) == 1


@pytest.mark.asyncio
async def test_delete_from_another_tenant_leaves_thread_intact() -> None:
    owner = _client(_tenant())
    intruder = _client(_tenant())
    thread_id = await _thread_with_completed_run(owner)

    assert await _status_of(intruder.threads.delete(thread_id)) == 404

    state = await owner.threads.get_state(thread_id)
    assert state["values"]["messages"]


@pytest.mark.asyncio
async def test_store_items_are_isolated_across_tenants() -> None:
    owner = _client(_tenant())
    intruder = _client(_tenant())
    namespace = ["notes", uuid.uuid4().hex[:8]]

    await owner.store.put_item(namespace, key="pref", value={"lang": "ja"})

    assert await _status_of(intruder.store.get_item(namespace, key="pref")) == 404
    listed = await intruder.store.search_items(namespace)
    assert listed["items"] == []
    mine = await owner.store.get_item(namespace, key="pref")
    assert mine["value"] == {"lang": "ja"}


@pytest.mark.asyncio
async def test_same_user_id_in_another_tenant_cannot_reach_custom_assistant() -> None:
    owner = _client(_tenant())
    intruder = _client(_tenant())
    config = {"configurable": {"system_prompt": f"secret-{uuid.uuid4().hex}"}}

    mine = await owner.assistants.create(graph_id="stress_test", config=config, if_exists="do_nothing")
    theirs = await intruder.assistants.create(graph_id="stress_test", config=config, if_exists="do_nothing")

    # Identical (user_id, graph_id, config) in another tenant gets its own row, not the owner's.
    assert theirs["assistant_id"] != mine["assistant_id"]
    assert await _status_of(intruder.assistants.get(mine["assistant_id"])) == 404
    listed = await intruder.assistants.search(limit=100)
    assert mine["assistant_id"] not in {a["assistant_id"] for a in listed}


@pytest.mark.asyncio
async def test_shared_default_assistant_is_usable_by_every_tenant() -> None:
    for client in (_client(_tenant()), _client(_tenant())):
        found = await client.assistants.search(graph_id="stress_test", metadata={"created_by": "system"}, limit=10)
        assert len(found) == 1
        thread = await client.threads.create()
        result = await client.runs.wait(thread["thread_id"], found[0]["assistant_id"], input=RUN_INPUT)
        assert result["messages"]


async def _run_store_probe(client: Any, *, namespace: list[str], key: str, value: str) -> dict[str, Any]:
    assistant = await client.assistants.create(graph_id="tenant_store_probe", if_exists="do_nothing")
    thread = await client.threads.create()
    probe_input = {"namespace": namespace, "key": key, "value": value}
    return await client.runs.wait(thread["thread_id"], assistant["assistant_id"], input=probe_input)


@pytest.mark.asyncio
async def test_graph_store_writes_with_same_key_stay_separate_per_tenant() -> None:
    # Same namespace and key from two tenants: without the tenant prefix the
    # second write hits the first tenant's hidden row and the run fails.
    namespace = ["memories", uuid.uuid4().hex[:8]]
    tenant_a, tenant_b = _client(_tenant()), _client(_tenant())

    result_a = await _run_store_probe(tenant_a, namespace=namespace, key="pref", value="a")
    result_b = await _run_store_probe(tenant_b, namespace=namespace, key="pref", value="b")
    again_a = await _run_store_probe(tenant_a, namespace=namespace, key="other", value="a2")

    assert (result_a["async_read"], result_a["sync_read"]) == ("a", "a")
    assert (result_b["async_read"], result_b["sync_read"]) == ("b", "b")
    assert again_a["async_read"] == "a2"


@pytest.mark.asyncio
async def test_graph_store_writes_are_visible_to_own_tenant_through_http_api_only() -> None:
    # The HTTP API buries namespaces under ["users", identity]; a graph writing
    # that path must land in the same tenant-prefixed rows the API reads.
    suffix = uuid.uuid4().hex[:8]
    owner, intruder = _client(_tenant()), _client(_tenant())

    await _run_store_probe(owner, namespace=["users", SHARED_USER, "graph", suffix], key="k", value="from-graph")

    mine = await owner.store.get_item(["graph", suffix], key="k")
    assert mine["value"] == {"value": "from-graph"}
    assert mine["namespace"] == ["users", SHARED_USER, "graph", suffix]
    assert await _status_of(intruder.store.get_item(["graph", suffix], key="k")) == 404


@pytest.mark.prod_only
@pytest.mark.asyncio
async def test_redis_event_buffer_holds_only_sealed_payloads() -> None:
    marker = f"tenant-secret-{uuid.uuid4().hex}"
    owner, intruder = _client(_tenant()), _client(_tenant())
    assistant = await owner.assistants.create(graph_id="stress_test", if_exists="do_nothing")
    thread = await owner.threads.create()
    marked_input = {"messages": [{"role": "user", "content": json.dumps({"delay": 0.1, "steps": 2, "marker": marker})}]}
    run = await owner.runs.create(thread["thread_id"], assistant["assistant_id"], input=marked_input)
    await owner.runs.join(thread["thread_id"], run["run_id"])

    # A terminal run replays only with a Last-Event-ID; one absent from the buffer replays everything.
    replayed = [
        part
        async for part in owner.runs.join_stream(
            thread["thread_id"], run["run_id"], last_event_id=f"{run['run_id']}_event_0"
        )
    ]
    assert not any(part.event == "error" for part in replayed), replayed
    assert any(marker in json.dumps(part.data) for part in replayed)

    redis_client = aioredis.from_url(settings.redis.REDIS_URL, decode_responses=True)
    try:
        raw = await redis_client.lrange(f"{settings.redis.REDIS_CHANNEL_PREFIX}cache:{run['run_id']}", 0, -1)
    finally:
        await redis_client.aclose()
    assert raw, "run left no replay buffer in Redis"
    assert all(set(json.loads(item)) == {"event_id", "end", "sealed"} for item in raw)
    assert not any(marker in item for item in raw)

    assert await _status_of(intruder.runs.join(thread["thread_id"], run["run_id"])) == 404


@pytest.mark.asyncio
async def test_tenant_rejected_by_the_configured_resolver_gets_403() -> None:
    # examples/tenant_header_auth_example.py installs a registry that rejects "e2e-inactive".
    client = _client("e2e-inactive")

    create_status = await _status_of(client.threads.create())
    search_status = await _status_of(client.threads.search())

    assert create_status == 403
    assert search_status == 403
