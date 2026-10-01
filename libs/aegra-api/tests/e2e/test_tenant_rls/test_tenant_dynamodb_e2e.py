"""Per-tenant DynamoDB checkpoints through a real server (proposal §7).

Needs docker-compose.tenant-dynamodb.yml (Postgres 5446, DynamoDB Local 8100) and a
server started with AEGRA_CHECKPOINT_BACKEND=dynamodb against it; set
AEGRA_E2E_TENANT_DYNAMODB=1 to run. Each test provisions its own tenant tables the
way IaC would, then checks that a tenant's checkpoints land only in its own table.
Uses the stress_test graph (no LLM) so it runs without API keys.
"""

import asyncio
import json
import os
import uuid
from collections.abc import Iterator
from typing import Any

import httpx
import pytest
from langgraph_sdk import get_client

from aegra_api.settings import settings

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.skipif(
        os.getenv("AEGRA_E2E_TENANT_DYNAMODB") != "1",
        reason="needs a dynamodb-backend server (docker-compose.tenant-dynamodb.yml) and AEGRA_E2E_TENANT_DYNAMODB=1",
    ),
]

boto3 = pytest.importorskip("boto3")

SHARED_USER = "shared-user"
RUN_INPUT = {"messages": [{"role": "user", "content": json.dumps({"delay": 0.1, "steps": 2})}]}
ENDPOINT_URL = os.getenv("AEGRA_DYNAMODB_ENDPOINT_URL", "http://localhost:8100")
REGION = os.getenv("AEGRA_DYNAMODB_REGION", "us-east-1")
TABLE_PREFIX = os.getenv("AEGRA_DYNAMODB_TABLE_PREFIX", "aegra-ckpt-")


def _dynamodb() -> Any:
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "local")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "local")
    return boto3.client("dynamodb", region_name=REGION, endpoint_url=ENDPOINT_URL)


def _client(tenant: str, user: str = SHARED_USER) -> Any:
    return get_client(url=settings.app.SERVER_URL, headers={"x-tenant-id": tenant, "x-user-id": user})


def _tenant() -> str:
    return f"tenant-{uuid.uuid4().hex[:8]}"


def _table(tenant: str) -> str:
    return f"{TABLE_PREFIX}{tenant}"


def _items_for_thread(tenant: str, thread_id: str) -> list[dict[str, Any]]:
    # Every item of a thread carries the thread id in its PK (CHECKPOINT_/WRITES_/CHUNK_ prefixes).
    items: list[dict[str, Any]] = []
    params: dict[str, Any] = {
        "TableName": _table(tenant),
        "FilterExpression": "contains(PK, :tid)",
        "ExpressionAttributeValues": {":tid": {"S": thread_id}},
        "ProjectionExpression": "PK, SK",
    }
    while True:
        page = _dynamodb().scan(**params)
        items.extend(page.get("Items", []))
        if "LastEvaluatedKey" not in page:
            return items
        params["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def _create_table(dynamodb: Any, table_name: str) -> None:
    # Same shape as scripts/tenant_dynamodb_tables.py and the IaC in production (proposal §6.2).
    dynamodb.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "PK", "KeyType": "HASH"}, {"AttributeName": "SK", "KeyType": "RANGE"}],
        AttributeDefinitions=[
            {"AttributeName": "PK", "AttributeType": "S"},
            {"AttributeName": "SK", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    dynamodb.get_waiter("table_exists").wait(TableName=table_name)


@pytest.fixture
def tenants() -> Iterator[tuple[str, str]]:
    """Two provisioned tenants; tables are created like IaC would and dropped afterwards."""
    pair = (_tenant(), _tenant())
    dynamodb = _dynamodb()
    for tenant in pair:
        _create_table(dynamodb, _table(tenant))
    yield pair
    for tenant in pair:
        dynamodb.delete_table(TableName=_table(tenant))


async def _thread_with_completed_run(client: Any, **thread_kwargs: Any) -> str:
    assistant = await client.assistants.create(graph_id="stress_test", if_exists="do_nothing")
    thread = await client.threads.create(**thread_kwargs)
    run = await client.runs.create(thread["thread_id"], assistant["assistant_id"], input=RUN_INPUT)
    await client.runs.join(thread["thread_id"], run["run_id"])
    finished = await client.runs.get(thread["thread_id"], run["run_id"])
    assert finished["status"] == "success", f"run ended as {finished['status']}"
    return thread["thread_id"]


async def _status_of(call: Any) -> int:
    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        await call
    return exc_info.value.response.status_code


@pytest.mark.asyncio
async def test_health_reports_the_dynamodb_backend_connected() -> None:
    response = httpx.get(f"{settings.app.SERVER_URL}/health", timeout=10.0)

    assert response.status_code == 200, response.text
    assert response.json()["langgraph_checkpointer"] == "connected"


@pytest.mark.asyncio
async def test_each_tenant_checkpoints_into_its_own_table(tenants: tuple[str, str]) -> None:
    tenant_a, tenant_b = tenants
    client_a, client_b = _client(tenant_a), _client(tenant_b)

    thread_a = await _thread_with_completed_run(client_a)
    thread_b = await _thread_with_completed_run(client_b)

    state_a = await client_a.threads.get_state(thread_a)
    assert json.loads(state_a["values"]["messages"][-1]["content"])["status"] == "completed"
    assert len(await client_a.threads.get_history(thread_a)) > 1

    assert _items_for_thread(tenant_a, thread_a), "tenant A's run left no items in its table"
    assert _items_for_thread(tenant_b, thread_b), "tenant B's run left no items in its table"
    assert _items_for_thread(tenant_b, thread_a) == []
    assert _items_for_thread(tenant_a, thread_b) == []


@pytest.mark.asyncio
async def test_other_tenant_cannot_read_state_even_with_the_same_user(tenants: tuple[str, str]) -> None:
    tenant_a, tenant_b = tenants
    owner, intruder = _client(tenant_a), _client(tenant_b)
    thread_id = await _thread_with_completed_run(owner)

    assert await _status_of(intruder.threads.get_state(thread_id)) == 404
    assert await _status_of(intruder.threads.get_history(thread_id)) == 404
    assert await _status_of(intruder.threads.delete(thread_id)) == 404
    assert _items_for_thread(tenant_a, thread_id), "the owner's checkpoints must survive the intruder"


@pytest.mark.asyncio
async def test_deleting_a_thread_clears_only_its_own_table(tenants: tuple[str, str]) -> None:
    tenant_a, tenant_b = tenants
    client_a, client_b = _client(tenant_a), _client(tenant_b)
    thread_a = await _thread_with_completed_run(client_a)
    thread_b = await _thread_with_completed_run(client_b)

    await client_a.threads.delete(thread_a)

    assert await _status_of(client_a.threads.get(thread_a)) == 404
    assert _items_for_thread(tenant_a, thread_a) == []
    assert _items_for_thread(tenant_b, thread_b), "tenant B's items must be untouched"


@pytest.mark.asyncio
async def test_ttl_sweep_removes_expired_checkpoints_from_the_tenant_table_only(tenants: tuple[str, str]) -> None:
    # /threads/prune drives the same claim-and-apply path as the background sweep.
    tenant_a, tenant_b = tenants
    client_a, client_b = _client(tenant_a), _client(tenant_b)
    expired_a = await _thread_with_completed_run(client_a, ttl=0.001)
    kept_a = await _thread_with_completed_run(client_a)
    thread_b = await _thread_with_completed_run(client_b)

    await asyncio.sleep(1.0)
    response = httpx.post(
        f"{settings.app.SERVER_URL}/threads/prune",
        headers={"x-tenant-id": tenant_a, "x-user-id": SHARED_USER},
        timeout=30.0,
    )

    assert response.status_code == 200, response.text
    assert response.json()["deleted"] >= 1
    assert await _status_of(client_a.threads.get(expired_a)) == 404
    assert _items_for_thread(tenant_a, expired_a) == []
    assert _items_for_thread(tenant_a, kept_a), "an unexpired thread in the same tenant must keep its items"
    assert _items_for_thread(tenant_b, thread_b), "another tenant's items must be untouched"


@pytest.mark.asyncio
async def test_keep_latest_prunes_history_in_the_tenant_table(tenants: tuple[str, str]) -> None:
    tenant_a, _ = tenants
    client = _client(tenant_a)
    thread_id = await _thread_with_completed_run(client, ttl={"ttl": 0.001, "strategy": "keep_latest"})
    state_before = await client.threads.get_state(thread_id)
    assert len(await client.threads.get_history(thread_id)) > 1

    await asyncio.sleep(1.0)
    response = httpx.post(
        f"{settings.app.SERVER_URL}/threads/prune",
        headers={"x-tenant-id": tenant_a, "x-user-id": SHARED_USER},
        timeout=30.0,
    )

    assert response.status_code == 200, response.text
    assert response.json()["pruned"] >= 1
    assert len(await client.threads.get_history(thread_id)) == 1
    assert (await client.threads.get_state(thread_id))["values"] == state_before["values"]
    # The compacted state is usable: a second run resumes from it.
    assistant = await client.assistants.create(graph_id="stress_test", if_exists="do_nothing")
    run = await client.runs.create(thread_id, assistant["assistant_id"], input=RUN_INPUT)
    await client.runs.join(thread_id, run["run_id"])
    assert (await client.runs.get(thread_id, run["run_id"]))["status"] == "success"


@pytest.mark.asyncio
async def test_tenant_without_a_table_gets_403_not_500() -> None:
    client = _client(_tenant())  # no table was created for this tenant
    assistant = await client.assistants.create(graph_id="stress_test", if_exists="do_nothing")
    thread = await client.threads.create()  # Postgres only: still works
    run = await client.runs.create(thread["thread_id"], assistant["assistant_id"], input=RUN_INPUT)
    await client.runs.join(thread["thread_id"], run["run_id"])

    finished = await client.runs.get(thread["thread_id"], run["run_id"])
    state_status = await _status_of(client.threads.get_state(thread["thread_id"]))
    history_status = await _status_of(client.threads.get_history(thread["thread_id"]))

    assert finished["status"] == "error"
    assert state_status == 403
    assert history_status == 403
    # Delete goes through the checkpointer first, so the thread row survives until the tenant is provisioned.
    assert await _status_of(client.threads.delete(thread["thread_id"])) == 403
    assert (await client.threads.get(thread["thread_id"]))["thread_id"] == thread["thread_id"]
