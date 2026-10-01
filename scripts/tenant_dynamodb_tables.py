"""Create per-tenant checkpoint tables on DynamoDB Local for AEGRA_CHECKPOINT_BACKEND=dynamodb.

In production the tables come from IaC (dev-docs/tenant-dynamodb-checkpoints-proposal.md §6.2);
the server never creates them. This script is the Local stand-in: one table per tenant id,
string keys PK (HASH) and SK (RANGE), TTL on the attribute `ttl`, as langgraph-checkpoint-aws
expects.

    AWS_ACCESS_KEY_ID=local AWS_SECRET_ACCESS_KEY=local \\
      uv run --package aegra-api python scripts/tenant_dynamodb_tables.py tenant-a tenant-b
    uv run --package aegra-api python scripts/tenant_dynamodb_tables.py --list
"""

import argparse
import os
import sys

import boto3
from botocore.exceptions import ClientError

from aegra_api.core.tenancy.scope import is_valid_tenant_id

DEFAULT_ENDPOINT = "http://localhost:8100"
DEFAULT_REGION = "us-east-1"
DEFAULT_PREFIX = "aegra-ckpt-"


def create_tenant_table(client: object, table_name: str) -> bool:
    """Create the table; returns False when it already exists."""
    dynamodb = client  # boto3 clients have no static type without the stubs extra
    try:
        dynamodb.create_table(  # type: ignore[attr-defined]
            TableName=table_name,
            KeySchema=[{"AttributeName": "PK", "KeyType": "HASH"}, {"AttributeName": "SK", "KeyType": "RANGE"}],
            AttributeDefinitions=[
                {"AttributeName": "PK", "AttributeType": "S"},
                {"AttributeName": "SK", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ResourceInUseException":
            return False
        raise
    dynamodb.get_waiter("table_exists").wait(TableName=table_name)  # type: ignore[attr-defined]
    dynamodb.update_time_to_live(  # type: ignore[attr-defined]
        TableName=table_name, TimeToLiveSpecification={"Enabled": True, "AttributeName": "ttl"}
    )
    return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("tenants", nargs="*", help="tenant ids to create tables for")
    parser.add_argument("--endpoint-url", default=os.getenv("AEGRA_DYNAMODB_ENDPOINT_URL", DEFAULT_ENDPOINT))
    parser.add_argument("--region", default=os.getenv("AEGRA_DYNAMODB_REGION", DEFAULT_REGION))
    parser.add_argument("--prefix", default=os.getenv("AEGRA_DYNAMODB_TABLE_PREFIX", DEFAULT_PREFIX))
    parser.add_argument("--list", action="store_true", help="list existing tables and exit")
    args = parser.parse_args(argv)

    # DynamoDB Local accepts any key pair; real AWS is out of scope for this script.
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "local")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "local")
    client = boto3.client("dynamodb", region_name=args.region, endpoint_url=args.endpoint_url)

    if args.list:
        for name in client.list_tables()["TableNames"]:
            print(name)
        return 0
    if not args.tenants:
        parser.error("give at least one tenant id, or --list")

    for tenant_id in args.tenants:
        if not is_valid_tenant_id(tenant_id):
            print(f"invalid tenant id: {tenant_id!r}", file=sys.stderr)
            return 2
        table_name = f"{args.prefix}{tenant_id}"
        created = create_tenant_table(client, table_name)
        print(f"{'created' if created else 'exists '} {table_name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
