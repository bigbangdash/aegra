"""INSECURE test-only auth that trusts tenant/user headers. Never deploy this.

Used by the tenant RLS E2E setup (docker-compose.tenant-rls.yml) so tests can
act as different tenants without a real identity provider. Any caller can claim
any tenant by setting a header, which is exactly what production auth must not
allow: derive org_id from a verified token instead.

Headers:
  x-tenant-id  -> org_id   (default "e2e-tenant")
  x-user-id    -> identity (default "e2e-user")
"""

from langgraph_sdk import Auth

auth = Auth()

DEFAULT_TENANT = "e2e-tenant"
DEFAULT_USER = "e2e-user"


@auth.authenticate
async def authenticate(headers: dict[bytes | str, bytes | str]) -> dict[str, str | bool]:
    def header(name: str, default: str) -> str:
        value = headers.get(name) or headers.get(name.encode())
        if isinstance(value, bytes):
            value = value.decode()
        return value or default

    user_id = header("x-user-id", DEFAULT_USER)
    return {
        "identity": user_id,
        "display_name": user_id,
        "org_id": header("x-tenant-id", DEFAULT_TENANT),
        "is_authenticated": True,
    }
