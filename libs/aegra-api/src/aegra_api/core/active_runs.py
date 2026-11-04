"""Global registry of in-flight asyncio tasks for graph executions.

Defined in a dependency-free module so that any layer (API routes, broker
managers, streaming service) can import it without circular dependencies.
"""

import asyncio

active_runs: dict[str, asyncio.Task[None]] = {}

# Explicit API cancellations are marked so worker shutdown remains recoverable.
explicit_run_cancellations: set[str] = set()

# Tenant of each locally executing run, so out-of-band writers (the cancel
# listener) can act under the run's own tenant instead of one named in a message.
active_run_tenants: dict[str, str] = {}
