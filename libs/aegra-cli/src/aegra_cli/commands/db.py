"""Database migration commands for Aegra.

Uses alembic's Python API directly (instead of subprocess) so that
script_location is resolved relative to the ini file, not the CWD.

These commands let operators run migrations out-of-band (init container,
Helm pre-upgrade Job, scripted deploy) when ``RUN_MIGRATIONS_ON_STARTUP``
is disabled in multi-pod environments.

Import-order note: ``aegra_api.*`` is imported inside each subcommand
callback, not at module top. The ``db`` group's callback runs
``load_env_file`` before any subcommand callback fires, so deferring the
import lets ``aegra_api.settings`` read the freshly populated
``os.environ`` instead of the empty pre-CLI env. Top-level imports here
would freeze ``settings`` to defaults before the user's .env loads,
breaking ``aegra db <cmd>`` against any non-default database.
"""

import asyncio
import sys
from collections.abc import Callable
from io import StringIO
from pathlib import Path

import click
import psycopg
import structlog
from rich.console import Console
from rich.panel import Panel

from aegra_cli.env import load_env_file

console = Console()
logger = structlog.get_logger(__name__)


def _run_alembic(
    operation: str,
    fn: Callable[[], None],
    *,
    success_msg: str,
    error_prefix: str,
) -> None:
    """Run an alembic command with standard output handling.

    Args:
        operation: Description of the operation for display
        fn: Callable that invokes the alembic command
        success_msg: Message to display on success
        error_prefix: Prefix for error message
    """
    # Imported here (not at module top) so this module can be imported
    # before .env loads — see import-order note in module docstring.
    from alembic.util import CommandError
    from sqlalchemy.exc import SQLAlchemyError

    try:
        fn()
        console.print(f"\n[bold green]{success_msg}[/bold green]")
    except KeyboardInterrupt:
        console.print("\n[yellow]Operation cancelled by user.[/yellow]")
        sys.exit(1)
    except (SQLAlchemyError, CommandError) as e:
        logger.debug("Alembic operation failed", operation=operation, error=str(e))
        console.print(f"\n[bold red]Error:[/bold red] {error_prefix} failed.")
        sys.exit(1)
    except Exception:
        logger.debug(
            "Unexpected error during alembic operation",
            operation=operation,
            exc_info=True,
        )
        console.print(f"\n[bold red]Error:[/bold red] {error_prefix} failed.")
        sys.exit(1)


@click.group()
@click.option(
    "--env-file",
    "-e",
    type=click.Path(exists=True, dir_okay=False),
    default=None,
    help="Path to .env file (default: .env in current directory).",
)
def db(env_file: str | None) -> None:
    """Database migration commands.

    Manage database migrations using Alembic.
    These commands are wrappers around common Alembic operations.
    """
    env_path = Path(env_file) if env_file else None
    load_env_file(env_path)


@db.command()
def upgrade() -> None:
    """Apply all pending migrations.

    Runs 'alembic upgrade head' to apply all pending migrations
    and bring the database schema up to date.

    Example:

        aegra db upgrade
    """
    console.print(
        Panel(
            "[bold green]Upgrading database to latest migration[/bold green]",
            title="[bold]Database Upgrade[/bold]",
            border_style="green",
        )
    )

    from aegra_api.core.migrations import get_alembic_config
    from alembic import command

    cfg = get_alembic_config()

    def run() -> None:
        command.upgrade(cfg, "head")

    _run_alembic(
        "upgrade",
        run,
        success_msg="Database upgraded successfully!",
        error_prefix="Alembic upgrade",
    )


@db.command()
@click.argument("revision", default="-1")
def downgrade(revision: str) -> None:
    """Downgrade database to a previous revision.

    Runs 'alembic downgrade' with the specified revision.
    Use '-1' to downgrade by one revision, or specify a revision hash.

    Arguments:

        REVISION: Target revision (default: -1 for one step back)

    Examples:

        aegra db downgrade          # Downgrade by one revision

        aegra db downgrade -2       # Downgrade by two revisions

        aegra db downgrade base     # Downgrade to initial state

        aegra db downgrade abc123   # Downgrade to specific revision
    """
    console.print(
        Panel(
            f"[bold yellow]Downgrading database to revision: {revision}[/bold yellow]",
            title="[bold]Database Downgrade[/bold]",
            border_style="yellow",
        )
    )

    if revision == "base":
        console.print("[yellow]Warning:[/yellow] Downgrading to 'base' will remove all migrations!")

    from aegra_api.core.migrations import get_alembic_config
    from alembic import command

    cfg = get_alembic_config()

    def run() -> None:
        command.downgrade(cfg, revision)

    _run_alembic(
        "downgrade",
        run,
        success_msg="Database downgraded successfully!",
        error_prefix="Alembic downgrade",
    )


@db.command()
def current() -> None:
    """Show current migration version.

    Displays the current revision that the database is at.
    Useful for checking which migrations have been applied.

    Example:

        aegra db current
    """
    console.print(
        Panel(
            "[bold cyan]Checking current database revision[/bold cyan]",
            title="[bold]Database Current[/bold]",
            border_style="cyan",
        )
    )

    from aegra_api.core.migrations import get_alembic_config
    from alembic import command

    cfg = get_alembic_config()
    # Capture alembic output to stdout
    output = StringIO()
    cfg.print_stdout = output.write

    def run() -> None:
        command.current(cfg)

    _run_alembic(
        "current",
        run,
        success_msg="Current revision check complete.",
        error_prefix="Alembic current",
    )
    if output.getvalue():
        console.print(output.getvalue().strip())


@db.command()
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    help="Show detailed migration information.",
)
def history(verbose: bool) -> None:
    """Show migration history.

    Displays the list of migrations in the Alembic history.
    Use --verbose for more detailed information.

    Examples:

        aegra db history            # Show migration history

        aegra db history --verbose  # Show detailed history
    """
    console.print(
        Panel(
            "[bold cyan]Displaying migration history[/bold cyan]",
            title="[bold]Database History[/bold]",
            border_style="cyan",
        )
    )

    from aegra_api.core.migrations import get_alembic_config
    from alembic import command

    cfg = get_alembic_config()
    output = StringIO()
    cfg.print_stdout = output.write

    def run() -> None:
        command.history(cfg, verbose=verbose)

    _run_alembic(
        "history",
        run,
        success_msg="Migration history check complete.",
        error_prefix="Alembic history",
    )
    if output.getvalue():
        console.print(output.getvalue().strip())


@db.command("enable-tenant-rls")
@click.option(
    "--app-role",
    default=None,
    help=(
        "Login role the server connects as, when running this as a different role "
        "(e.g. a superuser). It is granted the tenant role (default: the connecting role)."
    ),
)
@click.option(
    "--schema",
    default=None,
    help="Schema holding Aegra's tables (default: current_schema() from the search_path).",
)
@click.option(
    "--assign-existing-to",
    default=None,
    metavar="TENANT",
    help=(
        "Give every row that has no tenant yet to TENANT (upgrading a single-tenant install). "
        "Without it, such rows stop the command before anything changes."
    ),
)
def enable_tenant_rls(
    app_role: str | None, schema: str | None, assign_existing_to: str | None
) -> None:
    """Turn on PostgreSQL row-level security for tenant isolation.

    Run once after the server has started with AEGRA_TENANT_RLS_ENABLED=true
    (so migrations and LangGraph setup have created every table), and again
    after enabling semantic search (store_vectors). Safe to re-run.

    Example:

        AEGRA_TENANT_RLS_ENABLED=true aegra db enable-tenant-rls
    """
    console.print(
        Panel(
            "[bold green]Enabling tenant row-level security[/bold green]",
            title="[bold]Tenant RLS[/bold]",
            border_style="green",
        )
    )

    # Imported here so settings read the .env loaded by the db group — see module docstring.
    from aegra_api.core.tenancy.rls import UntaggedRowsError
    from aegra_api.core.tenancy.rls import enable_tenant_rls as apply_tenant_rls
    from aegra_api.settings import settings

    # The policies make tenant_id NOT NULL; a server with the flag off cannot fill it.
    if not settings.tenant.AEGRA_TENANT_RLS_ENABLED:
        console.print(
            "\n[bold red]Error:[/bold red] AEGRA_TENANT_RLS_ENABLED is not true. "
            "Enable it for the server first; with it off, writes fail once RLS is on."
        )
        sys.exit(1)

    tenant_role = settings.tenant.AEGRA_TENANT_DB_ROLE

    async def run() -> tuple[str, str]:
        async with await psycopg.AsyncConnection.connect(
            settings.db.database_url_sync, autocommit=True
        ) as conn:
            login_role = app_role
            if login_role is None:
                cur = await conn.execute("SELECT current_user")
                row = await cur.fetchone()
                login_role = str(row[0]) if row else None
            covered = await apply_tenant_rls(
                conn,
                tenant_role,
                app_login_role=login_role,
                schema=schema,
                assign_existing_to=assign_existing_to,
            )
            return login_role or "", covered

    try:
        login_role, covered_schema = asyncio.run(run())
    except UntaggedRowsError as e:
        console.print(f"\n[bold red]Error:[/bold red] nothing was changed: {e}.")
        console.print(
            "Existing data needs a tenant. For a single-tenant install, re-run with "
            "[cyan]--assign-existing-to <tenant id>[/cyan] (stop the server first)."
        )
        sys.exit(1)
    except ValueError as e:
        console.print(f"\n[bold red]Error:[/bold red] {e}")
        sys.exit(1)
    except psycopg.errors.UndefinedTable as e:
        console.print(
            f"\n[bold red]Error:[/bold red] {e.diag.message_primary}. "
            "Start the server once so migrations and LangGraph setup create every table, "
            "then re-run."
        )
        sys.exit(1)
    except psycopg.Error as e:
        logger.debug("enable-tenant-rls failed", error=str(e))
        reason = e.diag.message_primary or e
        console.print(f"\n[bold red]Error:[/bold red] enabling tenant RLS failed: {reason}")
        sys.exit(1)

    console.print(
        f"\n[bold green]Tenant RLS enabled.[/bold green] Tenant role [cyan]{tenant_role}[/cyan] "
        f"granted to [cyan]{login_role}[/cyan] (schema [cyan]{covered_schema}[/cyan])."
    )
