"""
DBVault - Database Backup CLI Utility
Main entrypoint for all CLI commands.
"""

import sys
import click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import box

from .config import Config
from .logger import setup_logger, get_logger
from .connectors import get_connector
from .backup.manager import BackupManager
from .restore.manager import RestoreManager
from .backup.scheduler import BackupScheduler
from .storage import get_storage_backend

console = Console()


def print_banner():
    console.print(Panel.fit(
        "[bold cyan]DBVault[/bold cyan] [dim]— Database Backup Utility[/dim]\n"
        "[dim]Supports MySQL · PostgreSQL · MongoDB · SQLite[/dim]",
        border_style="cyan",
        padding=(0, 2),
    ))


@click.group()
@click.version_option(version="1.0.0", prog_name="dbvault")
@click.option("--config", "-c", default="dbvault.yml", help="Path to config file.", show_default=True)
@click.option("--log-level", default="INFO", type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"]), help="Logging level.", show_default=True)
@click.option("--log-file", default=None, help="Path to log file (optional).")
@click.pass_context
def cli(ctx, config, log_level, log_file):
    """
    \b
    DBVault — Database Backup & Restore CLI Utility
    Supports: MySQL, PostgreSQL, MongoDB, SQLite
    Run `dbvault COMMAND --help` for help on individual commands.
    """
    ctx.ensure_object(dict)
    setup_logger(log_level, log_file)
    cfg = Config(config)
    ctx.obj["config"] = cfg
    ctx.obj["log_level"] = log_level


@cli.command("test-connection")
@click.option("--db-type", "-t", required=True,
              type=click.Choice(["mysql", "postgresql", "mongodb", "sqlite"], case_sensitive=False))
@click.option("--host", default="localhost", show_default=True)
@click.option("--port", default=None, type=int)
@click.option("--username", "-u", default=None)
@click.option("--password", "-p", default=None, hide_input=True)
@click.option("--database", "-d", required=True)
@click.option("--auth-db", default="admin", show_default=True)
@click.pass_context
def test_connection(ctx, db_type, host, port, username, password, database, auth_db):
    """Test a database connection with the given credentials."""
    print_banner()
    logger = get_logger()
    if password is None and db_type != "sqlite":
        password = click.prompt(f"Password for {username}@{host}", hide_input=True, default="")
    params = _build_params(db_type, host, port, username, password, database, auth_db)
    connector = get_connector(db_type, params)
    with console.status(f"[cyan]Connecting to {db_type.upper()} at {host}…"):
        ok, msg = connector.test_connection()
    if ok:
        console.print(f"[bold green]✔ Connection successful:[/bold green] {msg}")
        logger.info("Connection test passed: %s@%s/%s", db_type, host, database)
    else:
        console.print(f"[bold red]✘ Connection failed:[/bold red] {msg}")
        logger.error("Connection test failed: %s@%s/%s — %s", db_type, host, database, msg)
        sys.exit(1)


@cli.command("backup")
@click.option("--db-type", "-t", required=True,
              type=click.Choice(["mysql", "postgresql", "mongodb", "sqlite"], case_sensitive=False))
@click.option("--host", default="localhost", show_default=True)
@click.option("--port", default=None, type=int)
@click.option("--username", "-u", default=None)
@click.option("--password", "-p", default=None, hide_input=True)
@click.option("--database", "-d", required=True)
@click.option("--auth-db", default="admin", show_default=True)
@click.option("--backup-type", default="full",
              type=click.Choice(["full", "incremental", "differential"], case_sensitive=False),
              show_default=True)
@click.option("--output-dir", "-o", default="./backups", show_default=True)
@click.option("--compress/--no-compress", default=True, show_default=True)
@click.option("--storage", default="local",
              type=click.Choice(["local", "s3", "gcs", "azure"], case_sensitive=False))
@click.option("--tables", default=None, help="Comma-separated tables/collections to back up.")
@click.option("--tag", default=None, help="Optional label for this backup.")
@click.option("--notify/--no-notify", default=False)
@click.pass_context
def backup(ctx, db_type, host, port, username, password, database, auth_db,
           backup_type, output_dir, compress, storage, tables, tag, notify):
    """Perform a database backup.

    \b
    Examples:
      dbvault backup -t mysql -u root -d mydb --compress
      dbvault backup -t postgresql -u postgres -d appdb --backup-type incremental
      dbvault backup -t mongodb -d myapp --storage s3
      dbvault backup -t sqlite -d ./data.db -o ./backups
    """
    print_banner()
    logger = get_logger()
    cfg = ctx.obj["config"]
    if password is None and db_type != "sqlite":
        password = click.prompt(f"Password for {username}@{host}", hide_input=True, default="")
    params = _build_params(db_type, host, port, username, password, database, auth_db)
    table_list = [t.strip() for t in tables.split(",")] if tables else None
    connector = get_connector(db_type, params)
    storage_backend = get_storage_backend(storage, cfg, output_dir)
    manager = BackupManager(connector=connector, storage=storage_backend, config=cfg)
    try:
        result = manager.run(
            backup_type=backup_type, compress=compress,
            tables=table_list, tag=tag, notify_on_complete=notify,
        )
        _print_backup_result(result)
    except Exception as e:
        console.print(f"[bold red]Backup failed:[/bold red] {e}")
        logger.exception("Backup failed")
        sys.exit(1)


@cli.command("restore")
@click.option("--db-type", "-t", required=True,
              type=click.Choice(["mysql", "postgresql", "mongodb", "sqlite"], case_sensitive=False))
@click.option("--host", default="localhost", show_default=True)
@click.option("--port", default=None, type=int)
@click.option("--username", "-u", default=None)
@click.option("--password", "-p", default=None, hide_input=True)
@click.option("--database", "-d", required=True)
@click.option("--auth-db", default="admin", show_default=True)
@click.option("--backup-file", "-f", required=True)
@click.option("--tables", default=None)
@click.option("--drop-existing", is_flag=True, default=False)
@click.option("--yes", "-y", is_flag=True, default=False)
@click.pass_context
def restore(ctx, db_type, host, port, username, password, database, auth_db,
            backup_file, tables, drop_existing, yes):
    """Restore a database from a backup file.

    \b
    Examples:
      dbvault restore -t mysql -u root -d mydb -f ./backups/mydb_20240101.sql.gz
      dbvault restore -t postgresql -d appdb -f backup.dump --tables users,orders
      dbvault restore -t mongodb -d myapp -f ./backups/myapp.archive
    """
    print_banner()
    logger = get_logger()
    if not yes:
        click.confirm(f"Restore '{database}' from '{backup_file}'?", abort=True)
    if password is None and db_type != "sqlite":
        password = click.prompt(f"Password for {username}@{host}", hide_input=True, default="")
    params = _build_params(db_type, host, port, username, password, database, auth_db)
    table_list = [t.strip() for t in tables.split(",")] if tables else None
    connector = get_connector(db_type, params)
    manager = RestoreManager(connector=connector)
    try:
        result = manager.run(backup_file=backup_file, tables=table_list, drop_existing=drop_existing)
        _print_restore_result(result)
    except Exception as e:
        console.print(f"[bold red]Restore failed:[/bold red] {e}")
        logger.exception("Restore failed")
        sys.exit(1)


@cli.command("list")
@click.option("--output-dir", "-o", default="./backups", show_default=True)
@click.option("--storage", default="local",
              type=click.Choice(["local", "s3", "gcs", "azure"], case_sensitive=False))
@click.pass_context
def list_backups(ctx, output_dir, storage):
    """List all available backup files."""
    cfg = ctx.obj["config"]
    storage_backend = get_storage_backend(storage, cfg, output_dir)
    backups = storage_backend.list_backups()
    if not backups:
        console.print("[yellow]No backup files found.[/yellow]")
        return
    table = Table(title="Available Backups", box=box.ROUNDED, border_style="cyan")
    table.add_column("File", style="bold white")
    table.add_column("Size", justify="right", style="green")
    table.add_column("Created", style="dim")
    table.add_column("Type", style="yellow")
    for b in backups:
        table.add_row(b["name"], b["size"], b["created"], b.get("type", "—"))
    console.print(table)


@cli.command("schedule")
@click.option("--cron", required=True, help='Cron expression e.g. "0 2 * * *"')
@click.option("--db-type", "-t", required=True,
              type=click.Choice(["mysql", "postgresql", "mongodb", "sqlite"], case_sensitive=False))
@click.option("--host", default="localhost", show_default=True)
@click.option("--port", default=None, type=int)
@click.option("--username", "-u", default=None)
@click.option("--password", "-p", default=None, hide_input=True)
@click.option("--database", "-d", required=True)
@click.option("--auth-db", default="admin", show_default=True)
@click.option("--backup-type", default="full",
              type=click.Choice(["full", "incremental", "differential"]))
@click.option("--output-dir", "-o", default="./backups", show_default=True)
@click.option("--compress/--no-compress", default=True)
@click.option("--storage", default="local",
              type=click.Choice(["local", "s3", "gcs", "azure"]))
@click.option("--notify/--no-notify", default=False)
@click.pass_context
def schedule(ctx, cron, db_type, host, port, username, password, database, auth_db,
             backup_type, output_dir, compress, storage, notify):
    """Schedule automatic backups using a cron expression.

    \b
    Examples:
      dbvault schedule --cron "0 2 * * *" -t mysql -u root -d mydb
      dbvault schedule --cron "0 */6 * * *" -t postgresql -d appdb --storage s3
    """
    print_banner()
    cfg = ctx.obj["config"]
    if password is None and db_type != "sqlite":
        password = click.prompt(f"Password for {username}@{host}", hide_input=True, default="")
    params = _build_params(db_type, host, port, username, password, database, auth_db)
    connector = get_connector(db_type, params)
    storage_backend = get_storage_backend(storage, cfg, output_dir)
    manager = BackupManager(connector=connector, storage=storage_backend, config=cfg)
    scheduler = BackupScheduler(manager=manager)
    console.print(f"[green]Scheduling backups:[/green] cron=[bold]{cron}[/bold]  db=[bold]{database}[/bold]")
    scheduler.start(cron_expr=cron, backup_type=backup_type, compress=compress, notify_on_complete=notify)


@cli.command("init")
@click.option("--output", "-o", default="dbvault.yml", show_default=True)
def init(output):
    """Generate a sample dbvault.yml configuration file."""
    Config.generate_sample(output)
    console.print(f"[green]✔ Sample config written to:[/green] [bold]{output}[/bold]")
    console.print("Edit it with your credentials and storage settings.")


def _build_params(db_type, host, port, username, password, database, auth_db):
    defaults = {"mysql": 3306, "postgresql": 5432, "mongodb": 27017, "sqlite": None}
    return {
        "db_type": db_type, "host": host,
        "port": port or defaults.get(db_type),
        "username": username, "password": password,
        "database": database, "auth_db": auth_db,
    }


def _print_backup_result(result):
    color = "green" if result.get("status") == "success" else "red"
    icon = "✔" if result.get("status") == "success" else "✘"
    table = Table(box=box.SIMPLE, show_header=False, padding=(0, 1))
    table.add_column("Key", style="dim")
    table.add_column("Value", style="bold")
    table.add_row("Status", f"[{color}]{icon} {result.get('status','—').upper()}[/{color}]")
    table.add_row("File", str(result.get("file", "—")))
    table.add_row("Size", result.get("size_human", "—"))
    table.add_row("Duration", result.get("duration", "—"))
    table.add_row("Storage", result.get("storage", "—"))
    console.print(Panel(table, title="[bold]Backup Complete[/bold]", border_style=color))


def _print_restore_result(result):
    color = "green" if result.get("status") == "success" else "red"
    icon = "✔" if result.get("status") == "success" else "✘"
    console.print(f"[{color}]{icon} Restore {result.get('status','unknown').upper()}[/{color}] — {result.get('message','')}")


def main():
    cli(obj={})
