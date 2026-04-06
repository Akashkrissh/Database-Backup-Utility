"""Abstract base class for all database connectors."""
from __future__ import annotations
import abc, subprocess, shutil
from typing import List, Optional


class BaseConnector(abc.ABC):
    DEFAULT_PORT: Optional[int] = None

    def __init__(self, params: dict):
        self.params = params
        self.db_type: str = params.get("db_type", "unknown")
        self.host: str = params.get("host", "localhost")
        self.port: int = params.get("port") or self.DEFAULT_PORT
        self.username: Optional[str] = params.get("username")
        self.password: Optional[str] = params.get("password")
        self.database: str = params["database"]
        self.tls: bool = params.get("tls", False)

    @abc.abstractmethod
    def test_connection(self) -> str:
        """Validate credentials. Returns server version string on success."""

    @abc.abstractmethod
    def backup_full(self, dest_path: str,
                    tables: Optional[List[str]] = None,
                    exclude_tables: Optional[List[str]] = None) -> None:
        """Full backup to dest_path."""

    def backup_incremental(self, dest_path: str, since: Optional[str] = None) -> None:
        """Incremental backup. Falls back to full by default."""
        self.backup_full(dest_path)

    def backup_differential(self, dest_path: str, base_backup: Optional[str] = None) -> None:
        """Differential backup. Falls back to full by default."""
        self.backup_full(dest_path)

    @abc.abstractmethod
    def restore_full(self, src_path: str,
                     tables: Optional[List[str]] = None,
                     drop_existing: bool = False,
                     dry_run: bool = False) -> None:
        """Restore the database from src_path."""

    def _run(self, cmd: list, env=None, input_data=None):
        result = subprocess.run(cmd, capture_output=True, env=env, input=input_data)
        if result.returncode != 0:
            stderr = result.stderr.decode(errors="replace")
            raise RuntimeError(
                f"Command failed (exit {result.returncode}): "
                f"{' '.join(str(c) for c in cmd)}\n{stderr}"
            )
        return result

    def _tool_exists(self, name: str) -> bool:
        return shutil.which(name) is not None

    def _require_tool(self, *names: str) -> str:
        for name in names:
            if shutil.which(name):
                return name
        raise RuntimeError(
            f"Required CLI tool(s) not found: {', '.join(names)}. "
            "Please install the relevant database client utilities."
        )
