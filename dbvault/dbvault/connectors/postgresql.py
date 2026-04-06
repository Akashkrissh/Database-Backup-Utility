"""PostgreSQL connector using pg_dump / psql CLI tools."""
from __future__ import annotations
import os, subprocess
from typing import List, Optional
from .base import BaseConnector


class PostgreSQLConnector(BaseConnector):
    DEFAULT_PORT = 5432

    def _env(self) -> dict:
        e = os.environ.copy()
        if self.password:
            e["PGPASSWORD"] = self.password
        return e

    def _args(self) -> list:
        a = ["--host", self.host, "--port", str(self.port)]
        if self.username:
            a += ["--username", self.username]
        return a

    def test_connection(self) -> str:
        self._require_tool("psql")
        cmd = ["psql"] + self._args() + [
            "--dbname", self.database,
            "--tuples-only", "--command", "SELECT version();"]
        r = self._run(cmd, env=self._env())
        return r.stdout.decode(errors="replace").strip().splitlines()[0]

    def backup_full(self, dest_path: str, tables: Optional[List[str]] = None,
                    exclude_tables: Optional[List[str]] = None) -> None:
        self._require_tool("pg_dump")
        cmd = ["pg_dump"] + self._args() + [
            "--dbname", self.database,
            "--file", dest_path, "--format", "plain", "--blobs"]
        if tables:
            for t in tables:
                cmd += ["--table", t]
        if exclude_tables:
            for t in exclude_tables:
                cmd += ["--exclude-table", t]
        self._run(cmd, env=self._env())

    def backup_incremental(self, dest_path: str, since: Optional[str] = None) -> None:
        if self._tool_exists("pg_basebackup"):
            cmd = ["pg_basebackup"] + self._args() + [
                "--pgdata", dest_path, "--format", "tar",
                "--gzip", "--checkpoint", "fast", "--wal-method", "stream"]
            try:
                self._run(cmd, env=self._env())
                return
            except RuntimeError:
                pass
        self.backup_full(dest_path)

    def restore_full(self, src_path: str, tables: Optional[List[str]] = None,
                     drop_existing: bool = False, dry_run: bool = False) -> None:
        self._require_tool("psql")
        env = self._env()
        if dry_run:
            with open(src_path, "r", errors="replace") as f:
                head = f.read(2048)
            if not any(k in head for k in ("PostgreSQL", "pg_dump", "CREATE", "COPY")):
                raise RuntimeError("File does not appear to be a valid PostgreSQL dump.")
            return
        if drop_existing:
            for sql, db in [
                (f"DROP DATABASE IF EXISTS {self.database};", "postgres"),
                (f"CREATE DATABASE {self.database};", "postgres"),
            ]:
                self._run(["psql"] + self._args() + ["--dbname", db, "--command", sql], env=env)
        self._run(["psql"] + self._args() + [
            "--dbname", self.database, "--quiet", "--file", src_path], env=env)
