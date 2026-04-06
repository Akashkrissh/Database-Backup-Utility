"""MySQL / MariaDB connector using mysqldump and mysql CLI tools."""
from __future__ import annotations
import os, subprocess
from typing import List, Optional
from .base import BaseConnector


class MySQLConnector(BaseConnector):
    DEFAULT_PORT = 3306

    def _args(self) -> list:
        a = [f"--host={self.host}", f"--port={self.port}"]
        if self.username:
            a.append(f"--user={self.username}")
        if self.tls:
            a.append("--ssl-mode=REQUIRED")
        return a

    def _env(self) -> dict:
        e = os.environ.copy()
        if self.password:
            e["MYSQL_PWD"] = self.password
        return e

    def test_connection(self) -> str:
        self._require_tool("mysql")
        cmd = ["mysql"] + self._args() + [
            self.database, "--execute=SELECT VERSION();",
            "--batch", "--skip-column-names"]
        r = self._run(cmd, env=self._env())
        return "MySQL " + r.stdout.decode(errors="replace").strip()

    def backup_full(self, dest_path: str, tables: Optional[List[str]] = None,
                    exclude_tables: Optional[List[str]] = None) -> None:
        self._require_tool("mysqldump")
        cmd = ["mysqldump"] + self._args() + [
            "--single-transaction", "--routines", "--triggers",
            "--events", "--hex-blob"]
        if exclude_tables:
            for t in exclude_tables:
                cmd.append(f"--ignore-table={self.database}.{t}")
        cmd.append(self.database)
        if tables:
            cmd.extend(tables)
        with open(dest_path, "wb") as fh:
            proc = subprocess.Popen(cmd, stdout=fh, stderr=subprocess.PIPE, env=self._env())
            _, err = proc.communicate()
            if proc.returncode != 0:
                raise RuntimeError(f"mysqldump failed: {err.decode(errors='replace')}")

    def backup_incremental(self, dest_path: str, since: Optional[str] = None) -> None:
        if not self._tool_exists("mysqlbinlog"):
            self.backup_full(dest_path)
            return
        cmd = ["mysqlbinlog"]
        if since:
            cmd += ["--start-datetime", since]
        # Get current binlog file
        pos_cmd = ["mysql"] + self._args() + [
            "--execute=SHOW MASTER STATUS;", "--batch", "--skip-column-names"]
        try:
            r = self._run(pos_cmd, env=self._env())
            parts = r.stdout.decode(errors="replace").strip().split()
            if parts:
                cmd.append(parts[0])  # binlog filename
        except RuntimeError:
            self.backup_full(dest_path)
            return
        with open(dest_path, "wb") as fh:
            proc = subprocess.Popen(cmd, stdout=fh, stderr=subprocess.PIPE, env=self._env())
            _, err = proc.communicate()
            if proc.returncode != 0:
                self.backup_full(dest_path)  # graceful fallback

    def restore_full(self, src_path: str, tables: Optional[List[str]] = None,
                     drop_existing: bool = False, dry_run: bool = False) -> None:
        self._require_tool("mysql")
        env = self._env()
        if dry_run:
            with open(src_path, "r", errors="replace") as f:
                head = f.read(2048)
            if not any(k in head for k in ("CREATE", "INSERT", "mysqldump")):
                raise RuntimeError("File does not appear to be a valid MySQL dump.")
            return
        if drop_existing:
            sql = f"DROP DATABASE IF EXISTS `{self.database}`; CREATE DATABASE `{self.database}`;"
            self._run(["mysql"] + self._args() + ["--execute", sql], env=env)
        with open(src_path, "rb") as fh:
            proc = subprocess.Popen(
                ["mysql"] + self._args() + [self.database],
                stdin=fh, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
            _, err = proc.communicate()
            if proc.returncode != 0:
                raise RuntimeError(f"mysql restore failed: {err.decode(errors='replace')}")
