"""MongoDB connector using mongodump / mongorestore CLI tools."""
from __future__ import annotations
import os, subprocess
from typing import List, Optional
from .base import BaseConnector


class MongoDBConnector(BaseConnector):
    DEFAULT_PORT = 27017

    def _uri(self) -> str:
        auth = ""
        if self.username and self.password:
            auth = f"{self.username}:{self.password}@"
        elif self.username:
            auth = f"{self.username}@"
        auth_db = self.params.get("auth_db", "admin")
        tls = "?tls=true" if self.tls else ""
        return f"mongodb://{auth}{self.host}:{self.port}/{self.database}?authSource={auth_db}{tls}"

    def _args(self) -> list:
        a = ["--host", f"{self.host}:{self.port}", "--db", self.database]
        if self.username:
            a += ["--username", self.username]
        if self.password:
            a += ["--password", self.password]
        a += ["--authenticationDatabase", self.params.get("auth_db", "admin")]
        if self.tls:
            a += ["--tls"]
        return a

    def test_connection(self) -> str:
        tool = self._require_tool("mongosh", "mongo")
        cmd = [tool, self._uri(),
               "--eval", "JSON.stringify(db.serverStatus().version)", "--quiet"]
        r = self._run(cmd)
        return "MongoDB " + r.stdout.decode(errors="replace").strip().strip('"')

    def backup_full(self, dest_path: str, tables: Optional[List[str]] = None,
                    exclude_tables: Optional[List[str]] = None) -> None:
        self._require_tool("mongodump")
        if tables:
            for col in tables:
                cmd = ["mongodump"] + self._args() + [
                    "--collection", col, "--out", dest_path, "--gzip"]
                self._run(cmd)
            return
        cmd = ["mongodump"] + self._args() + ["--out", dest_path, "--gzip"]
        if exclude_tables:
            for col in exclude_tables:
                cmd += ["--excludeCollection", col]
        self._run(cmd)

    def backup_incremental(self, dest_path: str, since: Optional[str] = None) -> None:
        self._require_tool("mongodump")
        cmd = ["mongodump"] + self._args() + [
            "--out", dest_path, "--gzip", "--oplog"]
        try:
            self._run(cmd)
        except RuntimeError:
            self.backup_full(dest_path)

    def restore_full(self, src_path: str, tables: Optional[List[str]] = None,
                     drop_existing: bool = False, dry_run: bool = False) -> None:
        self._require_tool("mongorestore")
        if dry_run:
            if not os.path.isdir(src_path):
                raise RuntimeError(f"Expected mongodump directory, got: {src_path}")
            return
        cmd = ["mongorestore"] + self._args() + ["--gzip"]
        if drop_existing:
            cmd.append("--drop")
        if tables:
            for col in tables:
                col_cmd = cmd + [
                    "--collection", col,
                    os.path.join(src_path, self.database, f"{col}.bson.gz")]
                self._run(col_cmd)
            return
        cmd.append(src_path)
        self._run(cmd)
