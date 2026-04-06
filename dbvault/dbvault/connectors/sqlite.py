"""
SQLite connector.

SQLite stores an entire database in a single file, so backup is a
hot-copy using the built-in sqlite3 `backup` API (safe for concurrent
writes) and restore is a symmetric operation.
"""
from __future__ import annotations
import os, sqlite3
from typing import List, Optional
from .base import BaseConnector


class SQLiteConnector(BaseConnector):
    DEFAULT_PORT = None

    def __init__(self, params: dict):
        super().__init__(params)
        self.db_path: str = params["database"]

    def test_connection(self) -> str:
        if not os.path.isfile(self.db_path):
            raise RuntimeError(f"SQLite database file not found: '{self.db_path}'")
        try:
            con = sqlite3.connect(self.db_path)
            version = con.execute("SELECT sqlite_version();").fetchone()[0]
            con.close()
        except sqlite3.Error as exc:
            raise RuntimeError(f"SQLite error: {exc}") from exc
        return f"SQLite {version}"

    def backup_full(self, dest_path: str, tables: Optional[List[str]] = None,
                    exclude_tables: Optional[List[str]] = None) -> None:
        if not os.path.isfile(self.db_path):
            raise RuntimeError(f"SQLite database file not found: '{self.db_path}'")
        if tables or exclude_tables:
            _partial_backup(self.db_path, dest_path, tables, exclude_tables)
            return
        src = sqlite3.connect(self.db_path)
        dst = sqlite3.connect(dest_path)
        try:
            src.backup(dst)
        finally:
            src.close(); dst.close()

    def backup_incremental(self, dest_path: str, since: Optional[str] = None) -> None:
        self.backup_full(dest_path)

    def restore_full(self, src_path: str, tables: Optional[List[str]] = None,
                     drop_existing: bool = False, dry_run: bool = False) -> None:
        if dry_run:
            try:
                con = sqlite3.connect(src_path)
                con.execute("PRAGMA integrity_check;")
                con.close()
            except sqlite3.Error as exc:
                raise RuntimeError(f"Backup file validation failed: {exc}") from exc
            return
        if drop_existing and os.path.isfile(self.db_path):
            os.remove(self.db_path)
        if tables:
            _partial_restore(src_path, self.db_path, tables)
        else:
            src = sqlite3.connect(src_path)
            dst = sqlite3.connect(self.db_path)
            try:
                src.backup(dst)
            finally:
                src.close(); dst.close()


def _partial_backup(src_path, dest_path, tables, exclude_tables):
    src = sqlite3.connect(src_path)
    dst = sqlite3.connect(dest_path)
    all_tables = [r[0] for r in src.execute(
        "SELECT name FROM sqlite_master WHERE type='table';").fetchall()]
    if tables:
        target = [t for t in all_tables if t in tables]
    elif exclude_tables:
        target = [t for t in all_tables if t not in exclude_tables]
    else:
        target = all_tables
    try:
        for table in target:
            schema = src.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?;",
                (table,)).fetchone()
            if schema and schema[0]:
                dst.execute(schema[0])
            rows = src.execute(f"SELECT * FROM [{table}];").fetchall()
            if rows:
                ph = ", ".join("?" * len(rows[0]))
                dst.executemany(f"INSERT INTO [{table}] VALUES ({ph});", rows)
        dst.commit()
    finally:
        src.close(); dst.close()


def _partial_restore(src_path, dest_path, tables):
    src = sqlite3.connect(src_path)
    dst = sqlite3.connect(dest_path)
    try:
        for table in tables:
            schema = src.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?;",
                (table,)).fetchone()
            if not schema or not schema[0]:
                continue
            dst.execute(f"DROP TABLE IF EXISTS [{table}];")
            dst.execute(schema[0])
            rows = src.execute(f"SELECT * FROM [{table}];").fetchall()
            if rows:
                ph = ", ".join("?" * len(rows[0]))
                dst.executemany(f"INSERT INTO [{table}] VALUES ({ph});", rows)
        dst.commit()
    finally:
        src.close(); dst.close()
