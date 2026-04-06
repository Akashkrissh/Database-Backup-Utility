"""BackupManager — orchestrates backup operations end-to-end."""
from __future__ import annotations
import gzip, os, shutil, tempfile, time
from datetime import datetime, timezone
from typing import List, Optional

from ..connectors.base import BaseConnector
from ..storage.base import BaseStorage
from ..utils.logger import get_logger


class BackupManager:
    def __init__(self, connector: BaseConnector, storage: BaseStorage, notifier=None):
        self.connector = connector
        self.storage = storage
        self.notifier = notifier
        self.logger = get_logger()

    def run_backup(self, backup_type: str = "full", compress: bool = True,
                   tables: Optional[List[str]] = None,
                   exclude_tables: Optional[List[str]] = None,
                   tag: Optional[str] = None) -> dict:
        start = time.monotonic()
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        db = self.connector.database
        ext = _ext_for(self.connector.db_type)
        tag_sfx = f"_{tag}" if tag else ""
        base_name = f"{db}_{backup_type}_{timestamp}{tag_sfx}{ext}"

        self.logger.info("Backup started | db=%s type=%s compress=%s", db, backup_type, compress)

        with tempfile.TemporaryDirectory(prefix="dbvault_") as tmpdir:
            raw_path = os.path.join(tmpdir, base_name)
            try:
                self._dump(backup_type, raw_path, tables, exclude_tables)
            except Exception as exc:
                self.logger.error("Dump failed: %s", exc)
                if self.notifier:
                    self.notifier.send_failure(self.connector.db_type, db, str(exc))
                raise

            final_path, final_name = raw_path, base_name
            if compress:
                final_path, final_name = _gzip(raw_path, base_name)

            size_bytes = os.path.getsize(final_path)
            try:
                location = self.storage.upload(final_path, final_name)
            except Exception as exc:
                self.logger.error("Upload failed: %s", exc)
                if self.notifier:
                    self.notifier.send_failure(self.connector.db_type, db, str(exc))
                raise

        duration = time.monotonic() - start
        result = {
            "filename": final_name, "location": location,
            "size_bytes": size_bytes, "size_human": _human(size_bytes),
            "duration_s": round(duration, 2), "backup_type": backup_type,
            "timestamp_utc": timestamp, "database": db,
            "db_type": self.connector.db_type, "compressed": compress,
            "tables": tables or "all",
        }
        self.logger.info("Backup complete | file=%s size=%s duration=%.1fs",
                         final_name, result["size_human"], duration)
        if self.notifier:
            self.notifier.send_success(result)
        return result

    def _dump(self, backup_type, dest_path, tables, exclude_tables):
        c = self.connector
        if backup_type == "full":
            c.backup_full(dest_path, tables=tables, exclude_tables=exclude_tables)
        elif backup_type == "incremental":
            c.backup_incremental(dest_path)
        elif backup_type == "differential":
            c.backup_differential(dest_path)
        else:
            raise ValueError(f"Unknown backup_type '{backup_type}'.")


def _ext_for(db_type: str) -> str:
    return {"mysql": ".sql", "postgresql": ".sql", "postgres": ".sql",
            "mongodb": "", "sqlite": ".db"}.get(db_type, ".bak")


def _gzip(src: str, name: str):
    if os.path.isdir(src):
        shutil.make_archive(src, "tar", os.path.dirname(src), os.path.basename(src))
        src = src + ".tar"
        name = name + ".tar"
    gz = src + ".gz"
    with open(src, "rb") as fi, gzip.open(gz, "wb") as fo:
        shutil.copyfileobj(fi, fo)
    return gz, name + ".gz"


def _human(n: int) -> str:
    for u in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {u}"
        n /= 1024
    return f"{n:.1f} PB"
