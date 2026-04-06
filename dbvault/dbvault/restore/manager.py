"""RestoreManager — orchestrates database restore operations."""
from __future__ import annotations
import gzip, os, shutil, tarfile, tempfile, time
from typing import List, Optional

from ..connectors.base import BaseConnector
from ..utils.logger import get_logger


class RestoreManager:
    def __init__(self, connector: BaseConnector):
        self.connector = connector
        self.logger = get_logger()

    def run_restore(self, backup_file: str, tables: Optional[List[str]] = None,
                    drop_existing: bool = False, dry_run: bool = False) -> dict:
        start = time.monotonic()
        self.logger.info("Restore started | db=%s file=%s dry_run=%s",
                         self.connector.database, backup_file, dry_run)
        with tempfile.TemporaryDirectory(prefix="dbvault_restore_") as tmpdir:
            work_path = _prepare(backup_file, tmpdir)
            try:
                self.connector.restore_full(src_path=work_path, tables=tables,
                                            drop_existing=drop_existing, dry_run=dry_run)
            except Exception as exc:
                self.logger.error("Restore failed: %s", exc)
                raise

        duration = time.monotonic() - start
        result = {"status": "dry-run" if dry_run else "ok",
                  "duration_s": round(duration, 2),
                  "tables_restored": tables or "all",
                  "database": self.connector.database,
                  "backup_file": backup_file}
        self.logger.info("Restore complete | duration=%.1fs", duration)
        return result


def _prepare(src: str, tmpdir: str) -> str:
    name = os.path.basename(src).lower()
    if name.endswith(".tar.gz") or name.endswith(".tgz"):
        with tarfile.open(src, "r:gz") as tf:
            tf.extractall(tmpdir)
        entries = os.listdir(tmpdir)
        return os.path.join(tmpdir, entries[0]) if entries else tmpdir
    if name.endswith(".tar"):
        with tarfile.open(src, "r") as tf:
            tf.extractall(tmpdir)
        entries = os.listdir(tmpdir)
        return os.path.join(tmpdir, entries[0]) if entries else tmpdir
    if name.endswith(".gz"):
        out = os.path.join(tmpdir, os.path.splitext(os.path.basename(src))[0])
        with gzip.open(src, "rb") as gz, open(out, "wb") as f:
            shutil.copyfileobj(gz, f)
        return out
    return src
