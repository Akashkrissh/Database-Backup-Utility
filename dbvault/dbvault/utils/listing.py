"""Utility functions for listing local backup files."""

from __future__ import annotations

import os
from datetime import datetime
from typing import List, Optional


def list_local_backups(
    directory: str,
    db_filter: Optional[str] = None,
    limit: int = 20,
) -> List[dict]:
    """
    Return a sorted list of backup file metadata from *directory*.

    Parameters
    ----------
    directory : str
        Local backup directory to scan.
    db_filter : str, optional
        Only include files whose name starts with this database name.
    limit : int
        Maximum number of entries to return (most recent first).

    Returns
    -------
    list of dict
        Each entry has: filename, size_bytes, size_human, modified, path.
    """
    if not os.path.isdir(directory):
        return []

    extensions = {".sql", ".gz", ".db", ".bak", ".tar", ".dump"}
    entries = []

    for fname in os.listdir(directory):
        fpath = os.path.join(directory, fname)
        if not os.path.isfile(fpath):
            continue
        # Filter by extension
        _, ext = os.path.splitext(fname.lower())
        if ext not in extensions:
            # Also allow double extension like .sql.gz
            if not any(fname.lower().endswith(e) for e in (".sql.gz", ".tar.gz", ".db.gz")):
                continue
        # Filter by database name prefix
        if db_filter and not fname.startswith(db_filter):
            continue

        stat = os.stat(fpath)
        size = stat.st_size
        mtime = datetime.fromtimestamp(stat.st_mtime)
        entries.append({
            "filename": fname,
            "path": fpath,
            "size_bytes": size,
            "size_human": _human_size(size),
            "modified": mtime.strftime("%Y-%m-%d %H:%M:%S"),
        })

    # Most recent first
    entries.sort(key=lambda e: e["modified"], reverse=True)
    return entries[:limit]


def _human_size(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"
