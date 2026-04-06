"""
Configuration manager.

Supports YAML and JSON config files.  CLI flags always take precedence
over file-based defaults.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Optional


SAMPLE_CONFIG = """\
# ─────────────────────────────────────────────────────────────
# DBVault sample configuration file
# Generate a fresh copy:  dbvault init-config
# Use it:                 dbvault --config dbvault.yaml backup ...
# ─────────────────────────────────────────────────────────────

# ── database connection defaults ──────────────────────────────
database:
  db_type: postgresql      # mysql | postgresql | mongodb | sqlite
  host: localhost
  port: 5432
  username: myuser
  # password: ""          # prefer DBVAULT_PASSWORD env var
  database: mydb
  tls: false
  auth_db: admin           # MongoDB only

# ── backup defaults ───────────────────────────────────────────
backup:
  backup_type: full        # full | incremental | differential
  compress: true
  tables: []               # empty = all tables
  exclude_tables: []

# ── storage defaults ──────────────────────────────────────────
storage:
  backend: local           # local | s3 | gcs | azure
  output_dir: ./backups

  # AWS S3
  s3_bucket: ""
  s3_prefix: dbvault/
  s3_region: us-east-1

  # Google Cloud Storage
  gcs_bucket: ""
  gcs_prefix: dbvault/

  # Azure Blob Storage
  azure_container: ""
  azure_prefix: dbvault/

# ── logging ───────────────────────────────────────────────────
logging:
  level: INFO              # DEBUG | INFO | WARNING | ERROR
  log_file: ""             # e.g. /var/log/dbvault/dbvault.log

# ── notifications ─────────────────────────────────────────────
notifications:
  slack_webhook: ""        # or set DBVAULT_SLACK_WEBHOOK env var
  slack_channel: ""        # optional channel override
"""


class ConfigManager:
    """
    Loads an optional YAML / JSON config file and merges its values
    with runtime parameters.

    CLI arguments always win; config file values fill in blanks.
    """

    def __init__(self, path: Optional[str] = None):
        self.data: dict = {}
        if path:
            self._load(path)

    # ── public API ─────────────────────────────────────────────────────────

    def merge(self, **cli_kwargs) -> dict:
        """
        Return a merged parameter dict: cli_kwargs override config file values.

        ``None`` values in *cli_kwargs* are replaced by config file defaults
        when available.
        """
        merged = {}

        # Flatten relevant config sections
        for section in ("database", "backup", "storage", "notifications"):
            merged.update(self.data.get(section, {}))

        # CLI values win over config defaults (skip None to allow fallthrough)
        for k, v in cli_kwargs.items():
            if v is not None:
                merged[k] = v
            elif k not in merged:
                merged[k] = v  # keep None if no config default either

        # Apply DBMS-specific port defaults if port is still missing
        if not merged.get("port"):
            merged["port"] = _default_port(merged.get("db_type", ""))

        return merged

    def get(self, *keys: str, default: Any = None) -> Any:
        """Traverse nested config with dot-separated or positional keys."""
        node = self.data
        for key in keys:
            if not isinstance(node, dict):
                return default
            node = node.get(key, default)
        return node

    # ── private ────────────────────────────────────────────────────────────

    def _load(self, path: str) -> None:
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"Config file not found: {path}")

        text = p.read_text(encoding="utf-8")
        ext = p.suffix.lower()

        if ext in (".yaml", ".yml"):
            self.data = _parse_yaml(text, path)
        elif ext == ".json":
            self.data = json.loads(text)
        else:
            # Try JSON first, then YAML
            try:
                self.data = json.loads(text)
            except json.JSONDecodeError:
                self.data = _parse_yaml(text, path)


# ── helpers ────────────────────────────────────────────────────────────────────

def _parse_yaml(text: str, path: str) -> dict:
    try:
        import yaml  # type: ignore
        return yaml.safe_load(text) or {}
    except ImportError:
        # Minimal YAML fallback: only supports simple key: value pairs
        # (no nested dicts).  Fine for the flat sections we need.
        return _naive_yaml_parse(text)


def _naive_yaml_parse(text: str) -> dict:
    """
    Extremely basic YAML parser — handles simple ``key: value`` lines only.
    Used as a fallback when PyYAML is not installed.
    """
    result: dict = {}
    current_section: Optional[str] = None
    for raw_line in text.splitlines():
        line = raw_line.split("#")[0].rstrip()  # strip comments
        if not line:
            continue
        if line.endswith(":") and not line.startswith(" "):
            current_section = line[:-1].strip()
            result[current_section] = {}
            continue
        if ":" in line:
            indent = len(line) - len(line.lstrip())
            key, _, val = line.strip().partition(":")
            val = val.strip().strip('"').strip("'")
            if val.lower() == "true":
                val = True
            elif val.lower() == "false":
                val = False
            elif val.isdigit():
                val = int(val)
            elif val == "":
                val = None
            if indent > 0 and current_section:
                if isinstance(result.get(current_section), dict):
                    result[current_section][key] = val
            else:
                result[key] = val
    return result


def _default_port(db_type: str) -> Optional[int]:
    return {
        "mysql": 3306,
        "postgresql": 5432,
        "postgres": 5432,
        "mongodb": 27017,
        "mongo": 27017,
    }.get(db_type.lower() if db_type else "", None)
