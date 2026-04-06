"""
Slack notification support via Incoming Webhooks.
No third-party SDK required — uses stdlib urllib.
"""

from __future__ import annotations

import json
import urllib.request
import urllib.error
from datetime import datetime, timezone
from typing import Optional

from ..utils.logger import get_logger


class SlackNotifier:
    """
    Send backup completion / failure notifications to Slack.

    Parameters
    ----------
    webhook_url : str
        Slack Incoming Webhook URL.
    channel : str, optional
        Override the default channel configured in the webhook.
    """

    def __init__(self, webhook_url: str, channel: Optional[str] = None):
        self.webhook_url = webhook_url
        self.channel = channel
        self.logger = get_logger()

    def send_success(self, result: dict) -> None:
        """Send a green success card with backup details."""
        ts = result.get("timestamp_utc", _now_str())
        payload = {
            "text": "✅ *DBVault backup succeeded*",
            "attachments": [
                {
                    "color": "good",
                    "fields": [
                        {"title": "Database",       "value": f"{result.get('db_type','?').upper()} · {result.get('database','?')}", "short": True},
                        {"title": "Type",           "value": result.get("backup_type", "full"),   "short": True},
                        {"title": "File",           "value": result.get("filename", "?"),          "short": False},
                        {"title": "Location",       "value": result.get("location", "?"),          "short": False},
                        {"title": "Size",           "value": result.get("size_human", "?"),        "short": True},
                        {"title": "Duration",       "value": f"{result.get('duration_s',0):.1f}s","short": True},
                        {"title": "Timestamp UTC",  "value": ts,                                   "short": True},
                    ],
                    "footer": "DBVault",
                    "ts": _parse_ts(ts),
                }
            ],
        }
        if self.channel:
            payload["channel"] = self.channel
        self._post(payload)

    def send_failure(self, db_type: str, database: str, error: str) -> None:
        """Send a red failure card."""
        payload = {
            "text": "❌ *DBVault backup FAILED*",
            "attachments": [
                {
                    "color": "danger",
                    "fields": [
                        {"title": "Database", "value": f"{db_type.upper()} · {database}", "short": True},
                        {"title": "Error",    "value": error,                               "short": False},
                        {"title": "Time UTC", "value": _now_str(),                          "short": True},
                    ],
                    "footer": "DBVault",
                }
            ],
        }
        if self.channel:
            payload["channel"] = self.channel
        self._post(payload)

    def _post(self, payload: dict) -> None:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            self.webhook_url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                if resp.status not in (200, 201):
                    self.logger.warning("Slack notification HTTP %s", resp.status)
        except urllib.error.URLError as exc:
            self.logger.warning("Slack notification failed: %s", exc)


def _now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _parse_ts(ts_str: str) -> int:
    try:
        dt = datetime.strptime(ts_str, "%Y%m%d_%H%M%S").replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except ValueError:
        return int(datetime.now(timezone.utc).timestamp())
