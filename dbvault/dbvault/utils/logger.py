"""
Logging utilities for DBVault.

Sets up a structured logger with:
  * Console handler  — colourised, human-readable output
  * Optional file handler — JSON-lines for machine parsing / audit trail
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Optional

_LOGGER_NAME = "dbvault"
_logger: Optional[logging.Logger] = None


# ── public API ─────────────────────────────────────────────────────────────────

def setup_logger(
    level: str = "INFO",
    log_file: Optional[str] = None,
) -> logging.Logger:
    """
    Initialise and return the application logger.

    Parameters
    ----------
    level : str
        One of DEBUG / INFO / WARNING / ERROR.
    log_file : str, optional
        If provided, append JSON-lines records to this file.

    Returns
    -------
    logging.Logger
    """
    global _logger

    numeric_level = getattr(logging, level.upper(), logging.INFO)

    logger = logging.getLogger(_LOGGER_NAME)
    logger.setLevel(numeric_level)

    # Avoid adding duplicate handlers if called more than once
    if logger.handlers:
        logger.handlers.clear()

    # ── console handler ────────────────────────────────────────────────────
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(_ColourFormatter())
    logger.addHandler(console_handler)

    # ── file handler (JSON lines) ──────────────────────────────────────────
    if log_file:
        os.makedirs(os.path.dirname(os.path.abspath(log_file)), exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(numeric_level)
        file_handler.setFormatter(_JsonFormatter())
        logger.addHandler(file_handler)

    logger.propagate = False
    _logger = logger
    return logger


def get_logger() -> logging.Logger:
    """Return the application logger (initialises with defaults if needed)."""
    global _logger
    if _logger is None:
        _logger = setup_logger()
    return _logger


# ── formatters ─────────────────────────────────────────────────────────────────

class _ColourFormatter(logging.Formatter):
    """Human-readable colourised formatter for console output."""

    _COLOURS = {
        logging.DEBUG:    "\033[37m",    # white
        logging.INFO:     "\033[36m",    # cyan
        logging.WARNING:  "\033[33m",    # yellow
        logging.ERROR:    "\033[31m",    # red
        logging.CRITICAL: "\033[35m",    # magenta
    }
    _RESET = "\033[0m"
    _BOLD  = "\033[1m"

    def format(self, record: logging.LogRecord) -> str:
        colour = self._COLOURS.get(record.levelno, "")
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        level = f"{colour}{self._BOLD}{record.levelname:<8}{self._RESET}"
        msg = super().format(record)
        return f"{ts}  {level}  {msg}"


class _JsonFormatter(logging.Formatter):
    """JSON-lines formatter for file output (machine-parseable audit log)."""

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "funcName": record.funcName,
            "lineno": record.lineno,
        }
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry)
