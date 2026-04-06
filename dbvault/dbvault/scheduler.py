"""
Backup scheduler using APScheduler.

Supports cron expressions and fixed intervals.
APScheduler is installed via: pip install apscheduler
"""

from __future__ import annotations

from typing import Optional

from .utils.logger import get_logger


class BackupScheduler:
    """
    Wraps APScheduler to run recurring backups.

    Parameters
    ----------
    manager : BackupManager
        The backup manager to invoke on each tick.
    backup_type : str
        Backup type to pass to manager.run_backup().
    compress : bool
        Whether to compress each backup.
    """

    def __init__(self, manager, backup_type: str = "full", compress: bool = True):
        self.manager = manager
        self.backup_type = backup_type
        self.compress = compress
        self.logger = get_logger()
        self._scheduler = None

    # ── public API ─────────────────────────────────────────────────────────

    def add_cron_job(self, cron_expr: str) -> None:
        """
        Schedule a backup with a cron expression.

        Parameters
        ----------
        cron_expr : str
            Standard 5-field cron: ``"minute hour day month day_of_week"``
            e.g. ``"0 2 * * *"`` for daily at 02:00 UTC.
        """
        fields = cron_expr.strip().split()
        if len(fields) != 5:
            raise ValueError(
                f"Invalid cron expression '{cron_expr}'. "
                "Expected 5 fields: minute hour day month day_of_week."
            )
        minute, hour, day, month, day_of_week = fields
        scheduler = self._get_scheduler()
        scheduler.add_job(
            self._run_backup,
            trigger="cron",
            minute=minute,
            hour=hour,
            day=day,
            month=month,
            day_of_week=day_of_week,
            id="dbvault_cron",
            replace_existing=True,
            misfire_grace_time=300,
        )
        self.logger.info("Cron job scheduled: %s", cron_expr)

    def add_interval_job(self, minutes: int) -> None:
        """
        Schedule a backup every *minutes* minutes.

        Parameters
        ----------
        minutes : int
            Interval in minutes between backups.
        """
        if minutes < 1:
            raise ValueError("Interval must be at least 1 minute.")
        scheduler = self._get_scheduler()
        scheduler.add_job(
            self._run_backup,
            trigger="interval",
            minutes=minutes,
            id="dbvault_interval",
            replace_existing=True,
            misfire_grace_time=300,
        )
        self.logger.info("Interval job scheduled: every %d minute(s)", minutes)

    def start(self) -> None:
        """Start the scheduler — blocks until interrupted."""
        scheduler = self._get_scheduler()
        scheduler.start()

    def stop(self) -> None:
        """Gracefully shut down the scheduler."""
        if self._scheduler and self._scheduler.running:
            self._scheduler.shutdown(wait=False)
            self.logger.info("Scheduler stopped.")

    # ── private ────────────────────────────────────────────────────────────

    def _get_scheduler(self):
        if self._scheduler is None:
            try:
                from apscheduler.schedulers.blocking import BlockingScheduler
                from apscheduler.executors.pool import ThreadPoolExecutor
            except ImportError:
                raise RuntimeError(
                    "APScheduler is required for scheduling. "
                    "Install it with: pip install apscheduler"
                )
            executors = {"default": ThreadPoolExecutor(max_workers=1)}
            self._scheduler = BlockingScheduler(
                executors=executors,
                timezone="UTC",
            )
        return self._scheduler

    def _run_backup(self) -> None:
        try:
            result = self.manager.run_backup(
                backup_type=self.backup_type,
                compress=self.compress,
            )
            self.logger.info(
                "Scheduled backup complete: %s", result.get("filename")
            )
        except Exception as exc:
            self.logger.error("Scheduled backup failed: %s", exc, exc_info=True)
