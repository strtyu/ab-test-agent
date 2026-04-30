from __future__ import annotations

import logging
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler

logger = logging.getLogger(__name__)
_scheduler = BackgroundScheduler(timezone="UTC")


def get_scheduler() -> BackgroundScheduler:
    return _scheduler


def start():
    if not _scheduler.running:
        _scheduler.start()
        logger.info("Scheduler started")


def stop():
    if _scheduler.running:
        _scheduler.shutdown(wait=False)


def schedule_test(test_id: str, run_immediately: bool = True) -> None:
    from ab_agent.pipeline.refresh_pipeline import run_refresh

    job_id = f"refresh_{test_id}"
    if _scheduler.get_job(job_id):
        return

    _scheduler.add_job(
        run_refresh,
        trigger="interval",
        hours=4,
        id=job_id,
        args=[test_id],
        replace_existing=True,
        next_run_time=datetime.utcnow() if run_immediately else None,
        misfire_grace_time=600,
    )
    logger.info("Scheduled refresh for test %s", test_id)


def unschedule_test(test_id: str) -> None:
    job_id = f"refresh_{test_id}"
    if _scheduler.get_job(job_id):
        _scheduler.remove_job(job_id)
        logger.info("Unscheduled refresh for test %s", test_id)


def restore_running_tests() -> None:
    """Called on startup to re-schedule all tests that are still running."""
    from ab_agent.db.repository import TestRepo

    tests = TestRepo().list_all()
    for t in tests:
        if t["status"] == "running":
            schedule_test(t["id"], run_immediately=False)
    logger.info("Restored %d running test jobs", sum(1 for t in tests if t["status"] == "running"))
