"""
Scheduler de tareas periódicas con APScheduler.

Jobs configurados:
  - backup_semanal: domingos 6 AM Colombia (UTC-5 = 11:00 UTC)

Se inicializa desde main.py al arrancar la aplicación.
"""
import logging

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from backup_email import run_backup_and_email

logger = logging.getLogger(__name__)

_scheduler: BackgroundScheduler | None = None


def _job_backup() -> None:
    logger.info("scheduler_backup_triggered")
    try:
        counts = run_backup_and_email()
        logger.info("scheduler_backup_done", extra={"counts": counts})
    except Exception as e:
        logger.exception("scheduler_backup_error", extra={"error": str(e)})


def start() -> BackgroundScheduler:
    global _scheduler
    _scheduler = BackgroundScheduler(timezone="UTC")

    # Domingos a las 11:00 UTC = 6:00 AM Colombia (UTC-5)
    _scheduler.add_job(
        _job_backup,
        CronTrigger(day_of_week="sun", hour=11, minute=0),
        id="backup_semanal",
        name="Backup semanal → email",
        replace_existing=True,
    )

    _scheduler.start()
    logger.info("scheduler_started", extra={"jobs": [j.id for j in _scheduler.get_jobs()]})
    return _scheduler


def stop() -> None:
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("scheduler_stopped")
