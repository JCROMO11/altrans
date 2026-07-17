"""
Scheduler de tareas periódicas con APScheduler.

Jobs configurados:
  - backup: miércoles y domingos 6 AM Colombia (backup a email)
  - auto_notify: diario 6 AM Colombia (envía WhatsApp a conductores)
  - auto_notify_novedades: diario 12 PM Colombia (segunda tanda)

Se inicializa desde main.py al arrancar la aplicación.
"""
import logging

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from backup_email import run_backup_and_email
from auto_notify import run_auto_notify

logger = logging.getLogger(__name__)

_scheduler: BackgroundScheduler | None = None


def _job_backup() -> None:
    logger.info("scheduler_backup_triggered")
    try:
        result = run_backup_and_email()
        logger.info("scheduler_backup_done", extra={"consistent": result.get("consistent")})
    except Exception as e:
        logger.exception("scheduler_backup_error", extra={"error": str(e)})


def _job_auto_notify() -> None:
    logger.info("scheduler_auto_notify_triggered")
    try:
        result = run_auto_notify()
        logger.info("scheduler_auto_notify_done", extra=result)
    except Exception as e:
        logger.exception("scheduler_auto_notify_error", extra={"error": str(e)})


def start() -> BackgroundScheduler:
    global _scheduler
    _scheduler = BackgroundScheduler(timezone="UTC")

    # Backup: miércoles y domingos 11:00 UTC = 6:00 AM Colombia
    _scheduler.add_job(
        _job_backup,
        CronTrigger(day_of_week="wed,sun", hour=11, minute=0),
        id="backup_2x_semanal",
        name="Backup → Email (mié/dom 6AM)",
        replace_existing=True,
    )

    # Auto-notify diario: 11:00 UTC = 6:00 AM Colombia
    _scheduler.add_job(
        _job_auto_notify,
        CronTrigger(hour=11, minute=0),
        id="auto_notify_diario",
        name="Notificaciones automáticas WhatsApp (6AM)",
        replace_existing=True,
    )

    # Segunda tanda: 17:00 UTC = 12:00 PM Colombia
    _scheduler.add_job(
        _job_auto_notify,
        CronTrigger(hour=17, minute=0),
        id="auto_notify_tarde",
        name="Notificaciones automáticas WhatsApp (12PM)",
        replace_existing=True,
    )

    _scheduler.start()
    logger.info("scheduler_started", extra={"jobs": [j.id for j in _scheduler.get_jobs()]})
    return _scheduler


def stop() -> None:
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("scheduler_stopped")
