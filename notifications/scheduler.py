"""
Scheduler de tareas periódicas con APScheduler.

Jobs configurados:
  - backup: lunes a viernes 10 PM Colombia (backup a email + bucket Supabase)
  - auto_notify: lunes y miércoles (configurable) 6 AM Colombia, con las 5
    plantillas espaciadas 5 minutos (pago_realizado primero, luego saldos). El
    miércoles sirve de reintento para fallos y casos nuevos. Si algo falla o
    queda pendiente, se reintenta manualmente con POST /admin/auto-notify-cycle.

Cadencia configurable con variables de entorno:
  AUTO_NOTIFY_DAYS      días separados por coma (default: mon,wed)
  AUTO_NOTIFY_HOUR_UTC  hora UTC de inicio (default: 11 = 6 AM Colombia)

Orden de plantillas y espaciado (Colombia = UTC-5):
  6:00  pago_realizado
  6:05  saldo_falta_factura
  6:10  saldo_falta_documentacion
  6:15  saldo_novedad_pendiente
  6:20  saldo_plazo_vigente

Cada job llama run_auto_notify(templates=[una]) para procesar solo ese lote
(los mensajes de la misma plantilla se envían en paralelo dentro de auto_notify).
"""
import logging
import os

import httpx

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from backup_email import run_backup_and_email
from auto_notify import run_auto_notify, TEMPLATE_ORDER
from health_report import run_morning_check

logger = logging.getLogger(__name__)

_scheduler: BackgroundScheduler | None = None

# Cadencia de auto-notify (configurable por entorno).
_VALID_DAYS = {"mon", "tue", "wed", "thu", "fri", "sat", "sun"}


def _parse_days(raw: str) -> str:
    """Normaliza AUTO_NOTIFY_DAYS a formato CronTrigger (ej. 'mon,wed')."""
    days = [d.strip()[:3] for d in raw.split(",") if d.strip()]
    valid = [d for d in days if d in _VALID_DAYS]
    if not valid:
        logger.warning("auto_notify_days_invalid", extra={"raw": raw, "using": "mon,wed"})
        return "mon,wed"
    return ",".join(dict.fromkeys(valid))


_AUTO_NOTIFY_DAYS = _parse_days(os.environ.get("AUTO_NOTIFY_DAYS", "mon,wed"))
_AUTO_NOTIFY_HOUR_UTC = int(os.environ.get("AUTO_NOTIFY_HOUR_UTC", "11"))
_INTERVAL_MINUTES = 5


def _ping_heartbeat(url: str | None) -> None:
    """Ping opcional a Healthchecks.io al terminar un job (si HC_*_URL definido)."""
    if not url:
        return
    try:
        httpx.get(url, timeout=10)
        logger.info("heartbeat_ping", extra={"url": url})
    except Exception as e:
        logger.warning("heartbeat_ping_failed", extra={"url": url, "error": str(e)})


def _job_backup() -> None:
    logger.info("scheduler_backup_triggered")
    try:
        result = run_backup_and_email()
        logger.info("scheduler_backup_done", extra={"consistent": result.get("consistent")})
    except Exception as e:
        logger.exception("scheduler_backup_error", extra={"error": str(e)})
    finally:
        _ping_heartbeat(os.environ.get("HC_BACKUP_URL"))


def _make_auto_notify_job(template: str):
    def _job() -> None:
        logger.info("scheduler_auto_notify_triggered", extra={"template": template})
        try:
            result = run_auto_notify(templates=[template])
            logger.info("scheduler_auto_notify_done", extra={"template": template, **result})
        except Exception as e:
            logger.exception("scheduler_auto_notify_error",
                             extra={"template": template, "error": str(e)})
        finally:
            _ping_heartbeat(os.environ.get("HC_NOTIFY_URL"))
    return _job


def _job_morning_report() -> None:
    logger.info("scheduler_morning_report_triggered")
    try:
        result = run_morning_check()
        logger.info("scheduler_morning_report_done", extra=result)
    except Exception as e:
        logger.exception("scheduler_morning_report_error", extra={"error": str(e)})
    finally:
        _ping_heartbeat(os.environ.get("HC_MORNING_URL"))


def start() -> BackgroundScheduler:
    global _scheduler
    _scheduler = BackgroundScheduler(timezone="UTC")

    # Backup: lunes a viernes 22:00 Colombia = 03:00 UTC del día siguiente
    _scheduler.add_job(
        _job_backup,
        CronTrigger(day_of_week="mon-fri", hour=22, minute=0, timezone="America/Bogota"),
        id="backup_entre_semana",
        name="Backup → Email+Bucket (lun-vie 10PM)",
        replace_existing=True,
    )

    # Auto-notify: 5 plantillas por tanda, 5 min entre cada una.
    for i, template in enumerate(TEMPLATE_ORDER):
        minute = i * _INTERVAL_MINUTES
        _scheduler.add_job(
            _make_auto_notify_job(template),
            CronTrigger(day_of_week=_AUTO_NOTIFY_DAYS, hour=_AUTO_NOTIFY_HOUR_UTC, minute=minute, timezone="UTC"),
            id=f"auto_notify_{template}",
            name=f"Notificación {template} ({_AUTO_NOTIFY_DAYS} {_AUTO_NOTIFY_HOUR_UTC}:00Z +{minute}m)",
            replace_existing=True,
        )

    # Chequeo matutino: 7:00 AM Colombia (12:00 UTC). Reporta el estado de
    # todos los módulos por WhatsApp/email para "todo listo para trabajar".
    _scheduler.add_job(
        _job_morning_report,
        CronTrigger(hour=7, minute=0, timezone="America/Bogota"),
        id="morning_report",
        name="Chequeo matutino (7AM)",
        replace_existing=True,
    )

    _scheduler.start()
    logger.info("scheduler_started", extra={"jobs": [j.id for j in _scheduler.get_jobs()]})
    return _scheduler


def stop() -> None:
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("scheduler_stopped")
