import asyncio
import os
from datetime import datetime, timedelta, timezone

from loguru import logger

from db import queries
from whatsapp.client import send_text


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_ts(value) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _env_min(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


async def _send_aviso(wa_from: str) -> None:
    await send_text(wa_from,
        "¿Sigues ahí? Si no respondes en unos minutos cierro tu sesión por seguridad.")


async def _send_cierre(wa_from: str) -> None:
    await send_text(wa_from,
        "Gracias por escribirnos 👋 Tu sesión se cerró por inactividad.\n"
        "Cuando me necesites, escríbeme y retomamos.")


async def _guardar_aviso(wa_from: str, avisado_at: datetime) -> None:
    session = await queries.get_session(wa_from)
    if not session:
        return
    session["inactividad_avisado_at"] = avisado_at.isoformat()
    await queries.upsert_session(session)


async def _check_inactividad(now: datetime, aviso_min: int, cierre_min: int) -> None:
    # Formato 'Z': evita el '+' del offset (+00:00) en la URL de PostgREST.
    def _iso_z(dt: datetime) -> str:
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    # Cierre primero: si ya pasó todo el plazo, se cierra aunque no se haya avisado.
    cutoff_cierre = now - timedelta(minutes=cierre_min)
    for s in await queries.get_sessions_inactivas(_iso_z(cutoff_cierre)):
        try:
            cur = await queries.get_session(s["wa_from"])
            if not cur or cur.get("estado") != "activa":
                continue
            if _now() - _parse_ts(cur["last_activity"]) < timedelta(minutes=cierre_min):
                continue
            await _send_cierre(cur["wa_from"])
            await queries.delete_session(cur["wa_from"])
            logger.info("session_closed_inactive", wa_from=cur["wa_from"])
        except Exception:
            logger.exception("inactive_close_failed", wa_from=s.get("wa_from"))

    # Aviso para las que pasaron el umbral y aún no lo recibieron.
    cutoff_aviso = now - timedelta(minutes=aviso_min)
    for s in await queries.get_sessions_inactivas(_iso_z(cutoff_aviso)):
        try:
            if s.get("inactividad_avisado_at"):
                continue
            await _send_aviso(s["wa_from"])
            await _guardar_aviso(s["wa_from"], _now())
            logger.info("session_warned_inactive", wa_from=s["wa_from"])
        except Exception:
            logger.exception("inactive_warn_failed", wa_from=s.get("wa_from"))


async def inactivity_worker(interval_s: int = 30) -> None:
    aviso_min = _env_min("WA_INACT_AVISO_MIN", 5)
    cierre_min = _env_min("WA_INACT_CIERRE_MIN", 10)
    if cierre_min <= aviso_min:
        cierre_min = aviso_min + 1
    logger.info("inactivity_worker_start",
                aviso_min=aviso_min, cierre_min=cierre_min)
    while True:
        try:
            await _check_inactividad(_now(), aviso_min, cierre_min)
        except Exception:
            logger.exception("inactivity_scan_failed")
        await asyncio.sleep(interval_s)
