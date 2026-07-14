"""
Servicio de notificaciones Altrans — segundo servicio Railway.

Endpoints:
  GET  /health            → liveness check
  POST /admin/backup      → dispara backup manual (requiere X-Admin-Token)
  POST /admin/notify/wa   → envía mensaje WA manual a lista de números
  POST /admin/auto-notify → dispara ronda de notificaciones automáticas
"""
import logging
import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

import httpx

from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from auto_notify import run_auto_notify
from backup_email import run_backup_and_email
from whatsapp_notify import send_whatsapp_bulk
from logging_config import setup_logging
import scheduler as sched

setup_logging(os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)


# ── Lifecycle ─────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    sched.start()
    yield
    sched.stop()


app = FastAPI(title="Altrans Notifications", lifespan=lifespan)


# ── Modelos ───────────────────────────────────────────────────────────────────

class WaNotifyRequest(BaseModel):
    phones:  list[str]
    message: str


# ── Auth helper ───────────────────────────────────────────────────────────────

def _check_admin_token(request: Request) -> None:
    expected = os.environ.get("ADMIN_TOKEN", "")
    if not expected:
        raise HTTPException(status_code=503, detail="ADMIN_TOKEN no configurado")
    if request.headers.get("x-admin-token") != expected:
        raise HTTPException(status_code=403, detail="Token inválido")


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/")
@app.get("/health")
def health():
    url = os.getenv("SUPABASE_URL", "")
    key = os.getenv("SUPABASE_SERVICE_KEY", "")
    if not url or not key:
        return JSONResponse(status_code=503, content={"status": "degraded", "detail": "Supabase no configurado"})
    try:
        r = httpx.head(
            f"{url}/rest/v1/manifiestos_flat",
            headers={
                "apikey":        key,
                "Authorization": f"Bearer {key}",
                "Range":         "0-0",
            },
            timeout=3,
        )
        r.raise_for_status()
    except Exception as exc:
        return JSONResponse(status_code=503, content={"status": "degraded", "detail": str(exc)})
    jobs = []
    if sched._scheduler:
        jobs = [{"id": j.id, "next_run": str(j.next_run_time)} for j in sched._scheduler.get_jobs()]
    return {"status": "ok", "scheduled_jobs": jobs}


@app.post("/admin/backup")
def admin_backup(request: Request, background_tasks: BackgroundTasks):
    """Dispara backup manual de todas las tablas y envía ZIP por email."""
    _check_admin_token(request)
    background_tasks.add_task(run_backup_and_email)
    return {"status": "scheduled", "detail": "Backup en curso, el email llegará en 1-2 minutos."}


@app.post("/admin/notify/wa")
def admin_notify_wa(request: Request, body: WaNotifyRequest, background_tasks: BackgroundTasks):
    """Envía un mensaje de WhatsApp a la lista de números indicada."""
    _check_admin_token(request)
    if not body.phones:
        raise HTTPException(status_code=400, detail="Lista de phones vacía")
    if not body.message.strip():
        raise HTTPException(status_code=400, detail="Mensaje vacío")
    background_tasks.add_task(send_whatsapp_bulk, body.phones, body.message)
    return {"status": "scheduled", "recipients": len(body.phones)}


@app.post("/admin/auto-notify")
def admin_auto_notify(request: Request, background_tasks: BackgroundTasks):
    """Dispara la ronda de notificaciones automáticas."""
    _check_admin_token(request)
    background_tasks.add_task(run_auto_notify)
    return {"status": "scheduled", "detail": "Notificaciones automáticas en curso."}
