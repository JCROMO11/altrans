import asyncio
import hashlib
import hmac
import os

import httpx

from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel

from agent.graph import run
from auth import create_token, get_current_conductor
from core.middleware import RateLimitMiddleware
from db import queries
from logging_config import setup_logging
from loguru import logger
from whatsapp import webhook as wa_webhook

setup_logging(os.getenv("LOG_LEVEL", "INFO"))

app = FastAPI(title="Altrans AI Agent")
app.add_middleware(RateLimitMiddleware)


# ── Modelos ───────────────────────────────────────────────────────────────────

class LoginRequest(BaseModel):
    cedula:     str
    manifiesto: int

class LoginResponse(BaseModel):
    token:  str
    nombre: str

class ChatRequest(BaseModel):
    mensaje:   str
    historial: list[dict] = []

class ChatResponse(BaseModel):
    respuesta: str


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.post("/login", response_model=LoginResponse)
async def login(req: LoginRequest):
    conductor = await queries.get_conductor_by_cedula(req.cedula.strip())
    if not conductor:
        raise HTTPException(status_code=401, detail="Cédula no encontrada")
    if not await queries.verificar_manifiesto_conductor(req.manifiesto, req.cedula.strip()):
        raise HTTPException(status_code=401, detail="El manifiesto no corresponde a esta cédula")

    token = create_token(conductor["nombre"], req.cedula.strip())
    return LoginResponse(token=token, nombre=conductor["nombre"])


@app.post("/chat", response_model=ChatResponse)
async def chat(req: ChatRequest, conductor: dict = Depends(get_current_conductor)):
    tipo = conductor.get("tipo_usuario") or "conductor"
    identificador = conductor.get("identificador") or conductor.get("cedula")
    kwargs = {"nombre": conductor.get("nombre"), "tipo_usuario": tipo}
    if tipo == "conductor":
        kwargs["conductor_cedula"] = identificador
    else:
        kwargs["placa"] = identificador
    respuesta, _tools_called = await run(req.mensaje, req.historial, **kwargs)
    return ChatResponse(respuesta=respuesta)


@app.get("/")
@app.get("/health")
async def health():
    url = os.getenv("SUPABASE_URL", "")
    key = os.getenv("SUPABASE_SERVICE_KEY", "")
    if not url or not key:
        return JSONResponse(status_code=503, content={"status": "degraded", "detail": "Supabase no configurado"})
    try:
        async with httpx.AsyncClient(timeout=3) as client:
            r = await client.head(
                f"{url}/rest/v1/manifiestos_flat",
                headers={
                    "apikey":        key,
                    "Authorization": f"Bearer {key}",
                    "Range":         "0-0",
                },
            )
            r.raise_for_status()
    except Exception as exc:
        return JSONResponse(status_code=503, content={"status": "degraded", "detail": str(exc)})
    return {"status": "ok"}


# ── WhatsApp Webhook ──────────────────────────────────────────────────────────

@app.get("/webhook")
def verify_webhook(request: Request):
    mode      = request.query_params.get("hub.mode")
    token     = request.query_params.get("hub.verify_token")
    challenge = request.query_params.get("hub.challenge")

    if mode == "subscribe" and token == os.getenv("WA_VERIFY_TOKEN"):
        return PlainTextResponse(challenge)
    raise HTTPException(status_code=403, detail="Verificación fallida")


def _validate_signature(raw_body: bytes, signature_header: str | None) -> bool:
    secret = os.getenv("WA_APP_SECRET", "")
    if not secret:
        logger.warning("hmac_skipped_no_secret")
        return True
    if not signature_header or not signature_header.startswith("sha256="):
        return False
    expected = "sha256=" + hmac.new(
        secret.encode("utf-8"), raw_body, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature_header)


@app.post("/webhook")
async def receive_webhook(request: Request):
    raw_body  = await request.body()
    signature = request.headers.get("x-hub-signature-256")

    if not _validate_signature(raw_body, signature):
        logger.warning("hmac_invalid", ip=request.client.host if request.client else None)
        raise HTTPException(status_code=403, detail="Firma inválida")

    try:
        body = await request.json()
    except Exception:
        return {"status": "ok"}

    try:
        messages = body["entry"][0]["changes"][0]["value"].get("messages", [])
    except (KeyError, IndexError):
        return {"status": "ok"}

    for msg in messages:
        if msg.get("type") != "text":
            continue
        asyncio.create_task(
            wa_webhook.handle_message(
                msg["from"], msg["id"], msg["text"]["body"],
            )
        )

    return {"status": "ok"}