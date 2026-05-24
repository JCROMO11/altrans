import hashlib
import hmac
import logging
import os

from fastapi import FastAPI, Depends, HTTPException, Request, BackgroundTasks
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel

from agent.graph import run
from auth import create_token, get_current_conductor
from db import queries
from logging_config import setup_logging
from whatsapp import webhook as wa_webhook

setup_logging(os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

app = FastAPI(title="Altrans AI Agent")


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
def login(req: LoginRequest):
    conductor = queries.get_conductor_by_cedula(req.cedula.strip())
    if not conductor:
        raise HTTPException(status_code=401, detail="Cédula no encontrada")
    if not queries.verificar_manifiesto_conductor(req.manifiesto, req.cedula.strip()):
        raise HTTPException(status_code=401, detail="El manifiesto no corresponde a esta cédula")

    token = create_token(conductor["nombre"], req.cedula.strip())
    return LoginResponse(token=token, nombre=conductor["nombre"])


@app.post("/chat", response_model=ChatResponse)
def chat(req: ChatRequest, conductor: dict = Depends(get_current_conductor)):
    tipo = conductor.get("tipo_usuario") or "conductor"
    identificador = conductor.get("identificador") or conductor.get("cedula")
    kwargs = {"nombre": conductor.get("nombre"), "tipo_usuario": tipo}
    if tipo == "conductor":
        kwargs["conductor_cedula"] = identificador
    else:
        kwargs["placa"] = identificador
    respuesta, _tools_called = run(req.mensaje, req.historial, **kwargs)
    return ChatResponse(respuesta=respuesta)


@app.get("/health")
def health():
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
    """Valida X-Hub-Signature-256 contra WA_APP_SECRET."""
    secret = os.getenv("WA_APP_SECRET", "")
    if not secret:
        # En desarrollo: si no hay secret configurado, dejar pasar pero advertir.
        logger.warning("hmac_skipped_no_secret")
        return True
    if not signature_header or not signature_header.startswith("sha256="):
        return False
    expected = "sha256=" + hmac.new(
        secret.encode("utf-8"), raw_body, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature_header)


@app.post("/webhook")
async def receive_webhook(request: Request, background_tasks: BackgroundTasks):
    raw_body  = await request.body()
    signature = request.headers.get("x-hub-signature-256")

    if not _validate_signature(raw_body, signature):
        logger.warning("hmac_invalid", extra={"ip": request.client.host if request.client else None})
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
        background_tasks.add_task(
            wa_webhook.handle_message,
            msg["from"], msg["id"], msg["text"]["body"],
        )

    return {"status": "ok"}
