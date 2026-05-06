import os
from fastapi import FastAPI, Depends, HTTPException, Request, BackgroundTasks
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel
from agent.graph import run
from auth import create_token, get_current_conductor
from db import queries
from whatsapp import webhook as wa_webhook

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
    respuesta = run(
        req.mensaje,
        req.historial,
        conductor_nombre=conductor["nombre"],
        conductor_cedula=conductor["cedula"],
    )
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


@app.post("/webhook")
async def receive_webhook(request: Request, background_tasks: BackgroundTasks):
    body = await request.json()
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
