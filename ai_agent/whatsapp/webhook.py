from datetime import datetime, timedelta, timezone

from db import queries
from agent.graph import run
from whatsapp.client import send_text, mark_as_read

SESSION_TTL   = timedelta(hours=8)
MAX_HISTORIAL = 20  # mensajes (10 pares usuario/asistente)

# Clave: wa_from (ej: "573001234567")
# Valor: {estado, cedula_temp, conductor_nombre_temp,
#         conductor_cedula, conductor_nombre, historial, last_activity}
_sessions: dict[str, dict] = {}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _get_session(wa_from: str) -> dict | None:
    session = _sessions.get(wa_from)
    if session and _now() - session["last_activity"] > SESSION_TTL:
        del _sessions[wa_from]
        return None
    return session


def _new_session(wa_from: str) -> dict:
    session = {
        "estado":                "esperando_cedula",
        "cedula_temp":           None,
        "conductor_nombre_temp": None,
        "conductor_cedula":      None,
        "conductor_nombre":      None,
        "historial":             [],
        "last_activity":         _now(),
    }
    _sessions[wa_from] = session
    return session


def _touch(session: dict) -> None:
    session["last_activity"] = _now()


def _primer_nombre(nombre_completo: str) -> str:
    return (nombre_completo or "").split()[0].capitalize()


def handle_message(wa_from: str, message_id: str, text: str) -> None:
    mark_as_read(message_id)

    session = _get_session(wa_from)

    if not session:
        session = _new_session(wa_from)
        send_text(wa_from, "Hola, soy el asistente de Altrans. Para continuar, escribe tu número de cédula.")
        return

    texto  = text.strip()
    estado = session["estado"]

    # ── Paso 1: recibir cédula ────────────────────────────────────────────────
    if estado == "esperando_cedula":
        conductor = queries.get_conductor_by_cedula(texto)
        if not conductor:
            send_text(wa_from, "No encontré esa cédula. Verifica el número e intenta de nuevo.")
            return

        session["cedula_temp"]           = texto
        session["conductor_nombre_temp"] = conductor["nombre"]
        session["estado"]                = "esperando_manifiesto"
        _touch(session)

        nombre = _primer_nombre(conductor["nombre"])
        send_text(wa_from, f"Hola {nombre}. Ahora escribe el número de uno de tus manifiestos para verificar tu identidad.")
        return

    # ── Paso 2: verificar manifiesto ──────────────────────────────────────────
    if estado == "esperando_manifiesto":
        if not texto.isdigit():
            send_text(wa_from, "El número de manifiesto debe ser numérico. Intenta de nuevo.")
            return

        cedula = session["cedula_temp"]
        if not queries.verificar_manifiesto_conductor(int(texto), cedula):
            send_text(wa_from, "Ese manifiesto no corresponde a tu cédula. Intenta con otro número.")
            return

        session["conductor_cedula"]  = cedula
        session["conductor_nombre"]  = session["conductor_nombre_temp"]
        session["estado"]            = "activa"
        _touch(session)

        nombre = _primer_nombre(session["conductor_nombre"])
        send_text(wa_from, f"Verificado. Bienvenido {nombre}, ¿en qué te puedo ayudar?")
        return

    # ── Sesión activa: pasar al agente ────────────────────────────────────────
    if estado == "activa":
        _touch(session)
        try:
            respuesta = run(
                texto,
                session["historial"],
                conductor_nombre=session["conductor_nombre"],
                conductor_cedula=session["conductor_cedula"],
            )
        except Exception:
            send_text(wa_from, "Ocurrió un error al procesar tu consulta. Intenta de nuevo.")
            raise

        session["historial"].append({"role": "user",      "content": texto})
        session["historial"].append({"role": "assistant",  "content": respuesta})
        if len(session["historial"]) > MAX_HISTORIAL:
            session["historial"] = session["historial"][-MAX_HISTORIAL:]

        send_text(wa_from, respuesta)
