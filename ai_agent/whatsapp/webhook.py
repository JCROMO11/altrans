import logging
import re
from datetime import datetime, timedelta, timezone

from db import queries
from agent.graph import run, moderate
from whatsapp.client import send_text, mark_as_read

logger = logging.getLogger(__name__)

SESSION_TTL          = timedelta(hours=8)
MAX_HISTORIAL        = 8     # 4 pares user/assistant (recorta tokens)
MAX_AUTH_FAILS       = 3
LOCKOUT_MIN          = 10
MAX_MSGS_PER_SESSION = 4     # límite de preguntas del conductor por sesión


# ── Patrones de inyección / jailbreak (primera capa, gratis) ─────────────────
_JAILBREAK_RE = re.compile(
    r'olvida\s+(tus\s+)?(instrucciones|reglas|prompt)'
    r'|ignora\s+(las\s+)?(instrucciones|reglas|anteriores?)'
    r'|borra\s+(el\s+)?(historial|contexto|memoria)'
    r'|eres\s+(ahora|un\s+nuevo|otro)'
    r'|haz\s+de\s+cuenta\s+que'
    r'|responde\s+como\s+si'
    r'|nuevo\s+rol'
    r'|cambia\s+(tu\s+)?(rol|identidad|nombre)'
    r'|act[uú]a\s+como'
    r'|pretende\s+(ser|que)'
    r'|finge\s+(ser|que)'
    r'|simula\s+(ser|que)'
    r'|repite\s+(el\s+)?(prompt|sistema|instrucciones)'
    r'|mu[eé]strame\s+(el\s+)?(prompt|sistema|instrucciones)'
    r'|sistema\s*:'
    r'|<\s*system\s*>'
    r'|"role"\s*:\s*"system"'
    r'|\{[^}]*system[^}]*\}'
    r'|prompt\s*injection'
    r'|bypass'
    r'|jailbreak'
    r'|developer\s+mode'
    r'|modo\s+(desarrollador|admin|administrador)'
    r'|dan\s+(mode|modo)'
    r'|ignore\s+previous'
    r'|disregard\s+(all\s+)?(previous|prior)'
    r'|forget\s+(your\s+)?(instructions|rules)'
    r'|reveal\s+(your\s+)?(prompt|instructions|system)'
    r'|para\s+verificar\s+(la\s+)?(integridad|seguridad|el\s+sistema)'
    r'|como\s+prueba\s+del\s+sistema'
    r'|en\s+nombre\s+de\s+(altrans|la\s+empresa|administraci[oó]n)'
    r'|registros\s+de\s+otros\s+(conductores?|usuarios?)'
    r'|todos\s+los\s+(conductores?|registros|manifiestos)\s+(de\s+)?(la\s+empresa|altrans|del\s+sistema)'
    r'|acceso\s+(de\s+)?(administrador|admin|root|superusuario)'
    r'|modo\s+(prueba|test|debug|dios)'
    r'|eres\s+libre',
    re.IGNORECASE,
)


# ── Helpers de tiempo ────────────────────────────────────────────────────────

def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_ts(value) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


# ── Helpers de sesión (persistidas en Supabase) ──────────────────────────────

def _new_session(wa_from: str) -> dict:
    session = {
        "wa_from":               wa_from,
        "estado":                "esperando_cedula",
        "cedula_temp":           None,
        "conductor_nombre_temp": None,
        "conductor_cedula":      None,
        "conductor_nombre":      None,
        "historial":             [],
        "msg_count":             0,
        "last_activity":         _now().isoformat(),
        "auth_fails":            0,
        "locked_until":          None,
    }
    queries.upsert_session(session)
    return session


def _load_session(wa_from: str) -> dict | None:
    session = queries.get_session(wa_from)
    if not session:
        return None
    last = _parse_ts(session["last_activity"])
    if _now() - last > SESSION_TTL:
        queries.delete_session(wa_from)
        return None
    return session


def _save(session: dict) -> None:
    session["last_activity"] = _now().isoformat()
    queries.upsert_session(session)


def _register_fail(session: dict) -> bool:
    session["auth_fails"] += 1
    if session["auth_fails"] >= MAX_AUTH_FAILS:
        session["locked_until"] = (_now() + timedelta(minutes=LOCKOUT_MIN)).isoformat()
        return True
    return False


def _is_locked(session: dict) -> bool:
    until = session.get("locked_until")
    if not until:
        return False
    if _now() < _parse_ts(until):
        return True
    session["locked_until"] = None
    session["auth_fails"]   = 0
    session["estado"]       = "esperando_cedula"
    return False


def _primer_nombre(nombre_completo: str) -> str:
    return (nombre_completo or "").split()[0].capitalize()


# ── Handler principal ────────────────────────────────────────────────────────

def handle_message(wa_from: str, message_id: str, text: str) -> None:
    # Idempotencia: si Meta reintenta el webhook, ignorar el duplicado.
    try:
        if not queries.mark_message_processed(message_id):
            logger.info("dup_message", extra={"wa_from": wa_from, "message_id": message_id})
            return
    except Exception:
        logger.exception("idempotency_check_failed", extra={"wa_from": wa_from})

    try:
        mark_as_read(message_id)
    except Exception:
        pass

    session = _load_session(wa_from)

    if not session:
        session = _new_session(wa_from)
        send_text(wa_from, "Hola, soy el asistente de Altrans. Para continuar, escribe tu número de cédula.")
        logger.info("session_new", extra={"wa_from": wa_from})
        return

    if _is_locked(session):
        mins_restantes = int((_parse_ts(session["locked_until"]) - _now()).total_seconds() / 60) + 1
        send_text(wa_from, f"Tu acceso está bloqueado temporalmente por intentos fallidos. Intenta de nuevo en {mins_restantes} minuto(s).")
        return

    texto  = text.strip()
    estado = session["estado"]

    # ── Paso 1: cédula ───────────────────────────────────────────────────────
    if estado == "esperando_cedula":
        cedula_limpia = re.sub(r'\D', '', texto)
        if not cedula_limpia:
            send_text(wa_from, "Por favor escribe solo el número de tu cédula, sin puntos ni espacios.")
            return

        conductor = queries.get_conductor_by_cedula(cedula_limpia)
        if not conductor:
            bloqueado = _register_fail(session)
            _save(session)
            if bloqueado:
                send_text(wa_from, f"Cédula no encontrada. Acceso bloqueado por {LOCKOUT_MIN} minutos por múltiples intentos fallidos.")
                logger.warning("auth_locked", extra={"wa_from": wa_from, "stage": "cedula"})
            else:
                restantes = MAX_AUTH_FAILS - session["auth_fails"]
                send_text(wa_from, f"No encontré esa cédula. Verifica el número e intenta de nuevo. ({restantes} intento(s) restante(s))")
            return

        session["cedula_temp"]           = cedula_limpia
        session["conductor_nombre_temp"] = conductor["nombre"]
        session["estado"]                = "esperando_manifiesto"
        _save(session)

        nombre = _primer_nombre(conductor["nombre"])
        send_text(wa_from, f"Hola {nombre}. Ahora escribe el número de uno de tus manifiestos para verificar tu identidad.")
        return

    # ── Paso 2: manifiesto ───────────────────────────────────────────────────
    if estado == "esperando_manifiesto":
        manifiesto_limpio = re.sub(r'\D', '', texto)
        if not manifiesto_limpio:
            send_text(wa_from, "El número de manifiesto debe ser numérico. Intenta de nuevo.")
            return

        cedula = session["cedula_temp"]
        if not queries.verificar_manifiesto_conductor(int(manifiesto_limpio), cedula):
            bloqueado = _register_fail(session)
            _save(session)
            if bloqueado:
                send_text(wa_from, f"Manifiesto incorrecto. Acceso bloqueado por {LOCKOUT_MIN} minutos por múltiples intentos fallidos.")
                logger.warning("auth_locked", extra={"wa_from": wa_from, "stage": "manifiesto", "cedula": cedula})
            else:
                restantes = MAX_AUTH_FAILS - session["auth_fails"]
                send_text(wa_from, f"Ese manifiesto no corresponde a tu cédula. Intenta con otro número. ({restantes} intento(s) restante(s))")
            return

        session["conductor_cedula"] = cedula
        session["conductor_nombre"] = session["conductor_nombre_temp"]
        session["estado"]           = "activa"
        session["auth_fails"]       = 0
        _save(session)

        nombre = _primer_nombre(session["conductor_nombre"])
        send_text(wa_from, f"Verificado. Bienvenido {nombre}, tienes {MAX_MSGS_PER_SESSION} consultas disponibles en esta sesión. ¿En qué te puedo ayudar?")
        logger.info("auth_ok", extra={"wa_from": wa_from, "cedula": cedula})
        return

    # ── Sesión activa ────────────────────────────────────────────────────────
    if estado == "activa":
        if not session.get("conductor_cedula"):
            queries.delete_session(wa_from)
            send_text(wa_from, "Tu sesión expiró. Por favor escribe tu cédula para volver a ingresar.")
            return

        cedula = session["conductor_cedula"]

        # Capa 1: regex barata
        if _JAILBREAK_RE.search(texto):
            queries.log_jailbreak(wa_from, cedula, texto, "regex")
            logger.warning("jailbreak_blocked", extra={
                "wa_from": wa_from, "cedula": cedula, "layer": "regex",
            })
            send_text(wa_from, "Ese tipo de mensaje no está permitido. Si tienes una consulta sobre tus manifiestos, con gusto te ayudo.")
            return

        # Capa 2: moderación LLM (solo si la regex no detectó nada y el texto
        # es sospechoso por longitud o caracteres raros)
        if len(texto) > 60 or any(c in texto for c in ("{", "<", "[INST]", "```")):
            try:
                if moderate(texto):
                    queries.log_jailbreak(wa_from, cedula, texto, "llm")
                    logger.warning("jailbreak_blocked", extra={
                        "wa_from": wa_from, "cedula": cedula, "layer": "llm",
                    })
                    send_text(wa_from, "Ese tipo de mensaje no está permitido. Si tienes una consulta sobre tus manifiestos, con gusto te ayudo.")
                    return
            except Exception:
                logger.exception("moderation_failed", extra={"wa_from": wa_from})
                # No bloquear el flujo si la moderación falla — la regex ya filtró

        # Límite de mensajes por sesión
        if session["msg_count"] >= MAX_MSGS_PER_SESSION:
            send_text(wa_from, f"Has alcanzado el límite de {MAX_MSGS_PER_SESSION} consultas en esta sesión. Tu acceso se renovará en unas horas o puedes contactar a tu supervisor.")
            logger.info("msg_limit_reached", extra={"wa_from": wa_from, "cedula": cedula})
            return

        try:
            respuesta = run(
                texto,
                session["historial"],
                conductor_nombre=session["conductor_nombre"],
                conductor_cedula=cedula,
            )
        except Exception:
            logger.exception("agent_error", extra={"wa_from": wa_from, "cedula": cedula})
            send_text(wa_from, "Ocurrió un error al procesar tu consulta. Intenta de nuevo.")
            return

        # Salvaguarda final: si por alguna razón la respuesta está vacía,
        # WhatsApp rechaza con 400. Sustituir por mensaje genérico.
        if not respuesta or not respuesta.strip():
            logger.warning("empty_agent_reply", extra={"wa_from": wa_from, "cedula": cedula, "len_in": len(texto)})
            respuesta = "No pude completar tu consulta. Intenta reformularla o pregúntame de otra forma."

        session["historial"].append({"role": "user",      "content": texto})
        session["historial"].append({"role": "assistant", "content": respuesta})
        if len(session["historial"]) > MAX_HISTORIAL:
            session["historial"] = session["historial"][-MAX_HISTORIAL:]
        session["msg_count"] += 1
        _save(session)

        logger.info("agent_reply", extra={
            "wa_from": wa_from, "cedula": cedula,
            "msg_count": session["msg_count"], "len_in": len(texto), "len_out": len(respuesta),
        })

        send_text(wa_from, respuesta)
        # Aviso de cierre cuando quede 1 consulta
        if session["msg_count"] == MAX_MSGS_PER_SESSION - 1:
            send_text(wa_from, "📌 Te queda 1 consulta en esta sesión.")
