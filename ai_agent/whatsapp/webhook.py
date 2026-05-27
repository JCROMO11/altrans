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
        "estado":                "esperando_identificador",
        # Nuevo flujo unificado:
        "tipo_usuario":          None,             # 'conductor' | 'propietario'
        "identificador_temp":    None,             # cédula o placa antes de verificar manifiesto
        "identificador_auth":    None,             # cédula o placa ya verificada
        "nombre_temp":           None,
        "nombre":                None,
        # Campos legacy (mantienen compat con sesiones existentes):
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


# Placa colombiana: 3 letras + 3 dígitos (carros), 3 letras + 2 dígitos + 1 letra (motos),
# o variantes para tráileres. Detección permisiva: cualquier mezcla de letras y dígitos.
_PLACA_RE = re.compile(r"^[A-Z]{2,3}[\s-]?\d{2,4}[A-Z]?$", re.IGNORECASE)


def _detectar_tipo_usuario(texto: str) -> tuple[str, str] | None:
    """Devuelve ('conductor', cedula_limpia) o ('propietario', placa) según el formato.
    None si no se reconoce.

    Acepta cédulas con o sin separadores (CC 1.130.668.182 → 1130668182) y
    placas en cualquier capitalización (abc-123 → ABC123).
    """
    t = texto.strip().upper()
    # Si extrayendo solo dígitos obtenemos una cédula válida y NO hay otras letras
    # significativas (aparte de prefijos como CC, C.C., NIT), tratar como cédula.
    digitos = re.sub(r"\D", "", t)
    letras = re.sub(r"[^A-Z]", "", t)
    if 5 <= len(digitos) <= 12 and letras in ("", "CC", "C", "NIT", "TI", "CE"):
        return ("conductor", digitos)
    # Para placa, normalizar quitando separadores
    t_limpio = re.sub(r"[\s\-\.]", "", t)
    if _PLACA_RE.match(t_limpio):
        return ("propietario", t_limpio)
    # Fallback: letras+dígitos sin formato estándar, intentar como placa
    if any(c.isalpha() for c in t_limpio) and any(c.isdigit() for c in t_limpio) and len(t_limpio) <= 8:
        return ("propietario", t_limpio)
    return None


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
    session["estado"]       = "esperando_identificador"
    return False


def _primer_nombre(nombre_completo: str) -> str:
    partes = (nombre_completo or "").split()
    return partes[0].capitalize() if partes else ""


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
        send_text(
            wa_from,
            "¡Hola! 👋 Bienvenido al asistente de Altrans.\n\n"
            "Puedo ayudarte a consultar tus manifiestos, fletes, pagos y más.\n\n"
            "Para empezar, cuéntame:\n"
            "- Si eres *conductor*, escribe tu número de cédula.\n"
            "- Si eres *propietario de vehículo*, escribe la placa."
        )
        logger.info("session_new", extra={"wa_from": wa_from})
        return

    if _is_locked(session):
        mins_restantes = int((_parse_ts(session["locked_until"]) - _now()).total_seconds() / 60) + 1
        send_text(wa_from, f"Tu acceso está bloqueado temporalmente por intentos fallidos. Intenta de nuevo en {mins_restantes} minuto(s).")
        return

    texto  = text.strip()
    estado = session["estado"]

    # ── Paso 1: identificador (cédula o placa) ──────────────────────────────
    if estado in ("esperando_identificador", "esperando_cedula"):
        deteccion = _detectar_tipo_usuario(texto)
        if not deteccion:
            send_text(wa_from, "No reconozco ese formato. Escribe solo tu cédula (números) o la placa de tu vehículo (ej: ABC123).")
            return

        tipo, identificador = deteccion

        if tipo == "conductor":
            registro = queries.get_conductor_by_cedula(identificador)
            etiqueta_err = "cédula"
        else:
            registro = queries.get_propietario_by_placa(identificador)
            etiqueta_err = "placa"

        if not registro:
            bloqueado = _register_fail(session)
            _save(session)
            if bloqueado:
                send_text(wa_from, f"{etiqueta_err.capitalize()} no encontrada. Acceso bloqueado por {LOCKOUT_MIN} minutos por múltiples intentos fallidos.")
                logger.warning("auth_locked", extra={"wa_from": wa_from, "stage": "identificador", "tipo": tipo})
            else:
                restantes = MAX_AUTH_FAILS - session["auth_fails"]
                send_text(wa_from, f"No encontré esa {etiqueta_err}. Verifica e intenta de nuevo. ({restantes} intento(s) restante(s))")
            return

        session["tipo_usuario"]       = tipo
        session["identificador_temp"] = identificador
        session["nombre_temp"]        = registro["nombre"]
        session["estado"]             = "esperando_manifiesto"
        _save(session)

        nombre = _primer_nombre(registro["nombre"])
        if tipo == "conductor":
            mensaje = f"Hola {nombre}. Ahora escribe el número de uno de tus manifiestos para verificar tu identidad."
        else:
            mensaje = f"Hola {nombre}. Ahora escribe el número de un manifiesto reciente de tu vehículo para verificar."
        send_text(wa_from, mensaje)
        return

    # ── Paso 2: manifiesto ───────────────────────────────────────────────────
    if estado == "esperando_manifiesto":
        manifiesto_limpio = re.sub(r'\D', '', texto)
        if not manifiesto_limpio:
            send_text(wa_from, "El número de manifiesto debe ser numérico. Intenta de nuevo.")
            return

        tipo = session.get("tipo_usuario") or "conductor"
        identificador = session.get("identificador_temp") or session.get("cedula_temp")
        nombre_temp = session.get("nombre_temp") or session.get("conductor_nombre_temp") or ""

        if tipo == "conductor":
            ok = queries.verificar_manifiesto_conductor(int(manifiesto_limpio), identificador)
            err_msg = "Ese manifiesto no corresponde a tu cédula."
        else:
            ok = queries.verificar_manifiesto_propietario(int(manifiesto_limpio), identificador)
            err_msg = "Ese manifiesto no corresponde a tu placa."

        if not ok:
            bloqueado = _register_fail(session)
            _save(session)
            if bloqueado:
                send_text(wa_from, f"Manifiesto incorrecto. Acceso bloqueado por {LOCKOUT_MIN} minutos por múltiples intentos fallidos.")
                logger.warning("auth_locked", extra={"wa_from": wa_from, "stage": "manifiesto", "tipo": tipo})
            else:
                restantes = MAX_AUTH_FAILS - session["auth_fails"]
                send_text(wa_from, f"{err_msg} Intenta con otro número. ({restantes} intento(s) restante(s))")
            return

        session["identificador_auth"] = identificador
        session["nombre"]             = nombre_temp
        # Espejar a campos legacy para retrocompatibilidad de sesiones existentes
        if tipo == "conductor":
            session["conductor_cedula"] = identificador
            session["conductor_nombre"] = nombre_temp
        session["estado"]      = "activa"
        session["auth_fails"]  = 0
        _save(session)

        nombre = _primer_nombre(nombre_temp)
        rol = "conductor" if tipo == "conductor" else "propietario"
        send_text(wa_from, f"Verificado. Bienvenido {nombre} ({rol}), tienes {MAX_MSGS_PER_SESSION} consultas disponibles en esta sesión. ¿En qué te puedo ayudar?")
        logger.info("auth_ok", extra={"wa_from": wa_from, "tipo": tipo, "identificador": identificador})
        return

    # ── Sesión activa ────────────────────────────────────────────────────────
    if estado == "activa":
        tipo = session.get("tipo_usuario") or "conductor"
        identificador = session.get("identificador_auth") or session.get("conductor_cedula")
        if not identificador:
            queries.delete_session(wa_from)
            send_text(wa_from, "Tu sesión expiró. Escribe tu cédula o placa para volver a ingresar.")
            return

        # Nombre para los logs y compatibilidad con código existente
        cedula = identificador  # usado solo para logging/jailbreak; ya no implica conductor

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
            run_kwargs = {
                "nombre":       session.get("nombre") or session.get("conductor_nombre"),
                "tipo_usuario": tipo,
            }
            if tipo == "conductor":
                run_kwargs["conductor_cedula"] = identificador
            else:
                run_kwargs["placa"] = identificador
            respuesta, tools_called = run(
                texto,
                session["historial"],
                **run_kwargs,
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

        # Normalizar negritas: ** → * (WhatsApp solo soporta un asterisco)
        respuesta = re.sub(r'\*\*(.+?)\*\*', r'*\1*', respuesta)

        session["historial"].append({"role": "user",      "content": texto})
        session["historial"].append({"role": "assistant", "content": respuesta})
        if len(session["historial"]) > MAX_HISTORIAL:
            session["historial"] = session["historial"][-MAX_HISTORIAL:]
        # Solo descontamos del límite cuando el agente realmente consultó datos.
        # Aclaraciones, saludos, rechazos de jailbreak o respuestas fuera de
        # alcance no llaman herramientas y no deberían gastar consultas.
        if tools_called:
            session["msg_count"] += 1
        _save(session)

        logger.info("agent_reply", extra={
            "wa_from": wa_from, "cedula": cedula,
            "msg_count": session["msg_count"], "tools_called": tools_called,
            "len_in": len(texto), "len_out": len(respuesta),
        })

        send_text(wa_from, respuesta)
        # Aviso de cierre cuando quede 1 consulta (solo si efectivamente consumimos una)
        if tools_called and session["msg_count"] == MAX_MSGS_PER_SESSION - 1:
            send_text(wa_from, "📌 Te queda 1 consulta en esta sesión.")
