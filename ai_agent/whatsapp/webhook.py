import re
from datetime import datetime, timedelta, timezone

import bcrypt
from loguru import logger

from db import queries
from agent.graph import run, moderate
from whatsapp.client import send_text, mark_as_read
from core.rate_limiter import rate_limiter

SESSION_TTL          = timedelta(hours=8)
MAX_HISTORIAL        = 8
MAX_AUTH_FAILS       = 3
LOCKOUT_MIN          = 10
MAX_MSGS_PER_SESSION = 4


# ── Patrones de inyección / jailbreak ─────────────────────────────────────────

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

_TIPS = (
    "📝 Escribe cada consulta completa en un solo mensaje. Tienes "
    f"{MAX_MSGS_PER_SESSION} consultas por sesión — úsalas para preguntas "
    "concretas como '¿cuánto me deben de junio?' o 'muéstrame mis manifiestos "
    "con novedades'."
)


# ── Helpers de tiempo ────────────────────────────────────────────────────────

def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_ts(value) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


# ── Helpers de sesión ─────────────────────────────────────────────────────────

async def _new_session(wa_from: str) -> dict:
    session = {
        "wa_from":               wa_from,
        "estado":                "esperando_identificador",
        "tipo_usuario":          None,
        "identificador_temp":    None,
        "identificador_auth":    None,
        "nombre_temp":           None,
        "nombre":                None,
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
    await queries.upsert_session(session)
    return session


_PLACA_RE = re.compile(r"^[A-Z]{2,3}[\s-]?\d{2,4}[A-Z]?$", re.IGNORECASE)


def _detectar_tipo_usuario(texto: str) -> tuple[str, str] | None:
    t = texto.strip().upper()
    digitos = re.sub(r"\D", "", t)
    letras = re.sub(r"[^A-Z]", "", t)
    if 5 <= len(digitos) <= 12 and letras in ("", "CC", "C", "NIT", "TI", "CE"):
        return ("conductor", digitos)
    t_limpio = re.sub(r"[\s\-\.]", "", t)
    if _PLACA_RE.match(t_limpio):
        return ("propietario", t_limpio)
    if any(c.isalpha() for c in t_limpio) and any(c.isdigit() for c in t_limpio) and len(t_limpio) <= 8:
        return ("propietario", t_limpio)
    return None


async def _load_session(wa_from: str) -> dict | None:
    session = await queries.get_session(wa_from)
    if not session:
        return None
    last = _parse_ts(session["last_activity"])
    if _now() - last > SESSION_TTL:
        await queries.delete_session(wa_from)
        return None
    return session


async def _save(session: dict) -> None:
    session["last_activity"] = _now().isoformat()
    await queries.upsert_session(session)


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

async def handle_message(wa_from: str, message_id: str, text: str) -> None:
    try:
        if not await queries.mark_message_processed(message_id):
            logger.info("dup_message", wa_from=wa_from, message_id=message_id)
            return
    except Exception:
        logger.exception("idempotency_check_failed", wa_from=wa_from)

    try:
        await mark_as_read(message_id)
    except Exception:
        pass

    message_data = {"wa_from": wa_from, "message_id": message_id, "text": text}
    ok, reason = await rate_limiter.try_acquire(wa_from, message_data)

    if not ok:
        if reason == "rate_limited":
            await send_text(wa_from,
                "⏳ Has enviado muchos mensajes en poco tiempo. "
                "Espera un momento antes de continuar.")
        elif reason == "queued":
            await send_text(wa_from,
                "⏳ Estoy procesando tu consulta anterior. "
                "Tu mensaje está en espera y será procesado en breve.")
        return

    try:
        await _process_message(wa_from, message_id, text)
    finally:
        next_msg = await rate_limiter.release(wa_from)
        if next_msg:
            await handle_message(**next_msg)


async def _process_message(wa_from: str, message_id: str, text: str) -> None:
    session = await _load_session(wa_from)

    if not session:
        admin = await queries.get_admin_by_wa_from(wa_from)
        if admin:
            session = {
                "wa_from":               wa_from,
                "estado":                "esperando_admin_pass",
                "tipo_usuario":          None,
                "admin_rol":             admin.get("rol"),
                "identificador_temp":    None,
                "identificador_auth":    None,
                "nombre_temp":           admin["nombre"],
                "nombre":                None,
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
            await queries.upsert_session(session)
            nombre_admin = _primer_nombre(admin["nombre"])
            await send_text(
                wa_from,
                f"Bienvenido {nombre_admin}. 👋\n\n"
                "Tienes acceso a datos consolidados de Altrans.\n\n"
                "Ingresa tu contraseña de administrador para continuar."
            )
            logger.info("admin_auth_prompt", wa_from=wa_from, nombre=admin["nombre"])
            return

        session = await _new_session(wa_from)
        await send_text(
            wa_from,
            "¡Hola! 👋 Bienvenido al asistente de Altrans.\n\n"
            "Puedo ayudarte a consultar tus manifiestos, fletes, pagos y más.\n\n"
            "Para empezar, cuéntame:\n"
            "- Si eres *conductor*, escribe tu número de cédula.\n"
            "- Si eres *propietario de vehículo*, escribe la placa.\n\n"
            f"{_TIPS}"
        )
        logger.info("session_new", wa_from=wa_from)
        return

    if _is_locked(session):
        mins_restantes = int((_parse_ts(session["locked_until"]) - _now()).total_seconds() / 60) + 1
        await send_text(wa_from,
            f"Tu acceso está bloqueado temporalmente por intentos fallidos. "
            f"Intenta de nuevo en {mins_restantes} minuto(s).")
        return

    texto  = text.strip()
    estado = session["estado"]

    # ── Admin: validar contraseña ─────────────────────────────────────────────
    if estado == "esperando_admin_pass":
        admin = await queries.get_admin_by_wa_from(wa_from)
        if not admin:
            await queries.delete_session(wa_from)
            await send_text(wa_from, "Error de sesión. Escribe de nuevo para empezar.")
            return

        if bcrypt.checkpw(texto.encode("utf-8"), admin["password_hash"].encode("utf-8")):
            session["estado"]             = "activa"
            session["tipo_usuario"]       = "admin"
            session["nombre"]             = admin["nombre"]
            session["auth_fails"]         = 0
            session["nombre_temp"]        = None
            await _save(session)
            await queries.update_admin_ultimo_acceso(wa_from)
            nombre_admin = _primer_nombre(admin["nombre"])
            await send_text(wa_from,
                f"Verificado. Bienvenido {nombre_admin}, tienes acceso a los "
                "datos consolidados de la empresa. ¿En qué te puedo ayudar?")
            logger.info("admin_auth_ok", wa_from=wa_from, nombre=admin["nombre"])
        else:
            bloqueado = _register_fail(session)
            await _save(session)
            if bloqueado:
                await send_text(wa_from,
                    f"Contraseña incorrecta. Acceso bloqueado por {LOCKOUT_MIN} minutos.")
                logger.warning("admin_auth_locked", wa_from=wa_from)
            else:
                restantes = MAX_AUTH_FAILS - session["auth_fails"]
                await send_text(wa_from,
                    f"Contraseña incorrecta. ({restantes} intento(s) restante(s))")
        return

    # ── Paso 1: identificador ─────────────────────────────────────────────────
    if estado in ("esperando_identificador", "esperando_cedula"):
        deteccion = _detectar_tipo_usuario(texto)
        if not deteccion:
            await send_text(wa_from,
                "No reconozco ese formato. Escribe solo tu cédula (números) o "
                "la placa de tu vehículo (ej: ABC123).")
            return

        tipo, identificador = deteccion

        if tipo == "conductor":
            registro = await queries.get_conductor_by_cedula(identificador)
            etiqueta_err = "cédula"
        else:
            registro = await queries.get_propietario_by_placa(identificador)
            etiqueta_err = "placa"

        if not registro:
            bloqueado = _register_fail(session)
            await _save(session)
            if bloqueado:
                await send_text(wa_from,
                    f"{etiqueta_err.capitalize()} no encontrada. Acceso bloqueado "
                    f"por {LOCKOUT_MIN} minutos por múltiples intentos fallidos.")
                logger.warning("auth_locked", wa_from=wa_from, stage="identificador", tipo=tipo)
            else:
                restantes = MAX_AUTH_FAILS - session["auth_fails"]
                await send_text(wa_from,
                    f"No encontré esa {etiqueta_err}. Verifica e intenta de nuevo. "
                    f"({restantes} intento(s) restante(s))")
            return

        session["tipo_usuario"]       = tipo
        session["identificador_temp"] = identificador
        session["nombre_temp"]        = registro["nombre"]
        session["estado"]             = "esperando_manifiesto"
        await _save(session)

        nombre = _primer_nombre(registro["nombre"])
        if tipo == "conductor":
            mensaje = (f"Hola {nombre}. Ahora escribe el número de uno de tus "
                       "manifiestos para verificar tu identidad.")
        else:
            mensaje = (f"Hola {nombre}. Ahora escribe el número de un manifiesto "
                       "reciente de tu vehículo para verificar.")
        await send_text(wa_from, mensaje)
        return

    # ── Paso 2: manifiesto ───────────────────────────────────────────────────
    if estado == "esperando_manifiesto":
        manifiesto_limpio = re.sub(r'\D', '', texto)
        if not manifiesto_limpio:
            await send_text(wa_from, "El número de manifiesto debe ser numérico. Intenta de nuevo.")
            return

        tipo = session.get("tipo_usuario") or "conductor"
        identificador = session.get("identificador_temp") or session.get("cedula_temp")
        nombre_temp = session.get("nombre_temp") or session.get("conductor_nombre_temp") or ""

        if tipo == "conductor":
            ok = await queries.verificar_manifiesto_conductor(int(manifiesto_limpio), identificador)
            err_msg = "Ese manifiesto no corresponde a tu cédula."
        else:
            ok = await queries.verificar_manifiesto_propietario(int(manifiesto_limpio), identificador)
            err_msg = "Ese manifiesto no corresponde a tu placa."

        if not ok:
            bloqueado = _register_fail(session)
            await _save(session)
            if bloqueado:
                await send_text(wa_from,
                    f"Manifiesto incorrecto. Acceso bloqueado por {LOCKOUT_MIN} "
                    "minutos por múltiples intentos fallidos.")
                logger.warning("auth_locked", wa_from=wa_from, stage="manifiesto", tipo=tipo)
            else:
                restantes = MAX_AUTH_FAILS - session["auth_fails"]
                await send_text(wa_from,
                    f"{err_msg} Intenta con otro número. ({restantes} intento(s) restante(s))")
            return

        session["identificador_auth"] = identificador
        session["nombre"]             = nombre_temp
        if tipo == "conductor":
            session["conductor_cedula"] = identificador
            session["conductor_nombre"] = nombre_temp
        session["estado"]      = "activa"
        session["auth_fails"]  = 0
        await _save(session)

        nombre = _primer_nombre(nombre_temp)
        rol = "conductor" if tipo == "conductor" else "propietario"
        await send_text(wa_from,
            f"Verificado. Bienvenido {nombre} ({rol}).\n\n"
            f"{_TIPS}\n\n"
            "¿En qué te puedo ayudar?")
        logger.info("auth_ok", wa_from=wa_from, tipo=tipo, identificador=identificador)
        return

    # ── Sesión activa ────────────────────────────────────────────────────────
    if estado == "activa":
        tipo = session.get("tipo_usuario") or "conductor"
        admin_rol = session.get("admin_rol")
        identificador = session.get("identificador_auth") or session.get("conductor_cedula")
        if not identificador and not admin_rol:
            await queries.delete_session(wa_from)
            await send_text(wa_from, "Tu sesión expiró. Escribe tu cédula o placa para volver a ingresar.")
            return

        cedula = identificador

        # Capa 1: regex
        if session.get("tipo_usuario") != "admin" and _JAILBREAK_RE.search(texto):
            await queries.log_jailbreak(wa_from, cedula, texto, "regex")
            logger.warning("jailbreak_blocked", wa_from=wa_from, cedula=cedula, layer="regex")
            await send_text(wa_from,
                "Ese tipo de mensaje no está permitido. Si tienes una consulta "
                "sobre tus manifiestos, con gusto te ayudo.")
            return

        # Capa 2: moderación LLM
        if session.get("tipo_usuario") != "admin" and (len(texto) > 60 or any(c in texto for c in ("{", "<", "[INST]", "```"))):
            try:
                if await moderate(texto):
                    await queries.log_jailbreak(wa_from, cedula, texto, "llm")
                    logger.warning("jailbreak_blocked", wa_from=wa_from, cedula=cedula, layer="llm")
                    await send_text(wa_from,
                        "Ese tipo de mensaje no está permitido. Si tienes una consulta "
                        "sobre tus manifiestos, con gusto te ayudo.")
                    return
            except Exception:
                logger.exception("moderation_failed", wa_from=wa_from)

        # Límite de mensajes por sesión
        if session.get("tipo_usuario") != "admin" and session["msg_count"] >= MAX_MSGS_PER_SESSION:
            await send_text(wa_from,
                f"Has alcanzado el límite de {MAX_MSGS_PER_SESSION} consultas en "
                "esta sesión. Tu acceso se renovará en unas horas o puedes "
                "contactar a tu supervisor.")
            logger.info("msg_limit_reached", wa_from=wa_from, cedula=cedula)
            return

        try:
            run_kwargs = {
                "nombre":       session.get("nombre") or session.get("conductor_nombre"),
                "tipo_usuario": tipo,
                "admin_rol":    session.get("admin_rol"),
            }
            if tipo == "conductor":
                run_kwargs["conductor_cedula"] = identificador
            elif tipo == "propietario":
                run_kwargs["placa"] = identificador
            respuesta, tools_called = await run(
                texto,
                session["historial"],
                **run_kwargs,
            )
        except Exception:
            logger.exception("agent_error", wa_from=wa_from, cedula=cedula)
            await send_text(wa_from, "Ocurrió un error al procesar tu consulta. Intenta de nuevo.")
            return

        if not respuesta or not respuesta.strip():
            logger.warning("empty_agent_reply", wa_from=wa_from, cedula=cedula, len_in=len(texto))
            respuesta = "No pude completar tu consulta. Intenta reformularla o pregúntame de otra forma."

        respuesta = re.sub(r'\*\*(.+?)\*\*', r'*\1*', respuesta)

        session["historial"].append({"role": "user",      "content": texto})
        session["historial"].append({"role": "assistant", "content": respuesta})
        if len(session["historial"]) > MAX_HISTORIAL:
            session["historial"] = session["historial"][-MAX_HISTORIAL:]
        if tools_called:
            session["msg_count"] += 1
        await _save(session)

        logger.info("agent_reply",
                    wa_from=wa_from, cedula=cedula,
                    msg_count=session["msg_count"], tools_called=tools_called,
                    len_in=len(texto), len_out=len(respuesta))

        await send_text(wa_from, respuesta)
        if tools_called and session["msg_count"] == MAX_MSGS_PER_SESSION - 1:
            await send_text(wa_from, "📌 Te queda 1 consulta en esta sesión.")