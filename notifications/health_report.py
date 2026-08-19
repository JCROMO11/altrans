"""
Chequeo matutino de salud — "¿todo listo para trabajar?".

Revisa en un solo lugar el estado de todos los módulos de ALTRANS:

  Servicios
    - Chatbot        → GET /health  (ai_agent en Railway)
    - Notifications  → GET /health  (este mismo servicio)
    - Dashboard      → GET /        (SPA en Railway, si DASHBOARD_URL existe)

  Infraestructura / datos
    - WA_TOKEN vigente (debug_token de Meta) → cuántas horas quedan
    - Supabase accesible + filas de manifiestos_flat
    - Última actualización de manifiestos_flat (ETL/dashboard)
    - Backup del día anterior (log scheduler_backup_done en app_logs)

  Servicios IA (cadena LLM del chatbot)
    - DeepSeek (primario): key + balance vía /user/balance
    - OpenRouter (alt): key + uso/límite vía /auth/key
    - Groq (última línea, free): key vía /models

  Actividad del chatbot / notificaciones
    - Auto-notify de hoy (messages_sent: enviados y con error)
    - Sesiones activas y bloqueadas
    - Jailbreaks (últimas 24h) y errores ERROR en app_logs (24h)

Genera un resumen de texto plano, lo envía por WhatsApp (si MORNING_REPORT_TO
está definido) y/o por email vía Brevo (si MORNING_REPORT_EMAIL está definido),
y lo registra en logs como health_report_done / health_report_failed.

Uso:
    from health_report import run_morning_check
    run_morning_check()
"""
import logging
import os
from datetime import datetime, timedelta, timezone

import httpx
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

_GRAPH = "https://graph.facebook.com/v23.0"
_COLOMBIA = timezone(timedelta(hours=-5))
_TIMEOUT = 10

_OK, _WARN, _FAIL = "OK", "WARN", "FAIL"


# ── Helpers Supabase REST ────────────────────────────────────────────────────

def _supabase_headers() -> dict:
    key = os.environ["SUPABASE_SERVICE_KEY"]
    return {"apikey": key, "Authorization": f"Bearer {key}"}


def _supabase_url() -> str:
    return os.environ["SUPABASE_URL"].rstrip("/")


def _count(client: httpx.Client, table: str, params: dict | None = None) -> int:
    """Conteo exacto vía header content-range (patrón de backup_email)."""
    url = f"{_supabase_url()}/rest/v1/{table}"
    r = client.head(
        url,
        headers={**_supabase_headers(), "Prefer": "count=exact", "Range": "0-0"},
        params=params,
        timeout=_TIMEOUT,
    )
    r.raise_for_status()
    content_range = r.headers.get("content-range", "")
    total = content_range.split("/")[-1]
    return int(total) if total.isdigit() else -1


def _iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


# ── Chequeos individuales ────────────────────────────────────────────────────

def _check_service(url: str) -> tuple[str, str]:
    """Devuelve (estado, detalle) para un GET de salud HTTP."""
    try:
        r = httpx.get(url, timeout=_TIMEOUT, follow_redirects=True)
        if r.status_code == 200:
            try:
                body = r.json()
                if body.get("status") == "ok":
                    return _OK, f"HTTP 200"
                return _WARN, f"HTTP 200 pero status={body.get('status')}"
            except Exception:
                return _OK, "HTTP 200"
        return _FAIL, f"HTTP {r.status_code}"
    except Exception as exc:
        return _FAIL, str(exc)[:80]


def _check_wa_token() -> tuple[str, str]:
    token = os.environ.get("WA_TOKEN", "")
    if not token:
        return _FAIL, "WA_TOKEN vacío en entorno"
    try:
        r = httpx.get(
            f"{_GRAPH}/debug_token",
            params={"input_token": token},
            headers={"Authorization": f"Bearer {token}"},
            timeout=_TIMEOUT,
        )
        r.raise_for_status()
        data = r.json().get("data", {})
        if not data.get("is_valid"):
            return _FAIL, "token inválido o expirado"
        expires = data.get("expires_at")
        if expires:
            dt = datetime.fromtimestamp(expires, tz=timezone.utc)
            horas = (dt - datetime.now(timezone.utc)).total_seconds() / 3600
            if horas <= 0:
                return _FAIL, "token expirado"
            if horas < 24:
                return _WARN, f"expira en {horas:.1f}h ({dt.strftime('%d/%m %H:%M')} UTC)"
            return _OK, f"expira en {horas:.0f}h ({dt.strftime('%d/%m')} UTC)"
        return _OK, "token válido (sin expiración informada)"
    except Exception as exc:
        return _FAIL, str(exc)[:80]


def _check_backup(client: httpx.Client) -> tuple[str, str]:
    """Último backup en el bucket altrans-backups (fuente de verdad)."""
    url = f"{_supabase_url()}/storage/v1/object/list/altrans-backups"
    try:
        r = client.post(
            url,
            headers=_supabase_headers(),
            json={"prefix": "backups", "limit": 10, "offset": 0},
            timeout=_TIMEOUT,
        )
        r.raise_for_status()
        items = [o for o in r.json() if (o.get("name") or "").endswith(".zip")]
        if not items:
            return _FAIL, "bucket sin backups"
        newest = max(items, key=lambda o: o.get("created_at") or "")
        name = newest["name"].split("/")[-1]
        created = (newest.get("created_at") or "?")[:16].replace("T", " ")
        edad_h = (datetime.now(timezone.utc)
                  - datetime.fromisoformat(newest["created_at"].replace("Z", "+00:00"))).total_seconds() / 3600
        if edad_h > 48:
            return _WARN, f"{name} ({created}) — hace {edad_h:.0f}h"
        return _OK, f"{name} ({created})"
    except Exception as exc:
        return _FAIL, str(exc)[:80]


def _check_auto_notify_hoy(client: httpx.Client) -> tuple[str, str]:
    """Conteo de messages_sent de hoy: enviados y con error."""
    hoy_0h = datetime.now(_COLOMBIA).replace(hour=0, minute=0, second=0, microsecond=0)
    params = {"sent_at": f"gte.{_iso_utc(hoy_0h)}"}
    try:
        total = _count(client, "messages_sent", params)
        err = _count(client, "messages_sent", {**params, "status": "eq.error"})
        if err:
            return _WARN, f"{total - err} enviados, {err} errores hoy"
        return _OK, f"{total} mensajes enviados hoy, 0 errores"
    except Exception as exc:
        return _FAIL, str(exc)[:80]


def _check_sesiones(client: httpx.Client) -> tuple[str, str]:
    try:
        activas = _count(client, "chatbot_sesiones", {"estado": "eq.activa"})
        bloqueadas = _count(client, "chatbot_sesiones",
                            {"locked_until": "gt." + _iso_utc(datetime.now(timezone.utc))})
        return _OK, f"{activas} activas, {bloqueadas} bloqueadas"
    except Exception as exc:
        return _FAIL, str(exc)[:80]


def _check_jailbreaks(client: httpx.Client) -> tuple[str, str]:
    desde = _iso_utc(datetime.now(timezone.utc) - timedelta(hours=24))
    try:
        n = _count(client, "jailbreak_log", {"detectado_en": f"gte.{desde}"})
        return (_WARN if n else _OK), f"{n} en 24h"
    except Exception as exc:
        return _FAIL, str(exc)[:80]


def _check_errores_app(client: httpx.Client) -> tuple[str, str]:
    desde = _iso_utc(datetime.now(timezone.utc) - timedelta(hours=24))
    try:
        n = _count(client, "app_logs", {"level": "eq.ERROR", "ts": f"gte.{desde}"})
        return (_WARN if n else _OK), f"{n} errores en 24h"
    except Exception as exc:
        return _FAIL, str(exc)[:80]


# ── Servicios IA (API keys de la cadena LLM) ─────────────────────────────────

def _check_deepseek() -> tuple[str, str]:
    """Key primaria de DeepSeek vía balance (GET /user/balance, no gasta tokens)."""
    key = os.environ.get("DEEPSEEK_API_KEY", "").strip()
    if not key:
        return _WARN, "no configurada (la cadena salta a OpenRouter)"
    try:
        r = httpx.get(
            "https://api.deepseek.com/user/balance",
            headers={"Authorization": f"Bearer {key}"},
            timeout=_TIMEOUT,
        )
        if r.status_code != 200:
            return _FAIL, f"HTTP {r.status_code} {r.text[:60]!r}"
        data = r.json()
        if not data.get("is_available"):
            return _FAIL, "cuenta no disponible (sin saldo)"
        total = ", ".join(
            f"{bi.get('total_balance')} {bi.get('currency', '')}" for bi in data.get("balance_infos", [])
        ) or "?"
        return _OK, f"key OK, balance {total}"
    except Exception as exc:
        return _FAIL, str(exc)[:80]


def _check_openrouter() -> tuple[str, str]:
    """Key de OpenRouter: verifica /auth/key y una llamada mínima (max_tokens=1).

    /auth/key responde 200 aunque el saldo esté agotado (402 en chat completions),
    así que se comprueba una llamada real de 1 token para detectar créditos.
    """
    key = os.environ.get("OPENROUTER_API_KEY", "").strip()
    if not key:
        return _WARN, "no configurada (la cadena salta a Groq)"
    try:
        r = httpx.get(
            "https://openrouter.ai/api/v1/auth/key",
            headers={"Authorization": f"Bearer {key}"},
            timeout=_TIMEOUT,
        )
        if r.status_code != 200:
            return _FAIL, f"auth HTTP {r.status_code} {r.text[:60]!r}"
        data = r.json().get("data", {})
        usage = data.get("usage")
        limit = data.get("limit")
        resp = httpx.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={"Authorization": f"Bearer {key}"},
            json={
                "model": "deepseek/deepseek-v4-flash",
                "messages": [{"role": "user", "content": "di ok"}],
                "max_tokens": 1,
            },
            timeout=_TIMEOUT,
        )
        if resp.status_code == 402:
            return _FAIL, "créditos agotados (402, agregar saldo en OpenRouter)"
        if resp.status_code != 200:
            return _FAIL, f"chat HTTP {resp.status_code} {resp.text[:60]!r}"
        detalle = f"uso {usage:,.0f}" if isinstance(usage, (int, float)) else "key OK"
        if isinstance(limit, (int, float)) and limit > 0:
            detalle += f"/{limit:,.0f}"
        return _OK, detalle
    except Exception as exc:
        return _FAIL, str(exc)[:80]


def _check_groq() -> tuple[str, str]:
    """Key de Groq (última línea, free) vía GET /models."""
    key = os.environ.get("GROQ_API_KEY", "").strip()
    if not key:
        return _WARN, "no configurada (última línea sin respaldo)"
    try:
        r = httpx.get(
            "https://api.groq.com/openai/v1/models",
            headers={"Authorization": f"Bearer {key}"},
            timeout=_TIMEOUT,
        )
        if r.status_code != 200:
            return _FAIL, f"HTTP {r.status_code} {r.text[:60]!r}"
        return _OK, "key OK"
    except Exception as exc:
        return _FAIL, str(exc)[:80]


def _check_datos(client: httpx.Client) -> tuple[str, str]:
    """Filas + frescura de manifiestos_flat."""
    try:
        total = _count(client, "manifiestos_flat")
        r = client.get(
            f"{_supabase_url()}/rest/v1/manifiestos_flat",
            headers=_supabase_headers(),
            params={"select": "actualizado_en", "order": "actualizado_en.desc", "limit": "1"},
            timeout=_TIMEOUT,
        )
        r.raise_for_status()
        rows = r.json() or []
        if not rows or not rows[0].get("actualizado_en"):
            return _WARN, f"{total:,} filas, sin fecha de actualización"
        last = (rows[0]["actualizado_en"])[:16].replace("T", " ")
        return _OK, f"{total:,} filas, última actualización {last}"
    except Exception as exc:
        return _FAIL, str(exc)[:80]


# ── Ensamblado del reporte ───────────────────────────────────────────────────

_ICONS = {_OK: "[OK] ", _WARN: "[⚠ ] ", _FAIL: "[!!] "}


def _fmt(estado: str, label: str, detalle: str) -> str:
    return f"{_ICONS.get(estado, '')}{label}: {detalle}"


def run_morning_check() -> dict:
    """Ejecuta todos los chequeos, arma el resumen y lo envía."""
    checks: list[tuple[str, str, str]] = []

    # ── Servicios ──
    chatbot_url = os.getenv("CHATBOT_URL", "").rstrip("/")
    if chatbot_url:
        est, det = _check_service(f"{chatbot_url}/health")
        checks.append((est, "Chatbot /health", det))
    else:
        checks.append((_WARN, "Chatbot /health", "CHATBOT_URL no definido"))

    est, det = _check_service(os.getenv("NOTIFICATIONS_URL", "http://127.0.0.1:8080").rstrip("/") + "/health")
    checks.append((est, "Notifications /health", det))

    dashboard_url = os.getenv("DASHBOARD_URL", "").rstrip("/")
    if dashboard_url:
        est, det = _check_service(dashboard_url)
        checks.append((est, "Dashboard", det))
    else:
        checks.append((_WARN, "Dashboard", "DASHBOARD_URL no definido (omitido)"))

    # ── Infraestructura / datos ──
    est, det = _check_wa_token()
    checks.append((est, "WA_TOKEN", det))

    # ── Servicios IA (cadena LLM del chatbot) ──
    for nombre, fn in (("DeepSeek", _check_deepseek),
                       ("OpenRouter", _check_openrouter),
                       ("Groq", _check_groq)):
        est, det = fn()
        checks.append((est, f"IA {nombre}", det))

    try:
        with httpx.Client(timeout=_TIMEOUT) as client:
            est, det = _check_datos(client)
            checks.append((est, "Supabase datos", det))
            est, det = _check_backup(client)
            checks.append((est, "Último backup", det))
            est, det = _check_auto_notify_hoy(client)
            checks.append((est, "Auto-notify hoy", det))
            est, det = _check_sesiones(client)
            checks.append((est, "Sesiones chatbot", det))
            est, det = _check_jailbreaks(client)
            checks.append((est, "Jailbreaks 24h", det))
            est, det = _check_errores_app(client)
            checks.append((est, "Errores app 24h", det))
    except Exception as exc:
        logger.error("health_report_db_failed", extra={"error": str(exc)})
        checks.append((_FAIL, "Supabase", str(exc)[:80]))

    # ── Resumen ──
    fails = sum(1 for e, *_ in checks if e == _FAIL)
    warns = sum(1 for e, *_ in checks if e == _WARN)
    oks = len(checks) - fails - warns
    hora = datetime.now(_COLOMBIA).strftime("%d/%m/%Y %H:%M")

    lines = [f"ALTRANS — Chequeo {hora} (CO)"]
    lines.append("-" * 44)
    lines.extend(_fmt(e, label, det) for e, label, det in checks)
    lines.append("-" * 44)
    lines.append(_resumen_texto(oks, warns, fails))
    lines.append("TODO LISTO ✅" if not fails else "⚠ REVISAR ANTES DE OPERAR")
    report = "\n".join(lines)

    logger.info("health_report_done", extra={"fails": fails, "warns": warns})

    # ── Envío: WhatsApp y/o email ──
    _enviar_whatsapp(report)
    _enviar_email(report, checks, hora, oks, warns, fails)

    return {"fails": fails, "warns": warns, "report": report}


# ── Resumen gráfico ───────────────────────────────────────────────────────────

_BAR, _BAR_VACIO = "█", "░"


def _barra(n: int, total: int, ancho: int = 14) -> str:
    """Barra proporcional: n/total de bloques llenos."""
    llenos = round(n / total * ancho) if total else 0
    return _BAR * llenos + _BAR_VACIO * (ancho - llenos)


def _resumen_texto(oks: int, warns: int, fails: int) -> str:
    total = oks + warns + fails
    return (
        f"Resumen: {oks} OK · {warns} aviso(s) · {fails} falla(s)\n"
        f"  OK    {_barra(oks, total)} {oks}\n"
        f"  Aviso {_barra(warns, total)} {warns}\n"
        f"  Falla {_barra(fails, total)} {fails}"
    )


def _html_report(checks: list[tuple[str, str, str]], hora: str,
                 oks: int, warns: int, fails: int) -> str:
    """Versión HTML del reporte (para email): colores y barras."""
    import html as _html

    total = oks + warns + fails
    _COLOR = {_OK: "#16a34a", _WARN: "#d97706", _FAIL: "#dc2626"}
    rows = []
    for est, label, det in checks:
        color = _COLOR.get(est, "#111827")
        rows.append(
            f'<tr><td style="padding:3px 10px 3px 0;font-weight:bold;'
            f'color:{color};white-space:nowrap;">[{_html.escape(est)}]</td>'
            f'<td style="padding:3px 0;color:#111827;"><b>{_html.escape(label)}</b>: '
            f'{_html.escape(det)}</td></tr>'
        )

    def _fila(color: str, tag: str, n: int) -> str:
        return (f'<div style="color:{color};margin:3px 0;">'
                f'{tag} {_barra(n, total)} <b>{n}</b></div>')

    return (
        f'<html><body style="font-family:Consolas,Menlo,monospace;font-size:14px;">'
        f'<p style="margin:0 0 8px;"><b>ALTRANS — Chequeo {_html.escape(hora)} (CO)</b></p>'
        f'<hr style="border:none;border-top:1px solid #e5e7eb;">'
        f'<table style="border-collapse:collapse;">' + "".join(rows) + '</table>'
        f'<hr style="border:none;border-top:1px solid #e5e7eb;">'
        f'<p style="margin:4px 0 6px;font-weight:bold;">'
        f'Resumen: {oks} OK · {warns} aviso(s) · {fails} falla(s)</p>'
        f'{_fila(_COLOR[_OK], "OK", oks)}'
        f'{_fila(_COLOR[_WARN], "Aviso", warns)}'
        f'{_fila(_COLOR[_FAIL], "Falla", fails)}'
        f'</body></html>'
    )


def _enviar_whatsapp(report: str) -> None:
    to = os.getenv("MORNING_REPORT_TO", "").strip()
    if not to:
        return
    try:
        from whatsapp_notify import send_whatsapp
        send_whatsapp(to, report)
        logger.info("health_report_wa_sent", extra={"to": to})
    except Exception as exc:
        logger.warning("health_report_wa_failed", extra={"error": str(exc)})


def _enviar_email(report: str, checks: list, hora: str,
                  oks: int, warns: int, fails: int) -> None:
    to = os.getenv("MORNING_REPORT_EMAIL", "").strip()
    if not to:
        return
    api_key = os.getenv("BREVO_API_KEY", "")
    if not api_key:
        logger.warning("health_report_email_skipped", extra={"detail": "BREVO_API_KEY vacío"})
        return
    try:
        from_email = os.getenv("BACKUP_EMAIL_FROM", "jromoguijarro@gmail.com")
        payload = {
            "sender":   {"email": from_email, "name": "Altrans Monitoreo"},
            "to":       [{"email": to}],
            "subject":  f"Chequeo ALTRANS {datetime.now(_COLOMBIA).strftime('%d/%m %H:%M')}",
            "textContent": report,
            "htmlContent": _html_report(checks, hora, oks, warns, fails),
        }
        r = httpx.post(
            "https://api.brevo.com/v3/smtp/email",
            headers={"api-key": api_key},
            json=payload,
            timeout=_TIMEOUT,
        )
        r.raise_for_status()
        logger.info("health_report_email_sent", extra={"to": to})
    except Exception as exc:
        logger.warning("health_report_email_failed", extra={"error": str(exc)})


if __name__ == "__main__":
    from logging_config import setup_logging
    setup_logging(os.getenv("LOG_LEVEL", "INFO"))
    result = run_morning_check()
    print("\n" + result["report"])
