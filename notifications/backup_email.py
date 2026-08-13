"""
Backup de todas las tablas -> ZIP -> Email via Brevo.

Prefiere el envío por API HTTP de Brevo (BREVO_API_KEY) porque Railway bloquea
el SMTP saliente en planes no-Pro. Si no hay API key, cae a SMTP (BREVO_SMTP_LOGIN
y BREVO_SMTP_PASSWORD) para entornos locales.

Accede a Supabase via REST (httpx) - sin dependencias pesadas (psycopg2/pandas).
Requiere: BACKUP_EMAIL_FROM, BACKUP_EMAIL_TO, SUPABASE_URL, SUPABASE_SERVICE_KEY
          y BREVO_API_KEY (o credenciales SMTP).
"""
import base64
import csv
import io
import logging
import os
import smtplib
import time
import zipfile
from datetime import datetime
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import httpx
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

logger = logging.getLogger(__name__)

_TABLES = [
    "manifiestos_flat",
    "audit_log",
    "chatbot_sesiones",
    "processed_messages",
    "jailbreak_log",
    "messages_sent",
    "system_prompts",
    "app_logs",
    "admin_usuarios",
]

_TABLE_LABELS = {
    "manifiestos_flat":   "Manifiestos (viajes, pagos, conductores)",
    "audit_log":          "Historial de cambios (auditoria)",
    "chatbot_sesiones":   "Sesiones del chatbot WhatsApp",
    "processed_messages": "Mensajes de WhatsApp procesados",
    "jailbreak_log":      "Intentos de ataque bloqueados",
    "messages_sent":      "Notificaciones enviadas (messages_sent)",
    "system_prompts":     "Prompts del chatbot (system_prompts)",
    "app_logs":           "Logs de la aplicacion (app_logs)",
    "admin_usuarios":     "Usuarios admin (admin_usuarios)",
}

_PAGE_SIZE = 1000
_MAX_RETRIES = 3
_RETRY_DELAY = 30


def _supabase_headers() -> dict:
    key = os.environ["SUPABASE_SERVICE_KEY"]
    return {
        "apikey":        key,
        "Authorization": f"Bearer {key}",
    }


def _fetch_table(client: httpx.Client, table: str) -> list[dict]:
    url = f"{os.environ['SUPABASE_URL']}/rest/v1/{table}"
    rows: list[dict] = []
    start = 0
    while True:
        r = client.get(
            url,
            headers={**_supabase_headers(), "Range-Unit": "items",
                     "Range": f"{start}-{start + _PAGE_SIZE - 1}"},
            params={"select": "*"},
        )
        r.raise_for_status()
        chunk = r.json()
        rows.extend(chunk)
        if len(chunk) < _PAGE_SIZE:
            return rows
        start += _PAGE_SIZE


def _rows_to_csv(rows: list[dict]) -> bytes:
    if not rows:
        return "(tabla vacia)\n".encode("utf-8-sig")
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8-sig")


def _count_live(client: httpx.Client, table: str) -> int:
    url = f"{os.environ['SUPABASE_URL']}/rest/v1/{table}"
    headers = {
        **_supabase_headers(),
        "Prefer":  "count=exact",
        "Range":   "0-0",
    }
    r = client.head(url, headers=headers)
    r.raise_for_status()
    content_range = r.headers.get("content-range", "")
    total_str = content_range.split("/")[-1]
    return int(total_str) if total_str.isdigit() else -1


def verify_consistency(backup_counts: dict[str, int]) -> dict[str, dict]:
    results: dict[str, dict] = {}
    with httpx.Client(timeout=30) as client:
        for table, backup_count in backup_counts.items():
            if backup_count < 0:
                results[table] = {"backup": backup_count, "live": -1, "ok": False}
                continue
            try:
                live_count = _count_live(client, table)
                results[table] = {
                    "backup": backup_count,
                    "live":   live_count,
                    "ok":     backup_count == live_count,
                }
            except Exception as exc:
                logger.warning("consistency_check_failed", extra={"table": table, "error": str(exc)})
                results[table] = {"backup": backup_count, "live": -1, "ok": False}
    return results


def _build_zip() -> tuple[bytes, dict[str, int]]:
    counts: dict[str, int] = {}
    buf = io.BytesIO()
    with httpx.Client(timeout=120) as client, \
         zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for table in _TABLES:
            try:
                rows = _fetch_table(client, table)
            except httpx.HTTPStatusError as e:
                logger.warning("backup_table_failed",
                               extra={"table": table, "status": e.response.status_code})
                counts[table] = -1
                continue
            counts[table] = len(rows)
            label = _TABLE_LABELS.get(table, table)
            fname = f"{label.replace('/', '-')}.csv"
            zf.writestr(fname, _rows_to_csv(rows))
    return buf.getvalue(), counts


_BREVO_API = "https://api.brevo.com/v3/smtp/email"
_BREVO_ATTACHMENT_LIMIT = 8 * 1024 * 1024


def _email_content(zip_bytes: bytes, counts: dict[str, int], recipients: list[str],
                   consistency: dict[str, dict] | None) -> tuple[str, str, str]:
    """Retorna (subject, body_text, fname) comunes a SMTP y a la API de Brevo."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [f"  . {_TABLE_LABELS.get(t, t)}: {n:,} filas" if n >= 0
             else f"  . {_TABLE_LABELS.get(t, t)}: ERROR (revisar logs)"
             for t, n in counts.items()]

    consistency_block = ""
    if consistency:
        all_ok = all(v["ok"] for v in consistency.values())
        status_icon = "OK" if all_ok else "MISMATCH"
        check_lines = []
        for t, v in consistency.items():
            label = _TABLE_LABELS.get(t, t)
            if v["ok"]:
                check_lines.append(f"  OK {label}: {v['backup']:,} filas (coincide con DB)")
            else:
                check_lines.append(
                    f"  ISSUE {label}: backup={v['backup']:,} | DB en vivo={v['live']:,} - REVISAR"
                )
        consistency_block = (
            f"\nVerificacion de consistencia {status_icon}:\n" + "\n".join(check_lines) + "\n"
        )

    body = (
        "Backup Altrans — Copia de seguridad\n"
        f"Generado: {ts}\n\n"
        "Datos incluidos:\n" + "\n".join(lines) +
        f"\n\nTamano del ZIP: {len(zip_bytes) // 1024} KB"
        + consistency_block
    )
    fname = f"altrans_backup_{datetime.now().strftime('%Y%m%d_%H%M')}.zip"
    subject = f"Backup Altrans: {ts}"
    return subject, body, fname


def _send_email_smtp(
    zip_bytes: bytes,
    counts: dict[str, int],
    recipients: list[str],
    consistency: dict[str, dict] | None = None,
) -> None:
    smtp_login = os.environ["BREVO_SMTP_LOGIN"]
    smtp_password = os.environ["BREVO_SMTP_PASSWORD"]
    from_email = os.environ.get("BACKUP_EMAIL_FROM", "jromoguijarro@gmail.com")

    subject, body, fname = _email_content(zip_bytes, counts, recipients, consistency)

    msg = MIMEMultipart()
    msg["From"] = from_email
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    part = MIMEApplication(zip_bytes, _subtype="zip")
    part.add_header("Content-Disposition", f'attachment; filename="{fname}"')
    msg.attach(part)

    with smtplib.SMTP("smtp-relay.brevo.com", 587) as server:
        server.starttls()
        server.login(smtp_login, smtp_password)
        server.send_message(msg)

    logger.info("email_sent", extra={
        "recipients": recipients, "zip_kb": len(zip_bytes) // 1024, "via": "smtp",
    })


def _send_email_api(
    zip_bytes: bytes,
    counts: dict[str, int],
    recipients: list[str],
    consistency: dict[str, dict] | None = None,
) -> None:
    api_key = os.environ.get("BREVO_API_KEY", "")
    if not api_key:
        raise RuntimeError("BREVO_API_KEY no configurado (requerido para envío por API HTTP)")
    if len(zip_bytes) > _BREVO_ATTACHMENT_LIMIT:
        raise RuntimeError(
            f"ZIP demasiado grande para la API de Brevo: {len(zip_bytes) // 1024 // 1024} MB "
            f"(límite 8 MB)"
        )
    from_email = os.environ.get("BACKUP_EMAIL_FROM", "jromoguijarro@gmail.com")

    subject, body, fname = _email_content(zip_bytes, counts, recipients, consistency)

    payload = {
        "sender":   {"email": from_email, "name": "Altrans Backups"},
        "to":       [{"email": r} for r in recipients],
        "subject":  subject,
        "textContent": body,
        "attachment": [{"name": fname, "content": base64.b64encode(zip_bytes).decode("ascii")}],
    }
    r = httpx.post(_BREVO_API, headers={"api-key": api_key}, json=payload, timeout=180)
    r.raise_for_status()
    logger.info("email_sent", extra={
        "recipients": recipients, "zip_kb": len(zip_bytes) // 1024, "via": "brevo-api",
    })


def _send_email(
    zip_bytes: bytes,
    counts: dict[str, int],
    recipients: list[str],
    consistency: dict[str, dict] | None = None,
) -> None:
    """Envía el ZIP por API HTTP de Brevo si hay key; si no, por SMTP."""
    if os.environ.get("BREVO_API_KEY", ""):
        _send_email_api(zip_bytes, counts, recipients, consistency)
    else:
        _send_email_smtp(zip_bytes, counts, recipients, consistency)


def run_backup_and_email(recipients: list[str] | None = None) -> dict:
    """Ejecuta el backup completo, verifica consistencia y envia email."""
    if recipients is None:
        env_to = os.environ.get("BACKUP_EMAIL_TO", "")
        recipients = [r.strip() for r in env_to.split(",") if r.strip()]
    if not recipients:
        raise RuntimeError("No hay destinatarios (definir BACKUP_EMAIL_TO)")

    logger.info("backup_started", extra={"recipients": recipients})

    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            zip_bytes, counts = _build_zip()
            break
        except Exception as exc:
            logger.warning("backup_build_failed", extra={"attempt": attempt, "error": str(exc)})
            if attempt < _MAX_RETRIES:
                time.sleep(_RETRY_DELAY)
            else:
                raise

    consistency = verify_consistency(counts)
    all_ok = all(v["ok"] for v in consistency.values())
    if not all_ok:
        logger.warning("backup_consistency_mismatch", extra={"consistency": consistency})

    _send_email(zip_bytes, counts, recipients, consistency)

    logger.info("backup_complete", extra={
        "counts": counts, "zip_kb": len(zip_bytes) // 1024,
        "consistent": all_ok,
    })
    return {"counts": counts, "consistency": consistency, "consistent": all_ok}


if __name__ == "__main__":
    import sys
    from logging_config import setup_logging
    setup_logging(os.getenv("LOG_LEVEL", "INFO"))
    result = run_backup_and_email()
    print("Backup completado:", result)
    sys.exit(0)
