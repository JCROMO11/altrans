"""
Backup semanal de todas las tablas → ZIP → email.

Pareja en producción de etl_individual/backup_db.py: misma lista de tablas y
mismo formato CSV, pero accede a la DB vía Supabase REST (httpx) en lugar de
psycopg2, para no agregar dependencias pesadas (pandas, sqlalchemy) a la
imagen de Railway.

Trigger: endpoint protegido en main.py (ver POST /admin/backup).
"""
import csv
import io
import logging
import os
import smtplib
import zipfile
from datetime import datetime
from email.message import EmailMessage

import httpx

from config import get_settings

logger = logging.getLogger(__name__)

# Mismas tablas que etl_individual/backup_db.py:TABLES
_TABLES = [
    "manifiestos_flat",
    "audit_log",
    "chatbot_sesiones",
    "processed_messages",
    "jailbreak_log",
]

_PAGE_SIZE = 1000  # límite por request de Supabase REST


def _fetch_table(client: httpx.Client, table: str) -> list[dict]:
    """Trae todas las filas de la tabla via Supabase REST con paginación por Range header."""
    cfg = get_settings()
    url = f"{cfg['supabase_url']}/rest/v1/{table}"
    headers = {
        "apikey":        cfg["supabase_service_key"],
        "Authorization": f"Bearer {cfg['supabase_service_key']}",
    }

    rows: list[dict] = []
    start = 0
    while True:
        r = client.get(
            url,
            headers={**headers, "Range-Unit": "items",
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
        return "(tabla vacía)\n".encode("utf-8-sig")
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8-sig")


def _build_zip() -> tuple[bytes, dict[str, int]]:
    counts: dict[str, int] = {}
    buf = io.BytesIO()
    with httpx.Client(timeout=60) as client, \
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
            zf.writestr(f"{table}.csv", _rows_to_csv(rows))
    return buf.getvalue(), counts


def _send_email(zip_bytes: bytes, counts: dict[str, int], recipients: list[str]) -> None:
    smtp_user = os.environ["SMTP_USER"]
    smtp_pass = os.environ["SMTP_APP_PASSWORD"]

    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [f"  · {t}: {n:,} filas" if n >= 0 else f"  · {t}: ERROR (revisar logs)"
             for t, n in counts.items()]
    body = (
        "Backup semanal Altrans\n"
        f"Generado: {ts}\n\n"
        "Tablas incluidas:\n" + "\n".join(lines) +
        f"\n\nTamaño ZIP: {len(zip_bytes) // 1024} KB\n"
    )

    msg = EmailMessage()
    msg["Subject"] = f"Backup Altrans — {ts}"
    msg["From"]    = smtp_user
    msg["To"]      = ", ".join(recipients)
    msg.set_content(body)

    fname = f"altrans_backup_{datetime.now().strftime('%Y%m%d_%H%M')}.zip"
    msg.add_attachment(zip_bytes, maintype="application", subtype="zip", filename=fname)

    with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as s:
        s.login(smtp_user, smtp_pass)
        s.send_message(msg)


def run_backup_and_email(recipients: list[str] | None = None) -> dict[str, int]:
    """Ejecuta el backup completo y envía email. Retorna conteo por tabla."""
    if recipients is None:
        env_to = os.environ.get("BACKUP_EMAIL_TO", "")
        recipients = [r.strip() for r in env_to.split(",") if r.strip()]
    if not recipients:
        raise RuntimeError("No hay destinatarios (definir BACKUP_EMAIL_TO o pasar recipients)")

    logger.info("backup_started", extra={"recipients": recipients})
    zip_bytes, counts = _build_zip()
    _send_email(zip_bytes, counts, recipients)
    logger.info("backup_sent", extra={"counts": counts, "zip_kb": len(zip_bytes) // 1024})
    return counts
