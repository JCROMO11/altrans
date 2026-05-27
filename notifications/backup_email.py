"""
Backup semanal de todas las tablas → ZIP → email vía Resend.

Accede a Supabase vía REST (httpx) — sin dependencias pesadas (psycopg2/pandas).
Requiere: RESEND_API_KEY, BACKUP_EMAIL_FROM, BACKUP_EMAIL_TO,
          SUPABASE_URL, SUPABASE_SERVICE_KEY.
"""
import csv
import io
import logging
import os
import zipfile
from datetime import datetime

import httpx
import resend
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

logger = logging.getLogger(__name__)

_TABLES = [
    "manifiestos_flat",
    "audit_log",
    "chatbot_sesiones",
    "processed_messages",
    "jailbreak_log",
]

_PAGE_SIZE = 1000  # límite por request de Supabase REST


def _fetch_table(client: httpx.Client, table: str) -> list[dict]:
    url = f"{os.environ['SUPABASE_URL']}/rest/v1/{table}"
    headers = {
        "apikey":        os.environ["SUPABASE_SERVICE_KEY"],
        "Authorization": f"Bearer {os.environ['SUPABASE_SERVICE_KEY']}",
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
    resend.api_key = os.environ["RESEND_API_KEY"]
    from_email = os.environ.get("BACKUP_EMAIL_FROM", "backup@altrans.dev")

    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [f"  · {t}: {n:,} filas" if n >= 0 else f"  · {t}: ERROR (revisar logs)"
             for t, n in counts.items()]
    body = (
        "Backup semanal Altrans\n"
        f"Generado: {ts}\n\n"
        "Tablas incluidas:\n" + "\n".join(lines) +
        f"\n\nTamaño ZIP: {len(zip_bytes) // 1024} KB\n"
    )

    fname = f"altrans_backup_{datetime.now().strftime('%Y%m%d_%H%M')}.zip"

    resend.Emails.send({
        "from":    from_email,
        "to":      recipients,
        "subject": f"Backup Altrans — {ts}",
        "text":    body,
        "attachments": [{
            "filename": fname,
            "content":  list(zip_bytes),
        }],
    })


def run_backup_and_email(recipients: list[str] | None = None) -> dict[str, int]:
    """Ejecuta el backup completo y envía email. Retorna conteo por tabla."""
    if recipients is None:
        env_to = os.environ.get("BACKUP_EMAIL_TO", "")
        recipients = [r.strip() for r in env_to.split(",") if r.strip()]
    if not recipients:
        raise RuntimeError("No hay destinatarios (definir BACKUP_EMAIL_TO)")

    logger.info("backup_started", extra={"recipients": recipients})
    zip_bytes, counts = _build_zip()
    _send_email(zip_bytes, counts, recipients)
    logger.info("backup_sent", extra={"counts": counts, "zip_kb": len(zip_bytes) // 1024})
    return counts


if __name__ == "__main__":
    import sys
    from logging_config import setup_logging
    setup_logging(os.getenv("LOG_LEVEL", "INFO"))
    counts = run_backup_and_email()
    print("Backup enviado:", counts)
    sys.exit(0)
