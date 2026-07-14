"""
Backup de todas las tablas → ZIP → SendGrid.

Accede a Supabase vía REST (httpx) — sin dependencias pesadas (psycopg2/pandas).
Requiere: SENDGRID_API_KEY, BACKUP_EMAIL_FROM, BACKUP_EMAIL_TO,
          SUPABASE_URL, SUPABASE_SERVICE_KEY.
"""
import base64
import csv
import io
import logging
import os
import time
import zipfile
from datetime import datetime

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
]

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
        return "(tabla vacía)\n".encode("utf-8-sig")
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
            zf.writestr(f"{table}.csv", _rows_to_csv(rows))
    return buf.getvalue(), counts


def _send_email_sendgrid(
    zip_bytes: bytes,
    counts: dict[str, int],
    recipients: list[str],
    consistency: dict[str, dict] | None = None,
) -> None:
    # Import here so the module loads even if sendgrid is not installed
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import (
        Attachment, Email, Mail, To, Content, MimeType, FileContent, FileName, FileType, Disposition,
    )

    api_key = os.environ["SENDGRID_API_KEY"]
    from_email = os.environ.get("BACKUP_EMAIL_FROM", "backup@altrans.dev")

    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [f"  · {t}: {n:,} filas" if n >= 0 else f"  · {t}: ERROR (revisar logs)"
             for t, n in counts.items()]

    consistency_block = ""
    if consistency:
        all_ok = all(v["ok"] for v in consistency.values())
        status_icon = "✅" if all_ok else "⚠️"
        check_lines = []
        for t, v in consistency.items():
            if v["ok"]:
                check_lines.append(f"  ✅ {t}: {v['backup']:,} filas (coincide con DB)")
            else:
                check_lines.append(
                    f"  ⚠️  {t}: backup={v['backup']:,} | DB en vivo={v['live']:,} — REVISAR"
                )
        consistency_block = (
            f"\nVerificación de consistencia {status_icon}:\n" + "\n".join(check_lines) + "\n"
        )

    body = (
        "Backup Altrans\n"
        f"Generado: {ts}\n\n"
        "Tablas incluidas:\n" + "\n".join(lines) +
        f"\n\nTamaño ZIP: {len(zip_bytes) // 1024} KB"
        + consistency_block
    )

    fname = f"altrans_backup_{datetime.now().strftime('%Y%m%d_%H%M')}.zip"

    message = Mail(
        from_email=Email(from_email),
        to_emails=[To(email) for email in recipients],
        subject=f"Backup Altrans — {ts}",
    )
    message.add_content(Content(MimeType.text, body))

    encoded = base64.b64encode(zip_bytes).decode()
    attachment = Attachment(
        FileContent(encoded),
        FileName(fname),
        FileType("application/zip"),
        Disposition("attachment"),
    )
    message.add_attachment(attachment)

    sg = SendGridAPIClient(api_key)
    response = sg.send(message)
    logger.info("email_sent", extra={
        "status_code": response.status_code,
        "recipients": recipients,
        "zip_kb": len(zip_bytes) // 1024,
    })


def run_backup_and_email(recipients: list[str] | None = None) -> dict:
    """Ejecuta el backup completo, verifica consistencia y envía email."""
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

    _send_email_sendgrid(zip_bytes, counts, recipients, consistency)

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
