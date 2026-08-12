"""
Envío automático de notificaciones WhatsApp a conductores.

Lee de la RPC get_pendientes_notificacion los 4 templates de saldo no pagado,
más mensajes pending de pago_realizado desde messages_sent, y envía la
plantilla correspondiente. Registra envíos en messages_sent para evitar reenvíos.

Requiere: SUPABASE_URL, SUPABASE_SERVICE_KEY,
          WA_TOKEN, WA_PHONE_NUMBER_ID.
"""
import logging
import os
import re
from datetime import datetime

import httpx
from dotenv import load_dotenv

from whatsapp_notify import send_whatsapp

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

logger = logging.getLogger(__name__)

_MAX_CONSECUTIVE_ERRORS = 10
_PROGRESS_LOG_EVERY = 50

_SPANISH_MONTHS = {
    1: "enero", 2: "febrero", 3: "marzo", 4: "abril",
    5: "mayo", 6: "junio", 7: "julio", 8: "agosto",
    9: "septiembre", 10: "octubre", 11: "noviembre", 12: "diciembre",
}

# Tokens que NO son novedades reales — son clasificación de vehículo o servicio.
# Coincide con el guardrail del RPC get_pendientes_notificacion.
_NOVEDAD_NOISE = ("TIPO VEHICULO", "TIPO VEHÍCULO", "TURBO", "URBANO", "URBANOS")


def _is_novedad_noise(novedades: str | None) -> bool:
    """True si novedades solo contiene ruido (clasificación, no problema real).

    Coincide con la lógica del RPC get_pendientes_notificacion (es_novedad_real).
    """
    if not novedades or not novedades.strip():
        return True
    nov = novedades.strip()
    if len(nov) <= 3:
        return True
    if len(nov) < 60 and any(t in nov.upper() for t in _NOVEDAD_NOISE):
        return True
    return False


def _build_template(template_name: str, manifiesto: int, fecha_estimada: str | None = None, monto: str | None = None, fecha_pago: str | None = None) -> str:
    """Construye el texto del mensaje para la plantilla indicada."""
    templates = {
        "saldo_falta_factura": (
            "Buen día, estimado transportador.\n\n"
            f"Le informo que el manifiesto {manifiesto} no se ha pagado porque "
            "no se ha legalizado mediante la factura electrónica que debe enviar "
            "el propietario, quien está obligado a hacerlo según el RUT. "
            "Por favor enviarla lo antes posible a facturaelectronica@altrans.com.co.\n\n"
            "En caso de haberla enviado correctamente, por favor reenviarla "
            "a la persona que contrató su servicio.\n\n"
            "Mensaje automático de ALTRANS. Puede contener errores."
        ),
        "saldo_falta_documentacion": (
            "Buen día, estimado transportador.\n\n"
            f"Le informo que el manifiesto {manifiesto} no se ha pagado porque "
            "no se ha cumplido formalmente con la documentación original firmada "
            "que nos permite evidenciar que el transporte concluyó satisfactoriamente. "
            "Por favor regularizar esta situación según las instrucciones "
            "de quien contrató su servicio.\n\n"
            "En caso de haber enviado los documentos por una empresa de mensajería, "
            "rastree y envíe la guía a la persona que contrató su servicio.\n\n"
            "Mensaje automático de ALTRANS. Puede contener errores."
        ),
        "saldo_novedad_pendiente": (
            "Buen día, estimado transportador.\n\n"
            f"Le informo que el saldo del manifiesto {manifiesto} no se ha pagado "
            "debido a una novedad sin resolver, que puede ser averías, faltantes "
            "o situaciones similares. Por favor comunicarse con la persona que "
            "contrató su servicio o adelantar las instrucciones dadas por ella.\n\n"
            "Mensaje automático de ALTRANS. Puede contener errores."
        ),
        "saldo_plazo_vigente": (
            "Buen día, estimado transportador.\n\n"
            f"Le informo que el saldo del manifiesto {manifiesto} aún no se ha "
            "pagado porque no se ha completado el plazo pactado para realizarlo. "
            "Nuestro acuerdo fue pagarlo dentro de los 15 días hábiles siguientes "
            "al completado formal del transporte.\n\n"
            f"Le pedimos amablemente una espera hasta aproximadamente el {fecha_estimada or 'N/D'}.\n\n"
            "Mensaje automático de ALTRANS. Puede contener errores."
        ),
        "pago_realizado": (
            "Buen día, estimado transportador.\n\n"
            f"Le informamos que el saldo del manifiesto {manifiesto} ha sido "
            f"pagado exitosamente el día {fecha_pago or 'N/D'} mediante "
            "transferencia bancaria. "
            "Por favor revise sus extractos bancarios.\n\n"
            "Gracias por su servicio.\n\n"
            "Mensaje automático de ALTRANS. Puede contener errores."
        ),
    }
    return templates.get(template_name, "Mensaje no disponible.")


def _log_sent(manifiesto: int, template_name: str, phone: str, status: str, error: str | None = None, ms_id: int | None = None) -> None:
    """Registra el envío en messages_sent vía REST.

    Si ms_id está presente, actualiza el registro pending existente
    en lugar de insertar uno nuevo (usado para pago_realizado).
    """
    supabase_url = os.environ['SUPABASE_URL']
    headers = {
        "apikey":        os.environ["SUPABASE_SERVICE_KEY"],
        "Authorization": f"Bearer {os.environ['SUPABASE_SERVICE_KEY']}",
        "Content-Type":  "application/json",
        "Prefer":        "return=minimal",
    }
    try:
        with httpx.Client(timeout=10) as client:
            if ms_id:
                client.patch(
                    f"{supabase_url}/rest/v1/messages_sent?id=eq.{ms_id}",
                    headers=headers,
                    json={"status": status, "error": error},
                )
            else:
                client.post(
                    f"{supabase_url}/rest/v1/messages_sent",
                    headers=headers,
                    json={
                        "manifiesto":    manifiesto,
                        "template_name": template_name,
                        "phone":         phone,
                        "status":        status,
                        "error":         error,
                    },
                )
    except Exception as exc:
        logger.warning("log_sent_failed", extra={"manifiesto": manifiesto, "error": str(exc)})


def _fetch_pending_pago_realizado(client: httpx.Client, headers: dict) -> list[dict]:
    """Obtiene TODOS los registros pending de pago_realizado con el valor pagado.

    Pagina automáticamente para evitar truncar con límites fijos.
    """
    _PAGE_SIZE = 500
    url_ms = f"{os.environ['SUPABASE_URL']}/rest/v1/messages_sent"
    url_mf = f"{os.environ['SUPABASE_URL']}/rest/v1/manifiestos_flat"

    all_rows: list[dict] = []
    start = 0
    while True:
        r = client.get(
            url_ms,
            headers={
                **headers,
                "Range": f"{start}-{start + _PAGE_SIZE - 1}",
            },
            params={
                "select": "id,manifiesto,phone",
                "template_name": "eq.pago_realizado",
                "status": "eq.pending",
                "order": "sent_at.asc",
            },
        )
        r.raise_for_status()
        chunk = r.json()
        if not chunk:
            break
        all_rows.extend(chunk)
        if len(chunk) < _PAGE_SIZE:
            break
        start += _PAGE_SIZE

    if not all_rows:
        return []

    manifiestos = [str(r["manifiesto"]) for r in all_rows]
    lookup: dict = {}

    # Fetch manifiestos_flat in chunks of 100 (Supabase REST in() filter limit)
    _MF_CHUNK = 100
    for i in range(0, len(manifiestos), _MF_CHUNK):
        chunk = manifiestos[i:i + _MF_CHUNK]
        r2 = client.get(
            url_mf,
            headers=headers,
            params={
                "select": "manifiesto,celular,valor_pagado,fecha_pago",
                "manifiesto": f"in.({','.join(chunk)})",
            },
        )
        r2.raise_for_status()
        for m in r2.json():
            lookup[m["manifiesto"]] = m

    result = []
    for row in all_rows:
        m = row["manifiesto"]
        info = lookup.get(m)
        if not info or not info.get("valor_pagado"):
            continue
        result.append({
            "ms_id": row["id"],
            "manifiesto": m,
            "phone": row["phone"],
            "template_name": "pago_realizado",
            "monto": str(info["valor_pagado"]),
            "fecha_pago": info.get("fecha_pago"),
        })
    return result


def run_auto_notify(manifestos: list[int] | None = None) -> dict:
    """Ejecuta la ronda de notificaciones automáticas.

    Consulta get_pendientes_notificacion para los 4 templates de saldo,
    más messages_sent para pago_realizado, y envía WhatsApp a conductores.

    Args:
        manifestos: Si se pasa, limita el procesamiento solo a esos manifiestos
                    (útil para pruebas aisladas sin barrer datos reales).
    """
    wa_token = os.environ.get("WA_TOKEN", "")
    wa_phone_id = os.environ.get("WA_PHONE_NUMBER_ID", "")
    if not wa_token or not wa_phone_id:
        logger.warning("wa_not_configured", extra={"detail": "WA_TOKEN o WA_PHONE_NUMBER_ID vacíos"})
        return {"status": "skipped", "reason": "WhatsApp no configurado"}

    url_rpc = f"{os.environ['SUPABASE_URL']}/rest/v1/rpc/get_pendientes_notificacion"
    headers = {
        "apikey":        os.environ["SUPABASE_SERVICE_KEY"],
        "Authorization": f"Bearer {os.environ['SUPABASE_SERVICE_KEY']}",
        "Content-Type":  "application/json",
    }

    try:
        with httpx.Client(timeout=30) as client:
            r = client.post(url_rpc, headers=headers)
            r.raise_for_status()
            pendientes: list = r.json() or []
            pagados = _fetch_pending_pago_realizado(client, headers)
    except Exception as exc:
        logger.error("get_pendientes_failed", extra={"error": str(exc)})
        return {"status": "error", "error": str(exc)}

    if manifestos is not None:
        allowed = set(manifestos)
        pendientes = [p for p in pendientes if p.get("manifiesto") in allowed]
        pagados = [p for p in pagados if p.get("manifiesto") in allowed]

    # Procesar primero los pago_realizado (urgentes: confirmar pago al conductor)
    # y luego los saldos pendientes. Así un bloqueo por errores consecutivos en
    # los saldos no deja los pagos sin notificar.
    all_items: list[dict] = list(pagados) + list(pendientes)
    if not all_items:
        logger.info("auto_notify_no_pendientes")
        return {"status": "ok", "sent": 0, "total": 0}

    sent = 0
    errors = 0
    consecutive_errors = 0

    def _fmt(d: str | None) -> str | None:
        if not d:
            return None
        try:
            dt = datetime.strptime(d, "%Y-%m-%d")
            mes = _SPANISH_MONTHS.get(dt.month, str(dt.month))
            return f"{dt.day} de {mes} de {dt.year}"
        except ValueError:
            return d

    def _format_phone(raw: str) -> str:
        cleaned = re.sub(r"[\+\-\s\(\)]", "", raw)
        if len(cleaned) == 10 and cleaned.isdigit():
            return f"57{cleaned}"
        if len(cleaned) == 12 and cleaned.startswith("57") and cleaned.isdigit():
            return cleaned
        if len(cleaned) > 10 and cleaned.isdigit():
            return cleaned
        return raw

    total = len(all_items)
    processed = 0

    for item in all_items:
        manifiesto = item.get("manifiesto")
        raw_phone = item.get("phone") or item.get("celular") or ""
        phone = _format_phone(raw_phone)
        template = item.get("template_name")
        fecha_est = item.get("fecha_estimada")
        monto = item.get("monto")
        ms_id = item.get("ms_id")
        fecha_pago_raw = item.get("fecha_pago")

        if not phone or not template:
            processed += 1
            logger.debug("auto_notify_skipped",
                         extra={"manifiesto": manifiesto, "reason": "missing phone or template"})
            continue

        # Defense in depth: saltar saldo_novedad_pendiente si novedades es ruido
        if template == 'saldo_novedad_pendiente' and _is_novedad_noise(item.get('novedades')):
            processed += 1
            logger.debug("auto_notify_skipped",
                         extra={"manifiesto": manifiesto, "reason": "novedad noise"})
            continue

        fecha_str = _fmt(fecha_est)
        fecha_pago_str = _fmt(fecha_pago_raw)

        message = _build_template(template, manifiesto, fecha_str, monto, fecha_pago_str)
        try:
            send_whatsapp(phone, message)
            _log_sent(manifiesto, template, phone, "sent", ms_id=ms_id)
            sent += 1
            consecutive_errors = 0
        except Exception as exc:
            logger.warning("auto_notify_send_failed",
                           extra={"manifiesto": manifiesto, "phone": phone, "error": str(exc)})
            _log_sent(manifiesto, template, phone, "error", str(exc), ms_id=ms_id)
            errors += 1
            consecutive_errors += 1
            if consecutive_errors >= _MAX_CONSECUTIVE_ERRORS:
                logger.error("auto_notify_too_many_consecutive_errors",
                             extra={"consecutive_errors": consecutive_errors, "manifiesto": manifiesto})
                break

        processed += 1
        if processed % _PROGRESS_LOG_EVERY == 0:
            logger.info("auto_notify_progress",
                        extra={"processed": processed, "sent": sent, "errors": errors, "total": total})

    skipped = total - processed
    logger.info("auto_notify_done",
                extra={"sent": sent, "errors": errors, "skipped": skipped, "total": total})
    return {"status": "ok", "sent": sent, "errors": errors, "total": total}
