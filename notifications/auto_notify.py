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
from datetime import datetime

import httpx
from dotenv import load_dotenv

from whatsapp_notify import send_whatsapp

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

logger = logging.getLogger(__name__)

_MAX_CONSECUTIVE_ERRORS = 10


def _build_template(template_name: str, manifiesto: int, fecha_estimada: str | None = None, monto: str | None = None) -> str:
    """Construye el texto del mensaje para la plantilla indicada."""
    templates = {
        "saldo_falta_factura": (
            "Buen día, estimado transportador.\n\n"
            f"Le informo que el manifiesto {manifiesto} no se ha pagado porque "
            "no se ha legalizado mediante la factura electrónica que debe enviar "
            "el propietario, quien está obligado a hacerlo según el RUT. "
            "Por favor enviarla lo antes posible.\n\n"
            "En caso de haberla enviado, por favor reenviarla a la persona "
            "que contrató su servicio.\n\n"
            "Mensaje automático de ALTRANS. Puede contener errores."
        ),
        "saldo_falta_documentacion": (
            "Buen día, estimado transportador.\n\n"
            f"Le informo que el manifiesto {manifiesto} no se ha pagado porque "
            "no se ha cumplido formalmente con la documentación original. "
            "Por favor cumplir lo antes posible.\n\n"
            "En caso de haber enviado los documentos por una empresa de mensajería, "
            "por favor rastrear y enviar la guía a la persona que contrató su servicio.\n\n"
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
            "pagado porque no se ha cumplido el tiempo pactado para realizarlo. "
            "Nuestro acuerdo fue pagarlo dentro de los 15 días hábiles siguientes "
            "al cumplido formal del transporte.\n\n"
            f"Le pedimos amablemente una espera hasta aproximadamente el {fecha_estimada or 'N/D'}.\n\n"
            "Mensaje automático de ALTRANS. Puede contener errores."
        ),
        "pago_realizado": (
            "Buen día, estimado transportador.\n\n"
            f"Le informamos que el pago del manifiesto {manifiesto} "
            f"por un monto de ${monto or 'N/D'} ha sido registrado. "
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
    """Obtiene registros pending de pago_realizado con el valor pagado."""
    url_ms = f"{os.environ['SUPABASE_URL']}/rest/v1/messages_sent"
    url_mf = f"{os.environ['SUPABASE_URL']}/rest/v1/manifiestos_flat"

    # Traer pending de pago_realizado
    r = client.get(
        url_ms,
        headers={**headers, "Range": "0-99"},
        params={
            "select": "id,manifiesto,phone",
            "template_name": "eq.pago_realizado",
            "status": "eq.pending",
            "order": "sent_at.asc",
        },
    )
    r.raise_for_status()
    rows = r.json()
    if not rows:
        return []

    manifiestos = [str(r["manifiesto"]) for r in rows]
    r2 = client.get(
        url_mf,
        headers=headers,
        params={
            "select": "manifiesto,celular,valor_pagado",
            "manifiesto": f"in.({','.join(manifiestos)})",
        },
    )
    r2.raise_for_status()
    lookup = {m["manifiesto"]: m for m in r2.json()}

    result = []
    for row in rows:
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
        })
    return result


def run_auto_notify() -> dict:
    """Ejecuta la ronda de notificaciones automáticas.

    Consulta get_pendientes_notificacion para los 4 templates de saldo,
    más messages_sent para pago_realizado, y envía WhatsApp a conductores.
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

    all_items: list[dict] = list(pendientes) + pagados
    if not all_items:
        logger.info("auto_notify_no_pendientes")
        return {"status": "ok", "sent": 0, "total": 0}

    sent = 0
    errors = 0
    for item in all_items:
        manifiesto = item.get("manifiesto")
        phone = item.get("phone") or item.get("celular")
        template = item.get("template_name")
        fecha_est = item.get("fecha_estimada")
        monto = item.get("monto")
        ms_id = item.get("ms_id")

        if not phone or not template:
            continue

        fecha_str = None
        if fecha_est:
            try:
                d = datetime.strptime(fecha_est, "%Y-%m-%d")
                fecha_str = d.strftime("%d de %B de %Y").lower()
            except ValueError:
                fecha_str = fecha_est

        message = _build_template(template, manifiesto, fecha_str, monto)
        try:
            send_whatsapp(phone, message)
            _log_sent(manifiesto, template, phone, "sent", ms_id=ms_id)
            sent += 1
        except Exception as exc:
            logger.warning("auto_notify_send_failed",
                           extra={"manifiesto": manifiesto, "phone": phone, "error": str(exc)})
            _log_sent(manifiesto, template, phone, "error", str(exc), ms_id=ms_id)
            errors += 1
            if errors >= _MAX_CONSECUTIVE_ERRORS:
                logger.error("auto_notify_too_many_errors", extra={"errors": errors})
                break

    logger.info("auto_notify_done",
                extra={"sent": sent, "errors": errors, "total": len(all_items)})
    return {"status": "ok", "sent": sent, "errors": errors, "total": len(all_items)}
