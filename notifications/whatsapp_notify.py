"""
Envío saliente de mensajes de WhatsApp vía Meta Cloud API.

Requiere: WA_TOKEN, WA_PHONE_NUMBER_ID.

Uso:
    from whatsapp_notify import send_whatsapp
    send_whatsapp("573001234567", "Hola, tu pago está listo.")
"""
import logging
import os

import httpx

logger = logging.getLogger(__name__)

_BASE_URL = "https://graph.facebook.com/v20.0"


def send_whatsapp(phone: str, message: str) -> None:
    """Envía un mensaje de texto a un número de WhatsApp.

    Args:
        phone: Número en formato internacional sin '+' (ej: '573001234567').
        message: Texto del mensaje.
    """
    phone_number_id = os.environ["WA_PHONE_NUMBER_ID"]
    token = os.environ["WA_TOKEN"]

    url = f"{_BASE_URL}/{phone_number_id}/messages"
    payload = {
        "messaging_product": "whatsapp",
        "to":   phone,
        "type": "text",
        "text": {"body": message},
    }
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
    }

    with httpx.Client(timeout=10.0) as client:
        r = client.post(url, json=payload, headers=headers)
        r.raise_for_status()

    logger.info("whatsapp_sent", extra={"to": phone, "chars": len(message)})


def send_whatsapp_bulk(recipients: list[str], message: str) -> dict[str, str]:
    """Envía el mismo mensaje a múltiples destinatarios.

    Returns:
        dict con phone → "ok" o mensaje de error.
    """
    results: dict[str, str] = {}
    for phone in recipients:
        try:
            send_whatsapp(phone, message)
            results[phone] = "ok"
        except Exception as e:
            logger.warning("whatsapp_send_failed", extra={"to": phone, "error": str(e)})
            results[phone] = str(e)
    return results
