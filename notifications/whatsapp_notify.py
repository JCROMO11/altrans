"""
Envío saliente de mensajes de WhatsApp vía Meta Cloud API.

Requiere: WA_TOKEN, WA_PHONE_NUMBER_ID.

Uso:
    from whatsapp_notify import send_whatsapp
    send_whatsapp("573001234567", "Hola, tu pago está listo.")
"""
import logging
import os
import time

import httpx

logger = logging.getLogger(__name__)

_BASE_URL = "https://graph.facebook.com/v20.0"
_MAX_RETRIES = 3
_RETRY_BACKOFF = 2.0  # segundos base, exponencial: 2, 4, 8
_RETRYABLE_STATUSES = {429, 500, 502, 503, 504}


class WhatsAppAPIError(Exception):
    """Error devuelto por Meta Cloud API, con código y mensaje reales."""

    def __init__(self, code: str, message: str, http_status: int | None = None) -> None:
        self.code = code
        self.message = message
        self.http_status = http_status
        super().__init__(f"Meta error {code}: {message}")


def _extract_api_error(r: httpx.Response) -> WhatsAppAPIError | None:
    """Extrae el error de Meta del body de la respuesta, si está presente.

    Meta puede responder con HTTP 200 y un bloque `{"error": {...}}`, o con
    4xx/5xx y el mismo bloque. Devuelve None si no hay error parseable.
    """
    try:
        body = r.json()
    except Exception:
        return None
    if not isinstance(body, dict):
        return None
    err = body.get("error")
    if not isinstance(err, dict):
        return None
    code = err.get("code") or err.get("type") or "unknown"
    message = err.get("message")
    if not message:
        ed = err.get("error_data")
        if isinstance(ed, dict):
            message = ed.get("details")
    message = message or "sin detalle"
    return WhatsAppAPIError(str(code), str(message), r.status_code)


def _is_retryable(exc: Exception) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in _RETRYABLE_STATUSES
    if isinstance(exc, (httpx.TimeoutException, httpx.ConnectError, httpx.RemoteProtocolError)):
        return True
    return False


def send_whatsapp(phone: str, message: str) -> None:
    """Envía un mensaje de texto a un número de WhatsApp con reintentos.

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
    _post_message(url, payload, token, phone, len(message))


def send_whatsapp_template(phone: str, template_name: str, params: list[str],
                           language: str = "es") -> None:
    """Envía un mensaje de plantilla aprobado de WhatsApp con reintentos.

    Los mensajes de plantilla funcionan fuera de la ventana de 24 horas de
    servicio al cliente, por lo que son los que debe usar el sistema de
    notificaciones automáticas.

    Args:
        phone: Número en formato internacional sin '+' (ej: '573001234567').
        template_name: Nombre de la plantilla en la WABA (ej: 'altrans_pago_realizado').
        params: Valores de los parámetros posicionales {{1}}, {{2}}, ... en orden.
        language: Código de idioma de la plantilla (default 'es').
    """
    phone_number_id = os.environ["WA_PHONE_NUMBER_ID"]
    token = os.environ["WA_TOKEN"]

    components = None
    if params:
        components = [{
            "type": "body",
            "parameters": [{"type": "text", "text": str(p)} for p in params],
        }]

    url = f"{_BASE_URL}/{phone_number_id}/messages"
    payload = {
        "messaging_product": "whatsapp",
        "to":   phone,
        "type": "template",
        "template": {
            "name": template_name,
            "language": {"code": language},
        },
    }
    if components:
        payload["template"]["components"] = components

    _post_message(url, payload, token, phone, sum(len(str(p)) for p in params) + len(template_name))


def _post_message(url: str, payload: dict, token: str, phone: str, chars: int) -> None:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
    }

    last_exc = None
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            with httpx.Client(timeout=10.0) as client:
                r = client.post(url, json=payload, headers=headers)
                api_err = _extract_api_error(r)
                if api_err is not None:
                    if r.status_code in _RETRYABLE_STATUSES:
                        r.raise_for_status()
                    raise api_err
                r.raise_for_status()
            logger.info("whatsapp_sent", extra={"to": phone, "chars": chars, "attempt": attempt})
            return
        except Exception as exc:
            last_exc = exc
            if attempt < _MAX_RETRIES and _is_retryable(exc):
                delay = _RETRY_BACKOFF ** attempt
                logger.warning("whatsapp_retry",
                               extra={"to": phone, "attempt": attempt, "delay": delay, "error": str(exc)})
                time.sleep(delay)
            else:
                break

    raise last_exc


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
