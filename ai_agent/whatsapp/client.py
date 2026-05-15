import logging
import httpx
from config import get_wa_settings

logger = logging.getLogger(__name__)

# Cliente Meta reutilizado: mismo patrón que en db/queries.py.
# Meta WhatsApp Business API rate-limits a ~80 msg/seg por número (Tier 1),
# así que 10 conductores simultáneos no son problema. El cuello de botella
# con httpx síncrono era el handshake TLS — ahora se reutiliza la conexión.
_cfg = get_wa_settings()
_PHONE_NUMBER_ID = _cfg["wa_phone_number_id"]
_HEADERS = {
    "Authorization": f"Bearer {_cfg['wa_token']}",
    "Content-Type":  "application/json",
}

_CLIENT = httpx.Client(
    base_url=f"https://graph.facebook.com/v19.0/{_PHONE_NUMBER_ID}",
    headers=_HEADERS,
    timeout=10.0,
    limits=httpx.Limits(
        max_connections=10,
        max_keepalive_connections=10,
        keepalive_expiry=30.0,
    ),
)


def send_text(to: str, text: str) -> None:
    _CLIENT.post(
        "/messages",
        json={
            "messaging_product": "whatsapp",
            "to":   to,
            "type": "text",
            "text": {"body": text},
        },
    ).raise_for_status()


def mark_as_read(message_id: str) -> None:
    try:
        _CLIENT.post(
            "/messages",
            json={
                "messaging_product": "whatsapp",
                "status":     "read",
                "message_id": message_id,
            },
        ).raise_for_status()
    except Exception as e:
        logger.warning("mark_as_read falló (no crítico): %s", e)
