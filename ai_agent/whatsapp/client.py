import httpx
from config import get_wa_settings
from loguru import logger

_cfg = get_wa_settings()
_PHONE_NUMBER_ID = _cfg["wa_phone_number_id"]
_HEADERS = {
    "Authorization": f"Bearer {_cfg['wa_token']}",
    "Content-Type":  "application/json",
}

_CLIENT = httpx.AsyncClient(
    base_url=f"https://graph.facebook.com/v20.0/{_PHONE_NUMBER_ID}",
    headers=_HEADERS,
    timeout=10.0,
    limits=httpx.Limits(
        max_connections=10,
        max_keepalive_connections=10,
        keepalive_expiry=30.0,
    ),
)


async def send_text(to: str, text: str) -> None:
    response = await _CLIENT.post(
        "/messages",
        json={
            "messaging_product": "whatsapp",
            "to":   to,
            "type": "text",
            "text": {"body": text},
        },
    )
    response.raise_for_status()


async def mark_as_read(message_id: str) -> None:
    try:
        response = await _CLIENT.post(
            "/messages",
            json={
                "messaging_product": "whatsapp",
                "status":     "read",
                "message_id": message_id,
            },
        )
        response.raise_for_status()
    except Exception as e:
        logger.warning("mark_as_read_failed", error=str(e))