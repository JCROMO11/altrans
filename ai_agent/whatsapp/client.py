import httpx
from config import get_wa_settings

_cfg = get_wa_settings()
_BASE = f"https://graph.facebook.com/v19.0/{_cfg['wa_phone_number_id']}/messages"
_HEADERS = {
    "Authorization": f"Bearer {_cfg['wa_token']}",
    "Content-Type":  "application/json",
}


def send_text(to: str, text: str) -> None:
    httpx.post(
        _BASE,
        headers=_HEADERS,
        json={
            "messaging_product": "whatsapp",
            "to":   to,
            "type": "text",
            "text": {"body": text},
        },
        timeout=10,
    ).raise_for_status()


def mark_as_read(message_id: str) -> None:
    httpx.post(
        _BASE,
        headers=_HEADERS,
        json={
            "messaging_product": "whatsapp",
            "status":     "read",
            "message_id": message_id,
        },
        timeout=10,
    ).raise_for_status()
