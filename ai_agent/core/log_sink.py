import json
import traceback

import httpx

_SKIP_EXTRA = frozenset({"name", "file", "function", "line", "module",
                         "process", "thread"})


def make_supabase_sink(supabase_url: str, service_key: str):
    headers = {
        "apikey":        service_key,
        "Authorization": f"Bearer {service_key}",
        "Content-Type":  "application/json",
        "Prefer":        "return=minimal",
    }
    client = httpx.Client(
        base_url=supabase_url,
        headers=headers,
        timeout=5.0,
    )

    def sink(message):
        record = message.record
        extra = {k: v for k, v in record["extra"].items() if k not in _SKIP_EXTRA}
        payload = {
            "ts":      record["time"].isoformat(),
            "level":   record["level"].name,
            "logger":  record["name"],
            "message": record["message"],
            "extra":   extra,
        }
        if record["exception"]:
            payload["exc"] = "".join(traceback.format_exception(
                type(record["exception"].value),
                record["exception"].value,
                record["exception"].traceback,
            ))
        try:
            client.post("/rest/v1/app_logs", json=payload)
        except Exception:
            pass

    return sink