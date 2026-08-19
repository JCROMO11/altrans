import json
import os
import sys
import traceback

from loguru import logger

_SKIP_EXTRA = frozenset({"name", "file", "function", "line", "module",
                         "process", "thread"})


def _json_format(record):
    payload = {
        "ts": record["time"].isoformat(),
        "level": record["level"].name,
        "logger": record["name"],
        "message": record["message"],
    }
    extra = {k: v for k, v in record["extra"].items() if k not in _SKIP_EXTRA}
    payload.update(extra)
    if record["exception"]:
        payload["exc"] = "".join(traceback.format_exception(
            type(record["exception"].value),
            record["exception"].value,
            record["exception"].traceback,
        ))
    # loguru 0.7.3 usa el string devuelto como template de formato → escapar
    # llaves para que el JSON salga literal (si no, KeyError en format_map).
    return json.dumps(payload, ensure_ascii=False, default=str).replace(
        "{", "{{").replace("}", "}}")


def setup_logging(level: str = "INFO") -> None:
    logger.remove()
    logger.add(sys.stdout, level=level, format=_json_format)

    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
    os.makedirs(log_dir, exist_ok=True)
    logger.add(
        os.path.join(log_dir, "app_{time:YYYY-MM-DD}.log"),
        level=level,
        format=_json_format,
        rotation="1 day",
        retention="7 days",
        compression="gz",
    )

    from config import get_settings
    from core.log_sink import make_supabase_sink
    cfg = get_settings()
    logger.add(
        make_supabase_sink(cfg["supabase_url"], cfg["supabase_service_key"]),
        level=level,
    )

    logger.disable("httpx")
    logger.disable("httpcore")