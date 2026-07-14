import json
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
    return json.dumps(payload, ensure_ascii=False, default=str)


def setup_logging(level: str = "INFO") -> None:
    logger.remove()
    logger.add(sys.stdout, level=level, format=_json_format)
    logger.disable("httpx")
    logger.disable("httpcore")