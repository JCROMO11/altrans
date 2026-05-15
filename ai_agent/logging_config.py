"""Logging estructurado JSON para producción."""
import json
import logging
import sys
from datetime import datetime, timezone


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts":      datetime.now(timezone.utc).isoformat(),
            "level":   record.levelname,
            "logger":  record.name,
            "message": record.getMessage(),
        }
        # Campos extra inyectados con logger.info("...", extra={...})
        for key, value in record.__dict__.items():
            if key in ("args", "msg", "levelname", "name", "pathname", "filename",
                       "module", "exc_info", "exc_text", "stack_info", "lineno",
                       "funcName", "created", "msecs", "relativeCreated", "thread",
                       "threadName", "processName", "process", "levelno",
                       "getMessage", "message"):
                continue
            payload[key] = value
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


def setup_logging(level: str = "INFO") -> None:
    root = logging.getLogger()
    root.handlers.clear()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root.addHandler(handler)
    root.setLevel(level)
    # Silenciar httpx en INFO (genera mucho ruido)
    logging.getLogger("httpx").setLevel(logging.WARNING)
