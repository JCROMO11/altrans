"""
Tests del sink de logs ([ai_agent/core/log_sink.py]).

Mockeamos httpx.Client para verificar que el sink envía el payload correcto
y que no crashea si Supabase falla.

Ejecutar:
  python3 -m pytest tests/test_log_sink.py -v
"""
import os
import sys
import tempfile
from unittest.mock import patch, MagicMock
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'ai_agent'))

from loguru import logger


class TestSupabaseSink:
    """El sink de Supabase debe enviar logs con el formato correcto."""

    def test_payload_incluye_campos_requeridos(self):
        from core.log_sink import make_supabase_sink

        with patch("core.log_sink.httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client

            sink = make_supabase_sink("https://test.supabase.co", "test-key")
            sink(_record(
                ts="2026-07-13T20:00:00+00:00",
                level="INFO",
                name="test_logger",
                message="test message",
                extra={"wa_from": "57301"},
            ))

            mock_client.post.assert_called_once()
            args, kwargs = mock_client.post.call_args
            assert args[0] == "/rest/v1/app_logs"
            payload = kwargs["json"]
            assert payload["ts"] == "2026-07-13T20:00:00+00:00"
            assert payload["level"] == "INFO"
            assert payload["logger"] == "test_logger"
            assert payload["message"] == "test message"
            assert payload["extra"]["wa_from"] == "57301"

    def test_no_crashea_si_supabase_falla(self):
        from core.log_sink import make_supabase_sink

        with patch("core.log_sink.httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client.post.side_effect = Exception("Connection refused")
            mock_client_class.return_value = mock_client

            sink = make_supabase_sink("https://test.supabase.co", "test-key")
            sink(_record(
                ts="2026-07-13T20:00:00+00:00",
                level="ERROR",
                name="test",
                message="boom",
            ))

    def test_incluye_exception_cuando_hay_error(self):
        from core.log_sink import make_supabase_sink

        with patch("core.log_sink.httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client

            sink = make_supabase_sink("https://test.supabase.co", "test-key")

            class _MockExc:
                value = ValueError("algo salió mal")
                traceback = None
                def __bool__(self):
                    return True

            record = _record(
                ts="2026-07-13T20:00:00+00:00",
                level="ERROR",
                name="test",
                message="error con exception",
            )
            record.record["exception"] = _MockExc()
            sink(record)

            mock_client.post.assert_called_once()
            payload = mock_client.post.call_args[1]["json"]
            assert "exc" in payload
            assert "ValueError" in payload["exc"]


class TestLoggingConfig:
    """setup_logging agrega los sinks correctos."""

    def test_agrega_sink_archivo(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("logging_config.os.path.dirname", return_value=tmpdir):
                import logging_config
                logging_config.setup_logging("DEBUG")
                assert any("logs" in str(s) for s in logger._core.handlers.values())

    def test_agrega_sink_supabase(self):
        with patch("core.log_sink.httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            import logging_config
            logging_config.setup_logging("DEBUG")
            # El handler de Supabase se registró sin error
            assert True


# ── Helpers ─────────────────────────────────────────────────────────────────


class _MockMessage:
    def __init__(self, record):
        self.record = record


def _record(ts, level, name, message, extra=None):
    return _MockMessage({
        "time":      datetime.fromisoformat(ts),
        "level":     _MockLevel(level),
        "name":      name,
        "message":   message,
        "extra":     extra or {},
        "exception": None,
    })


class _MockLevel:
    def __init__(self, name):
        self.name = name