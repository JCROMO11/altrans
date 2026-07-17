"""Config común para tests: setear defaults de env vars faltantes."""
import os

# WhatsApp — requerido por ai_agent/config.py.get_wa_settings()
os.environ.setdefault("WA_TOKEN", "test-token")
os.environ.setdefault("WA_PHONE_NUMBER_ID", "12345")
os.environ.setdefault("WA_VERIFY_TOKEN", "test-verify")
