import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))


def get_settings() -> dict:
    # OPENROUTER_API_KEY → agente principal (DeepSeek + failover a Haiku 4.5)
    # GROQ_API_KEY       → solo moderación (clasificador rápido y barato)
    required = ["OPENROUTER_API_KEY", "GROQ_API_KEY", "SUPABASE_URL", "SUPABASE_SERVICE_KEY", "JWT_SECRET"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Variables de entorno faltantes: {', '.join(missing)}")

    return {
        # OPENROUTER_API_KEY y GROQ_API_KEY las leen sus clientes directo del entorno
        "supabase_url":         os.environ["SUPABASE_URL"],
        "supabase_service_key": os.environ["SUPABASE_SERVICE_KEY"],
    }


def get_wa_settings() -> dict:
    required = ["WA_TOKEN", "WA_PHONE_NUMBER_ID", "WA_VERIFY_TOKEN"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Variables de entorno WA faltantes: {', '.join(missing)}")

    return {
        "wa_token":           os.environ["WA_TOKEN"],
        "wa_phone_number_id": os.environ["WA_PHONE_NUMBER_ID"],
        "wa_verify_token":    os.environ["WA_VERIFY_TOKEN"],
        # Opcional: para validar firma HMAC de Meta en producción
        "wa_app_secret":      os.getenv("WA_APP_SECRET", ""),
    }
