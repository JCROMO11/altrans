import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))


def get_settings() -> dict:
    required = ["GROQ_API_KEY", "SUPABASE_URL", "SUPABASE_SERVICE_KEY", "JWT_SECRET"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Variables de entorno faltantes: {', '.join(missing)}")

    return {
        # GROQ_API_KEY la lee el cliente de Groq directo del entorno
        "supabase_url":         os.environ["SUPABASE_URL"],
        "supabase_service_key": os.environ["SUPABASE_SERVICE_KEY"],

        # Claude / Anthropic (producción)
        # "anthropic_api_key": os.environ["ANTHROPIC_API_KEY"],
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
