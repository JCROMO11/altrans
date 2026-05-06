import os
import jwt
from datetime import datetime, timedelta, timezone
from fastapi import HTTPException, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

_bearer = HTTPBearer()
_SECRET = os.environ.get("JWT_SECRET", "cambiar-en-produccion")
_ALGO   = "HS256"
_TTL_H  = 24


def create_token(nombre: str, cedula: str) -> str:
    payload = {
        "nombre": nombre,
        "cedula": cedula,
        "exp":    datetime.now(timezone.utc) + timedelta(hours=_TTL_H),
    }
    return jwt.encode(payload, _SECRET, algorithm=_ALGO)


def decode_token(token: str) -> dict:
    try:
        return jwt.decode(token, _SECRET, algorithms=[_ALGO])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expirado")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Token inválido")


def get_current_conductor(
    credentials: HTTPAuthorizationCredentials = Security(_bearer),
) -> dict:
    """FastAPI dependency — inyecta nombre y cedula del conductor autenticado."""
    return decode_token(credentials.credentials)
