import os
import jwt
from datetime import datetime, timedelta, timezone
from fastapi import HTTPException, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

_bearer = HTTPBearer()
_SECRET = os.environ.get("JWT_SECRET")
if not _SECRET:
    raise RuntimeError("JWT_SECRET no está definido en el entorno")
_ALGO   = "HS256"
_TTL_H  = 24


def create_token(nombre: str, identificador: str, tipo_usuario: str = "conductor") -> str:
    """`identificador` es la cédula (conductor) o la placa (propietario)."""
    payload = {
        "nombre":        nombre,
        "identificador": identificador,
        "tipo_usuario":  tipo_usuario,
        # Campo legacy para clientes que aún leen `cedula` directamente.
        "cedula":        identificador if tipo_usuario == "conductor" else None,
        "exp":           datetime.now(timezone.utc) + timedelta(hours=_TTL_H),
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
    """FastAPI dependency — inyecta nombre, identificador y tipo_usuario del usuario autenticado.
    Mantiene `cedula` en el dict por compatibilidad con clientes existentes."""
    payload = decode_token(credentials.credentials)
    # Si el token es viejo (solo tenía `cedula`), normalizar al formato nuevo.
    if "identificador" not in payload and "cedula" in payload:
        payload["identificador"] = payload["cedula"]
        payload["tipo_usuario"]  = "conductor"
    return payload
