"""
Tests del endpoint REST /chat del chatbot.

Cubre:
  1. Auth: /login con cédula+manifiesto válidos
  2. Auth: /login con cédula inválida
  3. Auth: /login con manifiesto que no corresponde
  4. Chat: /chat con token válido (conductor)
  5. Chat: /chat con token válido (propietario)
  6. Chat: /chat sin token → 403
  7. Chat: /chat con token inválido → 401

Ejecutar: python3 -m pytest tests/test_chatbot_rest_api.py -v
"""
import os, sys
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ai_agent"))

import jwt
import pytest
from fastapi.testclient import TestClient
from main import app
from auth import create_token

client = TestClient(app)

_JWT_SECRET = os.environ.get("JWT_SECRET", "cambiar-en-produccion")


# ═══════════════════════════════════════════════════════════════════════════
# /login
# ═══════════════════════════════════════════════════════════════════════════

class TestLogin:
    def test_login_exitoso(self):
        with (
            patch('main.queries.get_conductor_by_cedula',
                  return_value={"nombre": "TEST", "cedula": "12345"}),
            patch('main.queries.verificar_manifiesto_conductor',
                  return_value=True),
        ):
            r = client.post("/login", json={"cedula": "12345", "manifiesto": 99999})
        assert r.status_code == 200
        body = r.json()
        assert body["nombre"] == "TEST"
        assert "token" in body
        # Verificar que el token contiene los datos correctos
        payload = jwt.decode(body["token"], _JWT_SECRET, algorithms=["HS256"])
        assert payload["identificador"] == "12345"
        assert payload["tipo_usuario"] == "conductor"

    def test_login_cedula_inexistente(self):
        with patch('main.queries.get_conductor_by_cedula', return_value=None):
            r = client.post("/login", json={"cedula": "99999", "manifiesto": 99999})
        assert r.status_code == 401
        assert "no encontrada" in r.json()["detail"].lower()

    def test_login_manifiesto_no_corresponde(self):
        with (
            patch('main.queries.get_conductor_by_cedula',
                  return_value={"nombre": "TEST", "cedula": "12345"}),
            patch('main.queries.verificar_manifiesto_conductor',
                  return_value=False),
        ):
            r = client.post("/login", json={"cedula": "12345", "manifiesto": 99999})
        assert r.status_code == 401
        assert "no corresponde" in r.json()["detail"].lower()


# ═══════════════════════════════════════════════════════════════════════════
# /chat
# ═══════════════════════════════════════════════════════════════════════════

class TestChat:
    def test_chat_conductor(self):
        token = create_token("TEST", "12345", "conductor")
        with patch('main.run', return_value=("respuesta de prueba", False)):
            r = client.post(
                "/chat",
                headers={"Authorization": f"Bearer {token}"},
                json={"mensaje": "cual es mi saldo?"},
            )
        assert r.status_code == 200
        assert r.json()["respuesta"] == "respuesta de prueba"

    def test_chat_propietario(self):
        token = create_token("TEST DUEÑO", "ABC123", "propietario")
        with patch('main.run', return_value=("respuesta para dueño", False)):
            r = client.post(
                "/chat",
                headers={"Authorization": f"Bearer {token}"},
                json={"mensaje": "cual es mi saldo?"},
            )
        assert r.status_code == 200
        assert r.json()["respuesta"] == "respuesta para dueño"

    def test_chat_sin_token(self):
        r = client.post("/chat", json={"mensaje": "hola"})
        # HTTPBearer lanza 403; si hay middleware que lo convierte aceptamos 401
        assert r.status_code in (401, 403)

    def test_chat_token_invalido(self):
        # Firmar con otra clave para que el decode falle
        fake_token = jwt.encode({"sub": "test"}, "otra-clave-secreta", algorithm="HS256")
        r = client.post(
            "/chat",
            headers={"Authorization": f"Bearer {fake_token}"},
            json={"mensaje": "hola"},
        )
        assert r.status_code == 401
        assert "inválido" in r.json()["detail"].lower()

    def test_chat_con_historial(self):
        token = create_token("TEST", "12345", "conductor")
        historial = [
            {"role": "user", "content": "hola"},
            {"role": "assistant", "content": "hola, ¿en qué puedo ayudarte?"},
        ]
        with patch('main.run', return_value=("respuesta con contexto", False)) as mock_run:
            r = client.post(
                "/chat",
                headers={"Authorization": f"Bearer {token}"},
                json={"mensaje": "siguiente pregunta", "historial": historial},
            )
        assert r.status_code == 200
        # Verificar que el historial se pasó al agente (2º arg posicional)
        args, _kwargs = mock_run.call_args
        assert len(args) >= 2
        assert args[1] == historial
