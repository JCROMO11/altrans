"""
Tests de API para endpoints del servicio de notificaciones.

Cubre autenticación (x-admin-token) y validación de parámetros.
Usa FastAPI TestClient para probar in-process.

Ejecutar: python3 -m pytest tests/test_notificaciones_api.py -v
"""
import os, sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "notifications"))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)

from fastapi.testclient import TestClient
import pytest
from main import app

client = TestClient(app)
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "")


class TestHealth:
    def test_health_returns_ok_or_degraded(self):
        """/health debe responder sin importar disponibilidad de Supabase."""
        r = client.get("/health")
        assert r.status_code in (200, 503)
        body = r.json()
        assert "status" in body

    def test_health_via_root(self):
        r = client.get("/")
        assert r.status_code in (200, 503)


class TestAuthBackup:
    """POST /admin/backup requiere x-admin-token válido."""

    def test_sin_token(self):
        r = client.post("/admin/backup")
        assert r.status_code == 403
        assert "Token" in r.json()["detail"]

    def test_token_incorrecto(self):
        r = client.post("/admin/backup", headers={"x-admin-token": "token_falso"})
        assert r.status_code == 403

    def test_token_valido(self):
        if not ADMIN_TOKEN:
            pytest.skip("ADMIN_TOKEN no configurado en .env")
        r = client.post("/admin/backup", headers={"x-admin-token": ADMIN_TOKEN})
        assert r.status_code == 200
        assert r.json()["status"] == "scheduled"


class TestAuthNotifyWa:
    """POST /admin/notify/wa requiere x-admin-token y body válido."""

    def test_sin_token(self):
        r = client.post("/admin/notify/wa", json={"phones": ["573001234567"], "message": "test"})
        assert r.status_code == 403

    def test_body_vacio(self):
        if not ADMIN_TOKEN:
            pytest.skip("ADMIN_TOKEN no configurado")
        r = client.post("/admin/notify/wa",
                        headers={"x-admin-token": ADMIN_TOKEN},
                        json={})
        assert r.status_code == 422

    def test_phones_vacio(self):
        if not ADMIN_TOKEN:
            pytest.skip("ADMIN_TOKEN no configurado")
        r = client.post("/admin/notify/wa",
                        headers={"x-admin-token": ADMIN_TOKEN},
                        json={"phones": [], "message": "test"})
        assert r.status_code == 400

    def test_mensaje_vacio(self):
        if not ADMIN_TOKEN:
            pytest.skip("ADMIN_TOKEN no configurado")
        r = client.post("/admin/notify/wa",
                        headers={"x-admin-token": ADMIN_TOKEN},
                        json={"phones": ["573001234567"], "message": "   "})
        assert r.status_code == 400


class TestAuthAutoNotify:
    """POST /admin/auto-notify requiere x-admin-token."""

    def test_sin_token(self):
        r = client.post("/admin/auto-notify")
        assert r.status_code == 403

    def test_token_valido(self):
        if not ADMIN_TOKEN:
            pytest.skip("ADMIN_TOKEN no configurado")
        r = client.post("/admin/auto-notify", headers={"x-admin-token": ADMIN_TOKEN})
        assert r.status_code == 200
        assert r.json()["status"] == "scheduled"
