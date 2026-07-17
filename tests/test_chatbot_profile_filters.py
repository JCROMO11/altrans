"""
Tests de filtros por perfil en queries del chatbot.

Verifica que cada query aplica los filtros correctos según el tipo de usuario
(conductor, propietario, admin) y que los filtros de exclusión de ANULADOS
están presentes.

Ejecutar: python3 -m pytest tests/test_chatbot_profile_filters.py -v
"""
import os, sys
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ai_agent"))

from db import queries
import pytest


def _mock_get(return_value=None):
    """Parchea _CLIENT.get y devuelve el mock para inspeccionar llamadas."""
    if return_value is None:
        return_value = []
    mock_resp = MagicMock()
    mock_resp.json.return_value = return_value
    mock_resp.raise_for_status.return_value = None
    patcher = patch.object(queries._CLIENT, 'get', return_value=mock_resp)
    return patcher


def _mock_request(return_value=None):
    """Parchea _CLIENT.request y devuelve el mock."""
    mock_resp = MagicMock()
    mock_resp.json.return_value = return_value
    mock_resp.raise_for_status.return_value = None
    patcher = patch.object(queries._CLIENT, 'request', return_value=mock_resp)
    return patcher


# ═══════════════════════════════════════════════════════════════════════════
# Helpers de filtro
# ═══════════════════════════════════════════════════════════════════════════

class TestApplyConductor:
    def test_agrega_cedula_conductor(self):
        params = {}
        queries._apply_conductor(params, "12345")
        assert params["cedula_conductor"] == "eq.12345"

    def test_sin_cedula_no_agrega(self):
        params = {}
        queries._apply_conductor(params, None)
        assert params == {}

    def test_cedula_vacia_no_agrega(self):
        params = {}
        queries._apply_conductor(params, "")
        assert params == {}


class TestApplyPlaca:
    def test_agrega_placa_mayuscula(self):
        params = {}
        queries._apply_placa(params, "abc123")
        assert params["placa"] == "eq.ABC123"

    def test_placa_ya_mayuscula(self):
        params = {}
        queries._apply_placa(params, "ABC123")
        assert params["placa"] == "eq.ABC123"

    def test_sin_placa_no_agrega(self):
        params = {}
        queries._apply_placa(params, None)
        assert params == {}


class TestApplyIdentificador:
    def test_conductor_aplica_cedula(self):
        params = {}
        queries._apply_identificador(params, "12345", "conductor")
        assert "cedula_conductor" in params
        assert "placa" not in params

    def test_propietario_aplica_placa(self):
        params = {}
        queries._apply_identificador(params, "ABC123", "propietario")
        assert "placa" in params
        assert "cedula_conductor" not in params

    def test_sin_tipo_no_aplica(self):
        params = {"select": "manifiesto"}
        queries._apply_identificador(params, "12345", None)
        assert params == {"select": "manifiesto"}

    def test_sin_identificador_no_aplica(self):
        params = {"select": "manifiesto"}
        queries._apply_identificador(params, None, "conductor")
        assert params == {"select": "manifiesto"}

    def test_fallback_a_conductor_si_tipo_desconocido(self):
        params = {}
        queries._apply_identificador(params, "12345", "otro_rol")
        assert "cedula_conductor" in params
        assert "placa" not in params


# ═══════════════════════════════════════════════════════════════════════════
# Listar manifiestos
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
class TestListarManifiestos:
    async def test_conductor_filtra_por_cedula(self):
        with _mock_get() as mock:
            await queries.listar_manifiestos(cedula="12345")
            url = mock.call_args[0][0]
            assert "cedula_conductor=eq.12345" in url

    async def test_propietario_filtra_por_placa(self):
        with _mock_get() as mock:
            await queries.listar_manifiestos(placa="ABC123")
            url = mock.call_args[0][0]
            assert "placa=eq.ABC123" in url

    async def test_conductor_y_placa_aplica_ambos(self):
        with _mock_get() as mock:
            await queries.listar_manifiestos(cedula="12345", placa="ABC123")
            url = mock.call_args[0][0]
            assert "cedula_conductor=eq.12345" in url
            assert "placa=eq.ABC123" in url

    async def test_sin_filtro_no_tiene_cedula_ni_placa(self):
        with _mock_get() as mock:
            await queries.listar_manifiestos()
            url = mock.call_args[0][0]
            assert "cedula_conductor" not in url
            assert "placa" not in url or "placa=" not in url

    async def test_usa_v_chatbot_manifiestos(self):
        with _mock_get() as mock:
            await queries.listar_manifiestos()
            url = mock.call_args[0][0]
            assert "v_chatbot_manifiestos" in url

    async def test_siempre_excluye_anulados(self):
        with _mock_get() as mock:
            await queries.listar_manifiestos()
            url = mock.call_args[0][0]
            assert "estado_interno.neq.ANULADO" in url

    async def test_limit_50(self):
        with _mock_get() as mock:
            await queries.listar_manifiestos()
            url = mock.call_args[0][0]
            assert "limit=50" in url


# ═══════════════════════════════════════════════════════════════════════════
# Consultar manifiesto individual
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
class TestConsultarManifiesto:
    async def test_filtra_por_manifiesto(self):
        with _mock_get() as mock:
            await queries.consultar_manifiesto(12345)
            url = mock.call_args[0][0]
            assert "manifiesto=eq.12345" in url

    async def test_conductor_filtra_cedula(self):
        with _mock_get() as mock:
            await queries.consultar_manifiesto(12345, cedula="67890")
            url = mock.call_args[0][0]
            assert "cedula_conductor=eq.67890" in url

    async def test_propietario_filtra_placa(self):
        with _mock_get() as mock:
            await queries.consultar_manifiesto(12345, placa="ABC123")
            url = mock.call_args[0][0]
            assert "placa=eq.ABC123" in url

    async def test_usa_v_chatbot_manifiestos(self):
        with _mock_get() as mock:
            await queries.consultar_manifiesto(12345)
            url = mock.call_args[0][0]
            assert "v_chatbot_manifiestos" in url

    async def test_devuelve_none_si_anulado(self):
        with _mock_get(return_value=[{"estado_interno": "ANULADO", "manifiesto": 12345}]) as mock:
            result = await queries.consultar_manifiesto(12345)
            assert result is None


# ═══════════════════════════════════════════════════════════════════════════
# Resumen período
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
class TestResumenPeriodo:
    async def test_excluye_anulados(self):
        with _mock_get() as mock:
            await queries.resumen_periodo()
            url = mock.call_args[0][0]
            assert "estado_interno.neq.ANULADO" in url

    async def test_conductor_aplica_cedula(self):
        with _mock_get() as mock:
            await queries.resumen_periodo(cedula="12345")
            url = mock.call_args[0][0]
            assert "cedula_conductor=eq.12345" in url

    async def test_propietario_aplica_placa(self):
        with _mock_get() as mock:
            await queries.resumen_periodo(placa="ABC123")
            url = mock.call_args[0][0]
            assert "placa=eq.ABC123" in url


# ═══════════════════════════════════════════════════════════════════════════
# Auth helpers
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
class TestAuthQueries:
    async def test_get_conductor_by_cedula_filtra_cedula(self):
        with _mock_get() as mock:
            await queries.get_conductor_by_cedula("12345")
            url = mock.call_args[0][0]
            assert "cedula_conductor=eq.12345" in url

    async def test_get_conductor_by_cedula_excluye_anulados(self):
        with _mock_get() as mock:
            await queries.get_conductor_by_cedula("12345")
            url = mock.call_args[0][0]
            assert "estado_interno.neq.ANULADO" in url

    async def test_verificar_manifiesto_conductor_filtra_manifiesto_y_cedula(self):
        with _mock_get() as mock:
            await queries.verificar_manifiesto_conductor(99999, "12345")
            url = mock.call_args[0][0]
            assert "manifiesto=eq.99999" in url
            assert "cedula_conductor=eq.12345" in url

    async def test_verificar_manifiesto_propietario_filtra_manifiesto_y_placa(self):
        with _mock_get() as mock:
            await queries.verificar_manifiesto_propietario(99999, "ABC123")
            url = mock.call_args[0][0]
            assert "manifiesto=eq.99999" in url
            assert "placa=eq.ABC123" in url

    async def test_get_propietario_by_placa_normaliza_mayusculas(self):
        with _mock_get() as mock:
            await queries.get_propietario_by_placa("abc123")
            url = mock.call_args[0][0]
            assert "placa=eq.ABC123" in url

    async def test_get_propietario_by_placa_excluye_anulados(self):
        with _mock_get() as mock:
            await queries.get_propietario_by_placa("ABC123")
            url = mock.call_args[0][0]
            assert "estado_interno.neq.ANULADO" in url


# ═══════════════════════════════════════════════════════════════════════════
# Filtro periódico (mes/año) en helpers
# ═══════════════════════════════════════════════════════════════════════════

class TestApplyPeriodo:
    def test_mes_y_año(self):
        params = {}
        queries._apply_periodo(params, "MARZO", 2024)
        assert params["mes"] == "eq.MARZO"
        assert params["año"] == "eq.2024"

    def test_solo_mes(self):
        params = {}
        queries._apply_periodo(params, "abril")
        assert "mes" in params
        assert "año" not in params

    def test_mes_se_convierte_a_mayuscula(self):
        params = {}
        queries._apply_periodo(params, "abril")
        assert params["mes"] == "eq.ABRIL"

    def test_sin_parametros_no_agrega(self):
        params = {}
        queries._apply_periodo(params)
        assert params == {}
