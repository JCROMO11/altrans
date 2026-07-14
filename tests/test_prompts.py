"""
Tests de prompts editables desde Supabase (system_prompts).

Mockeamos db.queries.get_prompt para verificar que build_system_prompt
y moderate_label carguen desde DB con fallback a inline.

Ejecutar:
  python3 -m pytest tests/test_prompts.py -v
"""
import os
import sys
from unittest.mock import patch, AsyncMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'ai_agent'))

import pytest


@pytest.fixture(autouse=True)
def clear_prompt_cache():
    """Limpia caché de get_prompt entre tests."""
    from db.queries import _prompt_cache
    _prompt_cache.clear()


# ── build_system_prompt ────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_base_prompt_usa_db_cuando_disponible():
    from agent.prompts import build_system_prompt
    from db.queries import _prompt_cache

    with patch("db.queries.get_prompt", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = "BASE: {anio}-{mes_actual}"
        result = await build_system_prompt()
        assert "BASE:" in result
        assert "2026" in result
        mock_get.assert_any_call("system_prompt_base")


@pytest.mark.asyncio
async def test_base_prompt_fallback_cuando_db_no_disponible():
    from agent.prompts import build_system_prompt

    with patch("db.queries.get_prompt", new_callable=AsyncMock) as mock_get:
        mock_get.side_effect = Exception("DB down")
        result = await build_system_prompt()
        assert "Altrans Bot" in result
        assert "colombiano" in result


@pytest.mark.asyncio
async def test_conductor_incluye_block():
    from agent.prompts import build_system_prompt

    with patch("db.queries.get_prompt", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = None  # fuerza fallback
        result = await build_system_prompt(
            nombre="Juan Pérez",
            cedula="12345",
            tipo_usuario="conductor",
        )
        assert "Juan" in result
        assert "12345" in result
        assert "Conductor autenticado" in result


@pytest.mark.asyncio
async def test_propietario_incluye_block():
    from agent.prompts import build_system_prompt

    with patch("db.queries.get_prompt", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = None
        result = await build_system_prompt(
            nombre="Carlos",
            placa="ABC123",
            tipo_usuario="propietario",
        )
        assert "Carlos" in result
        assert "ABC123" in result
        assert "Propietario autenticado" in result


@pytest.mark.asyncio
async def test_admin_incluye_block():
    from agent.prompts import build_system_prompt

    with patch("db.queries.get_prompt", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = None
        result = await build_system_prompt()
        assert "análisis interno" in result


@pytest.mark.asyncio
async def test_conductor_block_desde_db():
    from agent.prompts import build_system_prompt

    db_block = "## DB Conductor\nHablas con *{nombre}* (c.c. {cedula})."
    with patch("db.queries.get_prompt", new_callable=AsyncMock) as mock_get:
        def side_effect(clave):
            if clave == "system_prompt_base":
                return "BASE"
            if clave == "conductor_block":
                return db_block
            return None
        mock_get.side_effect = side_effect
        result = await build_system_prompt(
            nombre="Ana", cedula="999", tipo_usuario="conductor",
        )
        assert "BASE" in result
        assert "DB Conductor" in result
        assert "Ana" in result
        assert "999" in result


# ── moderate_label ─────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_moderate_label_carga_policy_desde_db():
    from agent.graph import moderate_label

    db_policy = "Responde solo con: SAFE"
    with patch("db.queries.get_prompt", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = db_policy
        with patch("agent.graph._mod_client.chat.completions.create", new_callable=AsyncMock) as mock_create:
            mock_create.return_value.choices = [type("obj", (), {"message": type("obj", (), {"content": "SAFE"})()})()]
            label = await moderate_label("Hola")
            assert label == "SAFE"
            sent_policy = mock_create.call_args[1]["messages"][0]["content"]
            assert "Responde solo con: SAFE" in sent_policy


@pytest.mark.asyncio
async def test_moderate_label_fallback_cuando_db_falla():
    from agent.graph import moderate_label, _INLINE_MODERATE_POLICY

    with patch("db.queries.get_prompt", new_callable=AsyncMock) as mock_get:
        mock_get.side_effect = Exception("DB down")
        with patch("agent.graph._mod_client.chat.completions.create", new_callable=AsyncMock) as mock_create:
            mock_create.return_value.choices = [type("obj", (), {"message": type("obj", (), {"content": "SAFE"})()})()]
            label = await moderate_label("Hola")
            assert label == "SAFE"
            sent_policy = mock_create.call_args[1]["messages"][0]["content"]
            assert sent_policy == _INLINE_MODERATE_POLICY


# ── get_prompt cache ───────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_prompt_cache_ttl():
    from db.queries import get_prompt, _prompt_cache

    with patch("db.queries._get") as mock_get:
        mock_get.return_value = [{"contenido": "cached_val"}]
        val1 = await get_prompt("test_clave")
        assert val1 == "cached_val"
        assert "test_clave" in _prompt_cache

        # segunda llamada no debe ir a DB
        _prompt_cache["test_clave"] = ("cached_val", 9999999999.0)  # timestamp futuro
        mock_get.reset_mock()
        val2 = await get_prompt("test_clave")
        assert val2 == "cached_val"
        mock_get.assert_not_called()


@pytest.mark.asyncio
async def test_get_prompt_retorna_none_si_no_existe():
    from db.queries import get_prompt

    with patch("db.queries._get") as mock_get:
        mock_get.return_value = []
        val = await get_prompt("no_existe")
        assert val is None


@pytest.mark.asyncio
async def test_get_prompt_no_crashea_en_error():
    from db.queries import get_prompt

    with patch("db.queries._get") as mock_get:
        mock_get.side_effect = Exception("timeout")
        val = await get_prompt("falla")
        assert val is None