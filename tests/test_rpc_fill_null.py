"""
Tests para guardar_digitador_batch: los campos de conductor/vehículo/propietario
se llenan SOLO cuando están vacíos (manifiestos placeholder) y se preservan
cuando ya tienen un valor.

Ejecutar: python3 -m pytest tests/test_rpc_fill_null.py -v
"""
import os, json
from pathlib import Path
import psycopg2
import pytest
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)

DB_URL = os.environ["DATABASE_URL"]

# Rango reservado para estos tests
TEST_MANIFIESTOS = [999500, 999501]

_RPC_KEYS = [
    "p_archivo_origen", "p_mes", "p_año", "p_periodo", "p_semana", "p_consecutivo_semanal",
    "p_fecha_despacho", "p_origen", "p_departamento_origen", "p_destino", "p_departamento_destino",
    "p_cliente", "p_remesas", "p_valor_remesa", "p_flete_conductor", "p_anticipo", "p_placa",
    "p_placa_remolque", "p_conductor", "p_celular", "p_cedula_conductor", "p_propietario",
    "p_agencia_despachadora", "p_nombre_responsable", "p_reteica", "p_r_fopat",
]


def _connect():
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = True
    cur = conn.cursor()
    claims = json.dumps({
        "sub": "test", "role": "authenticated", "email": "test@altrans.local",
        "app_metadata": {"role": "gerencia"},
    })
    cur.execute("SET request.jwt.claims = %s", (claims,))
    return conn, cur


def _cleanup(cur):
    cur.execute("DELETE FROM audit_log WHERE manifiesto = ANY(%s)", (TEST_MANIFIESTOS,))
    cur.execute("DELETE FROM manifiestos_flat WHERE manifiesto = ANY(%s)", (TEST_MANIFIESTOS,))


def _rpc(cur, manifiesto, **kw):
    params = {k: kw.get(k) for k in _RPC_KEYS}
    params["p_manifiesto"] = manifiesto
    cols = ["p_manifiesto"] + _RPC_KEYS
    placeholders = ",".join(f"%({k})s" for k in cols)
    cur.execute(f"SELECT public.guardar_digitador_batch({placeholders})", params)


@pytest.fixture()
def cur():
    conn, cursor = _connect()
    _cleanup(cursor)
    yield cursor
    _cleanup(cursor)
    conn.close()


def test_placeholder_se_llena(cur):
    cur.execute(
        "INSERT INTO manifiestos_flat (manifiesto, conductor, placa, cedula_conductor, propietario, placa_remolque) "
        "VALUES (999500, NULL, NULL, NULL, NULL, NULL)"
    )
    _rpc(cur, 999500, p_conductor="JUAN NUEVO", p_placa="ABC123",
         p_cedula_conductor="12345678", p_propietario="PROP TEST", p_placa_remolque="R1234")
    cur.execute(
        "SELECT conductor, placa, cedula_conductor, propietario, placa_remolque "
        "FROM manifiestos_flat WHERE manifiesto = 999500"
    )
    assert cur.fetchone() == ("JUAN NUEVO", "ABC123", "12345678", "PROP TEST", "R1234")


def test_dato_existente_se_preserva(cur):
    cur.execute(
        "INSERT INTO manifiestos_flat (manifiesto, conductor, placa, cedula_conductor, propietario) "
        "VALUES (999501, 'CONDUCTOR ORIGINAL', 'ZZZ999', '87654321', 'PROP ORIGINAL')"
    )
    _rpc(cur, 999501, p_conductor="CONDUCTOR NUEVO", p_placa="ABC123",
         p_cedula_conductor="12345678", p_propietario="PROP TEST")
    cur.execute(
        "SELECT conductor, placa, cedula_conductor, propietario "
        "FROM manifiestos_flat WHERE manifiesto = 999501"
    )
    assert cur.fetchone() == ("CONDUCTOR ORIGINAL", "ZZZ999", "87654321", "PROP ORIGINAL")


def test_campos_vacios_se_llenan_individualmente(cur):
    cur.execute(
        "INSERT INTO manifiestos_flat (manifiesto, conductor, placa) "
        "VALUES (999501, 'CONDUCTOR ORIGINAL', NULL)"
    )
    _rpc(cur, 999501, p_conductor="CONDUCTOR NUEVO", p_placa="ABC123",
         p_cedula_conductor="12345678", p_propietario="PROP TEST")
    cur.execute(
        "SELECT conductor, placa, cedula_conductor, propietario "
        "FROM manifiestos_flat WHERE manifiesto = 999501"
    )
    assert cur.fetchone() == ("CONDUCTOR ORIGINAL", "ABC123", "12345678", "PROP TEST")
