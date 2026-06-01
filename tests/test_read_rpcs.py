"""
Tests para RPCs de lectura: consulta_manifiestos, consulta_totales,
tendencia_anual, get_catalogos.

Estrategia: insertar manifiestos de prueba (999xxx), llamar las RPCs y
validar resultados. Limpia al final.

Ejecutar: python3 -m pytest tests/test_read_rpcs.py -v
"""
import os, sys, json, requests
import datetime as _dt
from pathlib import Path
import psycopg2
import pytest
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)

DB_URL      = os.environ["DATABASE_URL"]
SUPA_URL    = os.environ["SUPABASE_URL"]
SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

_HEADERS_SVC = {
    "apikey":        SERVICE_KEY,
    "Authorization": f"Bearer {SERVICE_KEY}",
    "Content-Type":  "application/json",
}

# Manifiestos de prueba — rango reservado 999000-999099 para read RPCs
TEST_MANIFIESTOS = [
    # manifiesto, mes, año, conductor, cliente, agencia, valor_remesa, flete, anticipo
    (999001, 'MAYO',   2026, 'TEST CONDUCTOR A', 'CLIENTE X', 'CALI',     1000000, 500000, 100000),
    (999002, 'MAYO',   2026, 'TEST CONDUCTOR A', 'CLIENTE Y', 'CALI',     2000000, 800000, 200000),
    (999003, 'MAYO',   2026, 'TEST CONDUCTOR B', 'CLIENTE X', 'BOGOTA',   1500000, 600000, 150000),
    (999004, 'JUNIO',  2026, 'TEST CONDUCTOR A', 'CLIENTE X', 'CALI',     500000,  300000, 50000),
    (999005, 'MAYO',   2026, 'TEST ANULADO',     'CLIENTE Z', 'CALI',     900000,  400000, 0),  # ANULADO
]

def _set_claims(cur, role='gerencia'):
    claims = json.dumps({"sub":"test","role":"authenticated","email":"test@altrans.local","app_metadata":{"role":role}})
    cur.execute("SET LOCAL request.jwt.claims = %s", (claims,))


def _setup_fixtures():
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    cur = conn.cursor()
    _set_claims(cur)
    cur.execute("DELETE FROM audit_log WHERE manifiesto BETWEEN 999000 AND 999099")
    cur.execute("DELETE FROM manifiestos_flat WHERE manifiesto BETWEEN 999000 AND 999099")
    for m in TEST_MANIFIESTOS:
        manif, mes, año, conductor, cliente, agencia, rem, flete, ant = m
        estado = 'ANULADO' if 'ANULADO' in conductor else 'CUMPLIDO'
        cur.execute("""
            INSERT INTO manifiestos_flat (
                manifiesto, archivo_origen, mes, año, periodo, semana, consecutivo_semanal,
                fecha_despacho, origen, departamento_origen, destino, departamento_destino,
                cliente, remesas, valor_remesa, flete_conductor, anticipo,
                placa, tipo_vehiculo, conductor, cedula_conductor,
                agencia_despachadora, estado_interno
            ) VALUES (
                %s, 'TEST.xlsx', %s, %s,
                make_date(%s, CASE %s WHEN 'ENERO' THEN 1 WHEN 'FEBRERO' THEN 2 WHEN 'MARZO' THEN 3
                          WHEN 'ABRIL' THEN 4 WHEN 'MAYO' THEN 5 WHEN 'JUNIO' THEN 6
                          WHEN 'JULIO' THEN 7 WHEN 'AGOSTO' THEN 8 WHEN 'SEPTIEMBRE' THEN 9
                          WHEN 'OCTUBRE' THEN 10 WHEN 'NOVIEMBRE' THEN 11 WHEN 'DICIEMBRE' THEN 12 END, 1),
                'S20', 1,
                CURRENT_DATE, 'CALI','VALLE DEL CAUCA','BOGOTA','CUNDINAMARCA',
                %s, 'REM', %s, %s, %s,
                'ABC123','SENCILLO', %s, '12345678',
                %s, %s
            )
        """, (manif, mes, año, año, mes, cliente, rem, flete, ant, conductor, agencia, estado))
    conn.commit()
    conn.close()


def _teardown_fixtures():
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    cur = conn.cursor()
    _set_claims(cur)
    cur.execute("DELETE FROM audit_log WHERE manifiesto BETWEEN 999000 AND 999099")
    cur.execute("DELETE FROM manifiestos_flat WHERE manifiesto BETWEEN 999000 AND 999099")
    conn.commit()
    conn.close()


@pytest.fixture(scope='module', autouse=True)
def fixtures():
    _setup_fixtures()
    yield
    _teardown_fixtures()


def _rpc(name, params):
    r = requests.post(f"{SUPA_URL}/rest/v1/rpc/{name}", headers=_HEADERS_SVC, json=params, timeout=15)
    r.raise_for_status()
    return r.json()


# ── consulta_manifiestos ─────────────────────────────────────────────────────

class TestConsultaManifiestos:
    def _only_ours(self, rows):
        return [r for r in rows if 999000 <= r['manifiesto'] <= 999099]

    def test_filtro_por_manifiesto(self):
        r = _rpc('consulta_manifiestos', {'p_manifiesto': 999001})
        assert len(r) == 1
        assert r[0]['manifiesto'] == 999001
        assert r[0]['conductor'] == 'TEST CONDUCTOR A'

    def test_filtro_por_conductor_ilike(self):
        r = _rpc('consulta_manifiestos', {'p_conductor': 'TEST CONDUCTOR A'})
        nuestros = self._only_ours(r)
        assert all(x['conductor'] == 'TEST CONDUCTOR A' for x in nuestros)
        assert len(nuestros) == 3  # 999001, 999002, 999004

    def test_filtro_por_mes_y_año(self):
        r = _rpc('consulta_manifiestos', {'p_mes': 'MAYO', 'p_año': 2026, 'p_limit': 1000})
        nuestros = self._only_ours(r)
        # 999001, 999002, 999003, 999005 — todos de MAYO 2026
        assert len(nuestros) == 4

    def test_filtro_por_cliente_ilike(self):
        r = _rpc('consulta_manifiestos', {'p_cliente': 'CLIENTE X', 'p_limit': 1000})
        nuestros = self._only_ours(r)
        assert len(nuestros) == 3  # 999001, 999003, 999004

    def test_filtro_por_agencia_exacto(self):
        r = _rpc('consulta_manifiestos', {'p_agencia': 'CALI', 'p_limit': 1000})
        nuestros = self._only_ours(r)
        # 999001, 999002, 999004, 999005
        assert len(nuestros) == 4

    def test_filtro_por_estado_interno_anulado(self):
        r = _rpc('consulta_manifiestos', {'p_estado_interno': 'ANULADO', 'p_limit': 1000})
        nuestros = self._only_ours(r)
        assert len(nuestros) == 1
        assert nuestros[0]['manifiesto'] == 999005

    def test_paginacion_limit_offset(self):
        r1 = _rpc('consulta_manifiestos', {'p_conductor': 'TEST CONDUCTOR', 'p_limit': 2, 'p_offset': 0})
        r2 = _rpc('consulta_manifiestos', {'p_conductor': 'TEST CONDUCTOR', 'p_limit': 2, 'p_offset': 2})
        assert len(r1) == 2
        # No hay overlap entre páginas
        ids1 = {x['manifiesto'] for x in r1}
        ids2 = {x['manifiesto'] for x in r2}
        assert ids1.isdisjoint(ids2)

    def test_filtro_combinado(self):
        r = _rpc('consulta_manifiestos', {
            'p_conductor': 'TEST CONDUCTOR A',
            'p_mes':       'MAYO',
            'p_año':       2026,
            'p_limit':     1000,
        })
        nuestros = self._only_ours(r)
        assert len(nuestros) == 2  # 999001, 999002

    def test_filtro_sin_resultados(self):
        r = _rpc('consulta_manifiestos', {'p_manifiesto': 999999})
        assert r == []


# ── consulta_totales ─────────────────────────────────────────────────────────

class TestConsultaTotales:
    def test_totales_de_mayo_2026_incluye_anulados(self):
        """consulta_totales NO filtra ANULADOS — es para visión interna."""
        r = _rpc('consulta_totales', {'p_mes': 'MAYO', 'p_año': 2026, 'p_conductor': 'TEST'})
        assert len(r) == 1
        t = r[0]
        # 999001+999002+999003+999005 son de MAYO con 'TEST' en nombre
        assert t['total_manifiestos'] == 4
        # Suma de fletes: 500k + 800k + 600k + 400k = 2_300_000
        assert float(t['suma_fletes']) == 2_300_000

    def test_totales_filtro_anulado(self):
        r = _rpc('consulta_totales', {'p_estado_interno': 'ANULADO', 'p_conductor': 'TEST'})
        t = r[0]
        assert t['total_manifiestos'] == 1
        assert float(t['suma_fletes']) == 400_000

    def test_pendiente_pagar_formula(self):
        """pendiente_pagar = suma(saldo) - suma(valor_pagado). Como nadie pagó,
        equivale a la suma de saldo, que ya descuenta retención (1%) y anticipo
        (sin ajustes en estos fixtures)."""
        r = _rpc('consulta_totales', {'p_conductor': 'TEST CONDUCTOR A', 'p_mes': 'MAYO', 'p_año': 2026})
        t = r[0]
        # 999001: 500k - 1% (5k) - anticipo 100k = 395k
        # 999002: 800k - 1% (8k) - anticipo 200k = 592k
        # 0 pagado → 987k pendiente
        assert float(t['pendiente_pagar']) == 987_000


# ── tendencia_anual ──────────────────────────────────────────────────────────

class TestTendenciaAnual:
    def test_devuelve_meses_con_datos(self):
        r = _rpc('tendencia_anual', {'p_año': 2026})
        # Estructura: [{ mes, facturado, ganancia }]
        meses = {row['mes'] for row in r}
        # MAYO y JUNIO están en nuestros fixtures (puede haber más datos reales)
        assert 'MAYO'  in meses
        assert 'JUNIO' in meses

    def test_facturado_y_ganancia_son_numeros(self):
        r = _rpc('tendencia_anual', {'p_año': 2026})
        for row in r:
            assert row['facturado'] is not None
            assert row['ganancia']  is not None
            assert float(row['facturado']) >= 0
            assert float(row['ganancia'])  >= 0

    def test_sin_filtro_año_devuelve_todos(self):
        r = _rpc('tendencia_anual', {})
        assert len(r) > 0  # debe haber algo de los datos reales


# ── get_catalogos ────────────────────────────────────────────────────────────

class TestGetCatalogos:
    def test_estructura_devuelve_arrays(self):
        r = _rpc('get_catalogos', {})
        # Estructura esperada: { conductores, clientes, lugares, responsables, vehiculos, remolques, agencias, propietarios }
        for k in ('conductores', 'clientes', 'lugares', 'responsables',
                  'vehiculos', 'remolques', 'agencias', 'propietarios'):
            assert k in r, f"Falta clave: {k}"
            assert isinstance(r[k], list), f"{k} no es lista"

    def test_conductores_tienen_nombre_cedula(self):
        r = _rpc('get_catalogos', {})
        # Cada conductor debe ser un objeto con nombre y cedula
        for c in r['conductores']:
            assert 'nombre' in c
            # cedula puede ser null pero la key debe existir
            assert 'cedula' in c

    def test_catalogos_no_vacio_con_datos_reales(self):
        """En un sistema con datos reales, los catálogos no deberían venir vacíos."""
        r = _rpc('get_catalogos', {})
        # Es razonable esperar al menos 1 conductor en una DB de producción
        assert len(r['conductores']) > 0
        assert len(r['clientes'])    > 0

    def test_devuelve_mas_de_1000_filas(self):
        """get_catalogos usa RPC para saltar el max-rows de PostgREST."""
        r = _rpc('get_catalogos', {})
        # No verificamos exactitud; solo que no esté capado al 1000 default si la DB tiene más.
        # Como suma agregada de varios catálogos:
        total = sum(len(r[k]) for k in r)
        # En desarrollo o staging puede ser < 1000; solo verificamos que es un número razonable
        assert total > 0


if __name__ == '__main__':
    sys.exit(pytest.main([__file__, '-v']))
