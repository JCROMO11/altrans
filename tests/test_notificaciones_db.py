"""
Tests de base de datos para el módulo de notificaciones.

Cubre:
  1. Triggers: trg_notify_plazo_vigente, trg_notify_pago_realizado
  2. RPC get_pendientes_notificacion: categorización + deduplicación
  3. Guardrail de novedades: ruido (TURBO/URBANO), trivial (./ok), real

Manifiestos de prueba: 1-20 (números < 21074 para que el RPC los incluya).

Ejecutar: python3 -m pytest tests/test_notificaciones_db.py -v
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

# Rango de manifiestos de prueba: números < 21074 (mínimo real) para que
# el RPC ordenado ASC los devuelva en el primer lote (Supabase REST limita
# a 1000 filas por RPC, sin soporte de paginación efectivo).
MIN = 1
MAX = 200


def _set_claims(cur, role='gerencia'):
    claims = json.dumps({"sub":"test","role":"authenticated","email":"test@altrans.local","app_metadata":{"role":role}})
    cur.execute("SET LOCAL request.jwt.claims = %s", (claims,))


def _clear_range():
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    cur = conn.cursor()
    _set_claims(cur)
    cur.execute("DELETE FROM messages_sent WHERE manifiesto BETWEEN %s AND %s", (MIN, MAX))
    cur.execute("DELETE FROM audit_log WHERE manifiesto BETWEEN %s AND %s", (MIN, MAX))
    cur.execute("DELETE FROM manifiestos_flat WHERE manifiesto BETWEEN %s AND %s", (MIN, MAX))
    conn.commit()
    conn.close()


def _insert_minimal(manif: int, **kw):
    """Inserta un manifiesto con valores mínimos. kw sobreescribe defaults."""
    defaults = {
        "manifiesto": manif,
        "archivo_origen": "TEST_NOTIF.xlsx",
        "mes": "JULIO", "año": 2026,
        "periodo": f"2026-07-01", "semana": "S29", "consecutivo_semanal": 1,
        "fecha_despacho": "2026-07-01",
        "origen": "CALI", "departamento_origen": "VALLE DEL CAUCA",
        "destino": "BOGOTA", "departamento_destino": "CUNDINAMARCA",
        "cliente": "TEST NOTIF", "remesas": "REM",
        "valor_remesa": 1000000, "flete_conductor": 500000, "anticipo": 100000,
        "placa": "TEST999", "tipo_vehiculo": "SENCILLO",
        "conductor": "TEST CONDUCTOR", "cedula_conductor": "12345678",
        "agencia_despachadora": "TEST AGENCIA", "estado_interno": "CUMPLIDO",
        "celular": "3001111111",
        "novedades": None, "factura_no": "F-TEST",
        "fecha_cumplido": None, "fecha_pago": None, "valor_pagado": None,
        "compromiso_pago": "PAGO A 15 DIAS",
    }
    defaults.update(kw)
    cols = ", ".join(defaults.keys())
    placeholders = ", ".join(["%s"] * len(defaults))
    values = list(defaults.values())

    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    cur = conn.cursor()
    _set_claims(cur)
    cur.execute(f"""
        INSERT INTO manifiestos_flat ({cols})
        VALUES ({placeholders})
    """, values)
    conn.commit()
    conn.close()


def _rpc(name, params=None):
    r = requests.post(f"{SUPA_URL}/rest/v1/rpc/{name}", headers=_HEADERS_SVC,
                      json=params or {}, timeout=15)
    r.raise_for_status()
    return r.json()


def _get_msgs(manif: int) -> list:
    """Lee messages_sent para un manifiesto."""
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("""
        SELECT id, template_name, phone, status, sent_at, error
        FROM messages_sent WHERE manifiesto = %s ORDER BY id
    """, (manif,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


# ── Fixture ────────────────────────────────────────────────────────────────

@pytest.fixture(scope='module', autouse=True)
def setup_teardown():
    _clear_range()
    yield
    _clear_range()


# ═══════════════════════════════════════════════════════════════════════════
# 1. TRIGGERS
# ═══════════════════════════════════════════════════════════════════════════

class TestTriggerPlazoVigente:
    """trg_notify_plazo_vigente: dispara al marcar fecha_cumplido."""

    def test_normal_crea_pending(self):
        _insert_minimal(1, celular="3001111111")
        conn = psycopg2.connect(DB_URL)
        conn.autocommit = False
        cur = conn.cursor()
        _set_claims(cur)
        cur.execute("UPDATE manifiestos_flat SET fecha_cumplido = CURRENT_DATE WHERE manifiesto = 1")
        conn.commit()
        conn.close()

        msgs = _get_msgs(1)
        assert len(msgs) == 1
        assert msgs[0][1] == 'saldo_plazo_vigente'
        assert msgs[0][2] == '3001111111'
        assert msgs[0][3] == 'pending'

    def test_ya_pagado_no_dispara(self):
        _insert_minimal(2, celular="3002222222", fecha_pago="2026-01-01")
        conn = psycopg2.connect(DB_URL)
        conn.autocommit = False
        cur = conn.cursor()
        _set_claims(cur)
        cur.execute("UPDATE manifiestos_flat SET fecha_cumplido = CURRENT_DATE WHERE manifiesto = 2")
        conn.commit()
        conn.close()

        msgs = _get_msgs(2)
        assert len(msgs) == 0

    def test_anulado_no_dispara(self):
        _insert_minimal(3, celular="3003333333", estado_interno="ANULADO")
        conn = psycopg2.connect(DB_URL)
        conn.autocommit = False
        cur = conn.cursor()
        _set_claims(cur)
        cur.execute("UPDATE manifiestos_flat SET fecha_cumplido = CURRENT_DATE WHERE manifiesto = 3")
        conn.commit()
        conn.close()

        msgs = _get_msgs(3)
        assert len(msgs) == 0

    def test_sin_celular_no_dispara(self):
        _insert_minimal(4, celular="300")  # invalido (<10 digitos)
        conn = psycopg2.connect(DB_URL)
        conn.autocommit = False
        cur = conn.cursor()
        _set_claims(cur)
        cur.execute("UPDATE manifiestos_flat SET fecha_cumplido = CURRENT_DATE WHERE manifiesto = 4")
        conn.commit()
        conn.close()

        msgs = _get_msgs(4)
        assert len(msgs) == 0


class TestTriggerPagoRealizado:
    """trg_notify_pago_realizado: dispara al marcar fecha_pago + valor."""

    def test_normal_crea_pending(self):
        _insert_minimal(5, celular="3004444444")
        conn = psycopg2.connect(DB_URL)
        conn.autocommit = False
        cur = conn.cursor()
        _set_claims(cur)
        cur.execute("""
            UPDATE manifiestos_flat
            SET fecha_pago = CURRENT_DATE, valor_pagado = 400000
            WHERE manifiesto = 5
        """)
        conn.commit()
        conn.close()

        msgs = _get_msgs(5)
        assert len(msgs) == 1
        assert msgs[0][1] == 'pago_realizado'
        assert msgs[0][3] == 'pending'

    def test_sin_valor_pagado_no_dispara(self):
        _insert_minimal(6, celular="3005555555")
        conn = psycopg2.connect(DB_URL)
        conn.autocommit = False
        cur = conn.cursor()
        _set_claims(cur)
        cur.execute("""
            UPDATE manifiestos_flat
            SET fecha_pago = CURRENT_DATE
            WHERE manifiesto = 6
        """)
        conn.commit()
        conn.close()

        msgs = _get_msgs(6)
        assert len(msgs) == 0

    def test_anulado_no_dispara(self):
        _insert_minimal(7, celular="3006666666", estado_interno="ANULADO")
        conn = psycopg2.connect(DB_URL)
        conn.autocommit = False
        cur = conn.cursor()
        _set_claims(cur)
        cur.execute("""
            UPDATE manifiestos_flat
            SET fecha_pago = CURRENT_DATE, valor_pagado = 400000
            WHERE manifiesto = 7
        """)
        conn.commit()
        conn.close()

        msgs = _get_msgs(7)
        assert len(msgs) == 0


# ═══════════════════════════════════════════════════════════════════════════
# 2. RPC get_pendientes_notificacion
# ═══════════════════════════════════════════════════════════════════════════

class TestRpcCategorizacion:
    """Verifica que el RPC asigna el template_name correcto según cada caso."""

    @classmethod
    def setup_class(cls):
        _clear_range()
        hoy = _dt.date.today()

        # 10: novedad real → saldo_novedad_pendiente
        _insert_minimal(10, celular="3007100001",
                        novedades="AVERÍA EN MERCANCÍA",
                        factura_no="F710",
                        fecha_cumplido=str(hoy - _dt.timedelta(days=5)))
        # 11: sin novedades, sin factura → saldo_falta_factura
        _insert_minimal(11, celular="3007110001",
                        novedades=None, factura_no=None,
                        fecha_cumplido=str(hoy - _dt.timedelta(days=5)))
        # 12: novedad ruido "TURBO", con factura + fecha >21d → saldo_falta_documentacion
        _insert_minimal(12, celular="3007120001",
                        novedades="TURBO",
                        factura_no="F712",
                        fecha_cumplido=str(hoy - _dt.timedelta(days=30)))
        # 13: novedad ruido "URBANOS", con factura + fecha reciente → saldo_plazo_vigente
        _insert_minimal(13, celular="3007130001",
                        novedades="URBANOS",
                        factura_no="F713",
                        fecha_cumplido=str(hoy - _dt.timedelta(days=5)))
        # 14: novedad trivial "." → no es real, fecha reciente → saldo_plazo_vigente
        _insert_minimal(14, celular="3007140001",
                        novedades=".",
                        factura_no="F714",
                        fecha_cumplido=str(hoy - _dt.timedelta(days=5)))
        # 18: novedad real "REAJUSTE" → saldo_novedad_pendiente
        _insert_minimal(18, celular="3007180001",
                        novedades="REAJUSTE DE FLETE POR DAÑOS",
                        factura_no="F718",
                        fecha_cumplido=str(hoy - _dt.timedelta(days=5)))
        # 19: novedad trivial "ok", sin factura → saldo_falta_factura
        _insert_minimal(19, celular="3007190001",
                        novedades="ok",
                        factura_no=None,
                        fecha_cumplido=str(hoy - _dt.timedelta(days=5)))

    def _match(self, rows, manifiesto):
        return [r for r in rows if r['manifiesto'] == manifiesto]

    def test_novedad_real(self):
        rows = _rpc('get_pendientes_notificacion')
        m = self._match(rows, 10)
        assert len(m) == 1, f"10 deberia aparecer, rows={[(r['manifiesto'],r['template_name']) for r in rows[:10]]}"
        assert m[0]['template_name'] == 'saldo_novedad_pendiente'
        assert m[0]['novedades'] == 'AVERÍA EN MERCANCÍA'

    def test_sin_novedad_sin_factura(self):
        rows = _rpc('get_pendientes_notificacion')
        m = self._match(rows, 11)
        assert len(m) == 1
        assert m[0]['template_name'] == 'saldo_falta_factura'

    def test_novedad_ruido_turbo_cae_a_falta_documentacion(self):
        """TURBO es ruido de clasificación → ignorado, fecha>21d → falta_documentacion"""
        rows = _rpc('get_pendientes_notificacion')
        m = self._match(rows, 12)
        assert len(m) == 1, f"12 deberia aparecer como falta_documentacion"
        assert m[0]['template_name'] == 'saldo_falta_documentacion', f"Got {m[0]['template_name']}"

    def test_novedad_ruido_urbanos_cae_a_plazo_vigente(self):
        """URBANOS es ruido → ignorado, fecha reciente → plazo_vigente"""
        rows = _rpc('get_pendientes_notificacion')
        m = self._match(rows, 13)
        assert len(m) == 1
        assert m[0]['template_name'] == 'saldo_plazo_vigente'

    def test_novedad_trivial_punto_cae_a_plazo_vigente(self):
        """'.' es trivial (len=1) → ignorado, fecha reciente → plazo_vigente"""
        rows = _rpc('get_pendientes_notificacion')
        m = self._match(rows, 14)
        assert len(m) == 1
        assert m[0]['template_name'] == 'saldo_plazo_vigente'

    def test_novedad_real_reajuste(self):
        rows = _rpc('get_pendientes_notificacion')
        m = self._match(rows, 18)
        assert len(m) == 1
        assert m[0]['template_name'] == 'saldo_novedad_pendiente'
        assert 'REAJUSTE' in (m[0]['novedades'] or '')

    def test_novedad_trivial_ok_cae_a_falta_factura(self):
        """'ok' es trivial (len=2) → ignorado, sin factura → falta_factura"""
        rows = _rpc('get_pendientes_notificacion')
        m = self._match(rows, 19)
        assert len(m) == 1
        assert m[0]['template_name'] == 'saldo_falta_factura'


class TestRpcDeduplicacion:
    """Verifica que el RPC excluye ya notificados (<7 días) y pagados."""

    @classmethod
    def setup_class(cls):
        hoy = _dt.date.today()
        # 15: ya notificado (insertar registro sent en messages_sent)
        _insert_minimal(15, celular="3007150001",
                        novedades=None, factura_no="F715",
                        fecha_cumplido=str(hoy - _dt.timedelta(days=5)))
        conn = psycopg2.connect(DB_URL)
        conn.autocommit = False
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO messages_sent (manifiesto, template_name, phone, status, sent_at)
            VALUES (15, 'saldo_plazo_vigente', '3007150001', 'sent', now() - interval '1 day')
        """)
        conn.commit()
        conn.close()

        # 16: ya pagado → no debe aparecer
        _insert_minimal(16, celular="3007160001",
                        novedades=None, factura_no="F716",
                        fecha_cumplido=str(hoy - _dt.timedelta(days=5)),
                        fecha_pago=str(hoy), valor_pagado=400000)

    def _match(self, rows, manifiesto):
        return [r for r in rows if r['manifiesto'] == manifiesto]

    def test_ya_notificado_no_aparece(self):
        rows = _rpc('get_pendientes_notificacion')
        m = self._match(rows, 15)
        assert len(m) == 0, f"15 no deberia aparecer (notificado hace 1 dia): {m}"

    def test_ya_pagado_no_aparece(self):
        rows = _rpc('get_pendientes_notificacion')
        m = self._match(rows, 16)
        assert len(m) == 0


class TestRpcFechaEstimada:
    """Verifica el cálculo de fecha_estimada según compromiso_pago."""

    @classmethod
    def setup_class(cls):
        hoy = _dt.date.today()
        _insert_minimal(17, celular="3007170001",
                        novedades=None, factura_no="F717",
                        fecha_cumplido=str(hoy - _dt.timedelta(days=5)),
                        compromiso_pago="PAGO A 30 DIAS")

    def _match(self, rows, manifiesto):
        return [r for r in rows if r['manifiesto'] == manifiesto]

    def test_fecha_estimada_30_dias(self):
        rows = _rpc('get_pendientes_notificacion')
        m = self._match(rows, 17)
        assert len(m) == 1
        hoy = _dt.date.today()
        expected = hoy - _dt.timedelta(days=5) + _dt.timedelta(days=42)
        actual = _dt.datetime.strptime(m[0]['fecha_estimada'], "%Y-%m-%d").date()
        assert actual == expected, f"Esperado {expected}, obtenido {actual}"
