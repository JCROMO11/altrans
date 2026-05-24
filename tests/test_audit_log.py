"""
Tests del audit_log: el trigger trg_audit_manifiestos debe registrar
correctamente CADA cambio en los campos auditados.

Ejecutar: python3 -m pytest tests/test_audit_log.py -v
"""
import os, sys, json
from pathlib import Path
import psycopg2
import pytest
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)

DB_URL = os.environ["DATABASE_URL"]

TEST_MANIF = 999201  # rango reservado para audit log tests


def _conn():
    return psycopg2.connect(DB_URL)


def _claims_for(role='admin', email='test@altrans.local'):
    return json.dumps({
        'sub':'test','role':'authenticated','email':email,
        'app_metadata':{'role':role},
    })


def _setup():
    conn = _conn(); conn.autocommit = False
    cur = conn.cursor()
    cur.execute("SET LOCAL request.jwt.claims = %s", (_claims_for(),))
    cur.execute("DELETE FROM audit_log WHERE manifiesto = %s", (TEST_MANIF,))
    cur.execute("DELETE FROM manifiestos_flat WHERE manifiesto = %s", (TEST_MANIF,))
    cur.execute("""
        INSERT INTO manifiestos_flat (
            manifiesto, archivo_origen, mes, año, periodo, semana,
            consecutivo_semanal, fecha_despacho, origen, departamento_origen,
            destino, departamento_destino, cliente, remesas,
            placa, tipo_vehiculo, conductor, cedula_conductor,
            valor_remesa, flete_conductor, anticipo
        ) VALUES (
            %s,'TEST.xlsx','MAYO',2026,'2026-05-01','S20',1,
            '2026-05-01','BOGOTA','CUNDINAMARCA','CALI','VALLE DEL CAUCA',
            'CLIENTE ORIG','REM','ABC123','SENCILLO','CONDUCTOR ORIG','12345678',
            1000000, 500000, 100000
        )
    """, (TEST_MANIF,))
    conn.commit()
    # Limpiar audit_log generado por inserts/updates de fixtures
    cur.execute("DELETE FROM audit_log WHERE manifiesto = %s", (TEST_MANIF,))
    conn.commit()
    conn.close()


def _teardown():
    conn = _conn(); conn.autocommit = False
    cur = conn.cursor()
    cur.execute("SET LOCAL request.jwt.claims = %s", (_claims_for(),))
    cur.execute("DELETE FROM audit_log WHERE manifiesto = %s", (TEST_MANIF,))
    cur.execute("DELETE FROM manifiestos_flat WHERE manifiesto = %s", (TEST_MANIF,))
    conn.commit()
    conn.close()


def _audit_rows(manifiesto):
    conn = _conn(); conn.autocommit = True
    cur = conn.cursor()
    cur.execute("""
        SELECT campo, valor_anterior, valor_nuevo, usuario, ejecutado_en
        FROM audit_log
        WHERE manifiesto = %s
        ORDER BY ejecutado_en, id
    """, (manifiesto,))
    rows = cur.fetchall()
    conn.close()
    return [{
        'campo':          r[0],
        'valor_anterior': r[1],
        'valor_nuevo':    r[2],
        'usuario':        r[3],
        'ejecutado_en':   r[4],
    } for r in rows]


def _clear_audit(manifiesto):
    conn = _conn(); conn.autocommit = False
    cur = conn.cursor()
    cur.execute("SET LOCAL request.jwt.claims = %s", (_claims_for(),))
    cur.execute("DELETE FROM audit_log WHERE manifiesto = %s", (manifiesto,))
    conn.commit()
    conn.close()


def _update_with(role, email, **changes):
    """Ejecuta UPDATE simulando rol y email específicos."""
    conn = _conn(); conn.autocommit = False
    cur = conn.cursor()
    cur.execute("SET LOCAL request.jwt.claims = %s", (_claims_for(role, email),))
    sets = ", ".join(f"{k} = %s" for k in changes.keys())
    cur.execute(
        f"UPDATE manifiestos_flat SET {sets} WHERE manifiesto = %s",
        (*changes.values(), TEST_MANIF),
    )
    conn.commit()
    conn.close()


@pytest.fixture(autouse=True)
def fixtures():
    _setup()
    yield
    _teardown()


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestAuditLog:

    def test_update_de_un_campo_genera_una_fila(self):
        _update_with('admin', 'tester@altrans.local', cliente='CLIENTE NUEVO')

        rows = _audit_rows(TEST_MANIF)
        assert len(rows) == 1
        r = rows[0]
        assert r['campo'] == 'cliente'
        assert r['valor_anterior'] == 'CLIENTE ORIG'
        assert r['valor_nuevo']    == 'CLIENTE NUEVO'

    def test_registra_email_del_usuario(self):
        _update_with('admin', 'maria@altrans.local', cliente='OTRO')
        rows = _audit_rows(TEST_MANIF)
        assert rows[0]['usuario'] == 'maria@altrans.local'

    def test_update_varios_campos_genera_varias_filas(self):
        _update_with('admin', 'x@y.com',
                     cliente='NUEVO', placa='XYZ999', conductor='CONDUCTOR NUEVO')

        rows = _audit_rows(TEST_MANIF)
        campos = {r['campo'] for r in rows}
        assert {'cliente', 'placa', 'conductor'} <= campos

    def test_update_a_mismo_valor_no_genera_fila(self):
        # Cliente actual = 'CLIENTE ORIG'. Asignar el mismo valor → no debe haber fila.
        _update_with('admin', 'x@y.com', cliente='CLIENTE ORIG')
        rows = _audit_rows(TEST_MANIF)
        # El campo 'cliente' NO debe estar
        assert not any(r['campo'] == 'cliente' for r in rows)

    def test_null_a_valor_se_audita(self):
        # novedades empieza en NULL; asignar texto
        _update_with('operativo', 'op@altrans.local', novedades='Revisar peso')
        rows = _audit_rows(TEST_MANIF)
        assert any(r['campo'] == 'novedades' and r['valor_anterior'] is None
                   and r['valor_nuevo'] == 'Revisar peso' for r in rows)

    def test_valor_a_null_se_audita(self):
        _update_with('admin', 'x@y.com', conductor=None)
        rows = _audit_rows(TEST_MANIF)
        c = next(r for r in rows if r['campo'] == 'conductor')
        assert c['valor_anterior'] == 'CONDUCTOR ORIG'
        assert c['valor_nuevo'] is None

    def test_campo_numerico_se_serializa_como_texto(self):
        _update_with('admin', 'x@y.com', flete_conductor=600000)
        rows = _audit_rows(TEST_MANIF)
        f = next(r for r in rows if r['campo'] == 'flete_conductor')
        # PostgreSQL serializa NUMERIC(14,2) como '500000.00'
        assert f['valor_anterior'] in ('500000', '500000.00')
        assert f['valor_nuevo']    in ('600000', '600000.00')

    def test_cambio_de_flete_genera_audit_de_flete_neto_tambien(self):
        """flete_neto_conductor es GENERATED — se recalcula y debe loguearse."""
        _update_with('admin', 'x@y.com', flete_conductor=700000)
        rows = _audit_rows(TEST_MANIF)
        campos = {r['campo'] for r in rows}
        assert 'flete_conductor'      in campos
        assert 'flete_neto_conductor' in campos

    def test_cambios_secuenciales_acumulan_filas(self):
        _update_with('admin', 'a@x.com', cliente='V1')
        _update_with('admin', 'b@x.com', cliente='V2')
        _update_with('admin', 'c@x.com', cliente='V3')
        rows = _audit_rows(TEST_MANIF)
        cliente_rows = [r for r in rows if r['campo'] == 'cliente']
        assert len(cliente_rows) == 3
        # Orden cronológico
        valores = [r['valor_nuevo'] for r in cliente_rows]
        assert valores == ['V1', 'V2', 'V3']
        usuarios = [r['usuario'] for r in cliente_rows]
        assert usuarios == ['a@x.com', 'b@x.com', 'c@x.com']

    def test_campos_no_auditados_no_generan_fila(self):
        """Campos como 'mes', 'año', 'archivo_origen' NO están en la lista del trigger."""
        _update_with('admin', 'x@y.com', archivo_origen='OTRO.xlsx')
        rows = _audit_rows(TEST_MANIF)
        assert not any(r['campo'] == 'archivo_origen' for r in rows)

    def test_insert_no_audita(self):
        """El trigger es AFTER UPDATE — los INSERT no deben generar audit_log."""
        # El fixture _setup() ya hizo el INSERT y limpió audit_log.
        # Hacer un INSERT adicional (otro manifiesto) y verificar que no audita.
        otro = TEST_MANIF + 1
        conn = _conn(); conn.autocommit = False
        cur = conn.cursor()
        cur.execute("SET LOCAL request.jwt.claims = %s", (_claims_for(),))
        cur.execute("DELETE FROM audit_log WHERE manifiesto = %s", (otro,))
        cur.execute("DELETE FROM manifiestos_flat WHERE manifiesto = %s", (otro,))
        cur.execute("""
            INSERT INTO manifiestos_flat (
                manifiesto, archivo_origen, mes, año, periodo, semana,
                consecutivo_semanal, fecha_despacho, origen, departamento_origen,
                destino, departamento_destino, cliente, remesas,
                placa, tipo_vehiculo, conductor, cedula_conductor
            ) VALUES (
                %s,'TEST.xlsx','MAYO',2026,'2026-05-01','S20',1,
                '2026-05-01','BOGOTA','CUNDINAMARCA','CALI','VALLE DEL CAUCA',
                'CLIENTE INS','REM','ABC123','SENCILLO','CONDUCTOR INS','12345678'
            )
        """, (otro,))
        conn.commit()

        try:
            assert _audit_rows(otro) == [], "INSERT no debe generar entradas en audit_log"
        finally:
            cur.execute("DELETE FROM manifiestos_flat WHERE manifiesto = %s", (otro,))
            conn.commit()
            conn.close()

    def test_borrado_de_manifiesto_borra_audit_log_en_cascada(self):
        # Generar algunas entradas
        _update_with('admin', 'x@y.com', cliente='X1')
        assert len(_audit_rows(TEST_MANIF)) > 0

        # Borrar el manifiesto
        conn = _conn(); conn.autocommit = False
        cur = conn.cursor()
        cur.execute("SET LOCAL request.jwt.claims = %s", (_claims_for(),))
        cur.execute("DELETE FROM manifiestos_flat WHERE manifiesto = %s", (TEST_MANIF,))
        conn.commit()
        conn.close()

        assert _audit_rows(TEST_MANIF) == []


if __name__ == '__main__':
    sys.exit(pytest.main([__file__, '-v']))
