"""
Setup de datos ficticios para probar notificaciones WhatsApp.

Inserta manifiestos de prueba apuntando a un celular de destino y cubre los
5 escenarios de plantilla:
  saldo_falta_factura, saldo_falta_documentacion, saldo_novedad_pendiente,
  saldo_plazo_vigente, pago_realizado

Uso (CLI):
  python -m tests.setup_datos_prueba --rango 6001              # inserta los 5 escenarios (6001-6005)
  python -m tests.setup_datos_prueba --full 7001               # inserta 20 manifiestos (4 por plantilla)
  python -m tests.setup_datos_prueba --cleanup --rango 6001    # borra el rango (manifiestos, messages_sent, audit_log)
  python -m tests.setup_datos_prueba --manifiesto 6001 --solo saldo_falta_factura

También exporta funciones importables (las usa orquestar_prueba_escalonada.py):
  insert_scenario(manifiesto, escenario, celular)
  clear_range(minimo, maximo)
  clear_manifiesto(manifiesto)
  get_msgs(manifiesto)

Env: CELULAR_DEMO (default 3145285119), DATABASE_URL
"""
import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=False)

DATABASE_URL = os.environ["DATABASE_URL"]
CELULAR = os.environ.get("CELULAR_DEMO", "3145285119")

TZ_COLOMBIA = timezone(timedelta(hours=-5))

# Orden de escenarios usado por --rango (un manifiesto consecutivo por plantilla)
SCENARIO_ORDER = [
    "saldo_falta_factura",
    "saldo_falta_documentacion",
    "saldo_novedad_pendiente",
    "saldo_plazo_vigente",
    "pago_realizado",
]

# Orden de producción (pago_realizado primero, luego saldos) usado por --full.
FULL_TEMPLATE_ORDER = [
    "pago_realizado",
    "saldo_falta_factura",
    "saldo_falta_documentacion",
    "saldo_novedad_pendiente",
    "saldo_plazo_vigente",
]

FULL_PER_TEMPLATE = 4  # 4 manifiestos por plantilla → 20 manifiestos en total


def _set_claims(cur, role="gerencia"):
    claims = json.dumps({
        "sub": "test", "role": "authenticated", "email": "test@altrans.local",
        "app_metadata": {"role": role},
    })
    cur.execute("SET LOCAL request.jwt.claims = %s", (claims,))


def _conn():
    conn = psycopg2.connect(DATABASE_URL, connect_timeout=15)
    return conn


def _base_defaults(manifiesto: int, celular: str) -> dict:
    return {
        "manifiesto": manifiesto,
        "archivo_origen": "TEST_NOTIF.xlsx",
        "mes": "AGOSTO", "año": 2026,
        "periodo": "2026-08-01", "semana": "Semana 32", "consecutivo_semanal": 1,
        "fecha_despacho": "2026-08-01",
        "origen": "CALI", "departamento_origen": "VALLE DEL CAUCA",
        "destino": "BOGOTA", "departamento_destino": "CUNDINAMARCA",
        "cliente": "TEST NOTIF", "remesas": "REM",
        "valor_remesa": 1000000, "flete_conductor": 500000, "anticipo": 100000,
        "placa": "TEST999",
        "conductor": "TEST NOTIF", "celular": celular,
        "cedula_conductor": "12345678",
        "agencia_despachadora": "CALI", "estado_interno": "CUMPLIDO",
        "novedades": None, "factura_no": "F-TEST",
        "factura_electronica": "SI",
        "fecha_cumplido": None, "fecha_pago": None, "valor_pagado": None,
        "compromiso_pago": "PAGO A 15 DIAS",
    }


def _d(days: int) -> str:
    return str(datetime.now(TZ_COLOMBIA).date() - timedelta(days=days))


def _insert(manifiesto: int, celular: str, **kw) -> None:
    data = _base_defaults(manifiesto, celular)
    data.update(kw)
    cols = ", ".join(data.keys())
    placeholders = ", ".join(["%s"] * len(data))
    values = list(data.values())
    conn = _conn()
    conn.autocommit = False
    try:
        cur = conn.cursor()
        _set_claims(cur)
        cur.execute(f"INSERT INTO manifiestos_flat ({cols}) VALUES ({placeholders})", values)
        conn.commit()
    finally:
        conn.close()


def _update(manifiesto: int, **kw) -> None:
    sets = ", ".join(f"{k} = %s" for k in kw)
    values = list(kw.values())
    conn = _conn()
    conn.autocommit = False
    try:
        cur = conn.cursor()
        _set_claims(cur)
        cur.execute(f"UPDATE manifiestos_flat SET {sets} WHERE manifiesto = %s", values + [manifiesto])
        conn.commit()
    finally:
        conn.close()


def insert_scenario(manifiesto: int, escenario: str, celular: str | None = None) -> None:
    """Inserta un manifiesto de prueba en el escenario de plantilla indicado."""
    cel = celular or CELULAR
    hoy = datetime.now(TZ_COLOMBIA).date()

    if escenario == "saldo_falta_factura":
        _insert(manifiesto, cel, factura_no=None, novedades=None, fecha_cumplido=_d(5))
    elif escenario == "saldo_falta_documentacion":
        _insert(manifiesto, cel, factura_no=f"F{manifiesto}", novedades=None, fecha_cumplido=_d(30))
    elif escenario == "saldo_novedad_pendiente":
        _insert(manifiesto, cel, factura_no=f"F{manifiesto}",
                novedades="AVERÍA EN MERCANCÍA", fecha_cumplido=_d(5))
    elif escenario == "saldo_plazo_vigente":
        _insert(manifiesto, cel, factura_no=f"F{manifiesto}", novedades=None, fecha_cumplido=_d(5))
    elif escenario == "pago_realizado":
        _insert(manifiesto, cel, factura_no=f"F{manifiesto}", novedades=None, fecha_cumplido=_d(10))
        # Marcar fecha_pago + valor_pagado dispara trg_notify_pago_realizado,
        # que crea el registro pending en messages_sent.
        _update(manifiesto, fecha_pago=str(hoy), valor_pagado=800000)
    else:
        raise ValueError(f"Escenario desconocido: {escenario}")


def insert_full(base: int, celular: str | None = None) -> dict[int, str]:
    """Inserta 20 manifiestos (4 por plantilla) y devuelve el mapeo manifiesto → plantilla.

    Distribución (base..base+19) en orden de producción:
      base+0..3   pago_realizado
      base+4..7   saldo_falta_factura
      base+8..11  saldo_falta_documentacion
      base+12..15 saldo_novedad_pendiente
      base+16..19 saldo_plazo_vigente
    """
    cel = celular or CELULAR
    mapping: dict[int, str] = {}
    idx = 0
    for template in FULL_TEMPLATE_ORDER:
        for _ in range(FULL_PER_TEMPLATE):
            manif = base + idx
            insert_scenario(manif, template, cel)
            mapping[manif] = template
            idx += 1
    return mapping


def print_mapping(mapping: dict[int, str]) -> None:
    """Imprime una tabla legible manifiesto → plantilla."""
    print(f"\n{'Manifiesto':<14}{'Plantilla'}")
    print("-" * 40)
    for manif in sorted(mapping):
        print(f"{manif:<14}{mapping[manif]}")


def clear_manifiesto(manifiesto: int) -> None:
    conn = _conn()
    conn.autocommit = False
    try:
        cur = conn.cursor()
        _set_claims(cur)
        cur.execute("DELETE FROM messages_sent WHERE manifiesto = %s", (manifiesto,))
        cur.execute("DELETE FROM audit_log WHERE manifiesto = %s", (manifiesto,))
        cur.execute("DELETE FROM manifiestos_flat WHERE manifiesto = %s", (manifiesto,))
        conn.commit()
    finally:
        conn.close()


def clear_range(minimo: int, maximo: int) -> None:
    conn = _conn()
    conn.autocommit = False
    try:
        cur = conn.cursor()
        _set_claims(cur)
        cur.execute("DELETE FROM messages_sent WHERE manifiesto BETWEEN %s AND %s", (minimo, maximo))
        cur.execute("DELETE FROM audit_log WHERE manifiesto BETWEEN %s AND %s", (minimo, maximo))
        cur.execute("DELETE FROM manifiestos_flat WHERE manifiesto BETWEEN %s AND %s", (minimo, maximo))
        conn.commit()
    finally:
        conn.close()


def get_msgs(manifiesto: int) -> list:
    conn = _conn()
    conn.autocommit = True
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, manifiesto, template_name, phone, status, error, sent_at
            FROM messages_sent WHERE manifiesto = %s ORDER BY id
        """, (manifiesto,))
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Setup de datos ficticios de notificaciones")
    parser.add_argument("--rango", type=int, metavar="NNNN",
                        help="Manifiesto base: inserta los 5 escenarios en NNNN..NNNN+4")
    parser.add_argument("--full", type=int, metavar="NNNN",
                        help="Manifiesto base: inserta 20 manifiestos (4 por plantilla) en NNNN..NNNN+19")
    parser.add_argument("--manifiesto", type=int, metavar="N", help="Manifiesto único (con --solo)")
    parser.add_argument("--solo", choices=SCENARIO_ORDER, help="Insertar un único escenario")
    parser.add_argument("--cleanup", action="store_true", help="Borrar el rango/manifiesto y salir")
    parser.add_argument("--celular", default=CELULAR, help="Celular destino (default 3145285119)")
    args = parser.parse_args()

    if args.cleanup:
        if args.manifiesto:
            clear_manifiesto(args.manifiesto)
            print(f"✅ Limpiado manifiesto {args.manifiesto}")
        elif args.full:
            clear_range(args.full, args.full + len(FULL_TEMPLATE_ORDER) * FULL_PER_TEMPLATE - 1)
            print(f"✅ Limpiado rango {args.full}-{args.full + 19}")
        elif args.rango:
            clear_range(args.rango, args.rango + 4)
            print(f"✅ Limpiado rango {args.rango}-{args.rango + 4}")
        else:
            print("❌ Indica --full, --rango o --manifiesto con --cleanup")
        return

    if args.solo:
        if not args.manifiesto:
            print("❌ --solo requiere --manifiesto N")
            return
        insert_scenario(args.manifiesto, args.solo, args.celular)
        print(f"✅ Insertado manifiesto {args.manifiesto} → {args.solo}")
        return

    if args.full:
        mapping = insert_full(args.full, args.celular)
        for manif in sorted(mapping):
            print(f"✅ Insertado manifiesto {manif} → {mapping[manif]}")
        print_mapping(mapping)
        return

    if args.rango:
        base = args.rango
        for i, esc in enumerate(SCENARIO_ORDER):
            insert_scenario(base + i, esc, args.celular)
            print(f"✅ Insertado manifiesto {base + i} → {esc}")
        return

    parser.print_help()


if __name__ == "__main__":
    main()
