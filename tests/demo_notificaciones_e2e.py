"""
Demo E2E de notificaciones — 11 Agosto 2026
=============================================
Valida el flujo completo de auto-notify en Railway contra todos los
escenarios de plantillas, deduplicación y skip.

Escenarios cubiertos:
  PHASE 1  Insertar manifiestos de prueba 5001-5032 (celular=3145285119)
  PHASE 2  Verificar categorización del RPC get_pendientes_notificacion
  PHASE 3  Llamar POST /admin/auto-notify (Railway)
  PHASE 4  Verificar messages_sent: status sent/error por cada manifiesto
  PHASE 5  Llamada 2 → verificar deduplicación (no crea duplicados)
  PHASE 6  Verificar formateo de teléfono y fecha (_format_phone/_fmt)
  PHASE 7  Limpieza

Modos:
  python -m tests.demo_notificaciones_e2e --now            # dispara ya
  python -m tests.demo_notificaciones_e2e --schedule 16:25 # dispara a esa hora (Colombia)

Env:
  CELULAR_DEMO    (por defecto 3145285119)
  NOTIF_URL       (por defecto servicio Railway)
"""
import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=False)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SERVICE_KEY  = os.environ["SUPABASE_SERVICE_KEY"]
DATABASE_URL = os.environ["DATABASE_URL"]
ADMIN_TOKEN  = os.environ["ADMIN_TOKEN"]
NOTIF_URL    = os.environ.get("NOTIF_URL",
                              "https://notifications-altrans-production-5b04.up.railway.app")
CELULAR      = os.environ.get("CELULAR_DEMO", "3145285119")

PHONE = f"57{CELULAR}" if not CELULAR.startswith("57") else CELULAR

MIN, MAX = 5000, 5099  # rango de prueba (números < 21074)

TZ_COLOMBIA = timezone(timedelta(hours=-5))

NOTIF_HEADERS = {
    "x-admin-token": ADMIN_TOKEN,
    "Content-Type":  "application/json",
}

PASS = "✅"
FAIL = "❌"
WARN = "⚠️"


def _set_claims(cur, role="gerencia"):
    claims = json.dumps({
        "sub": "test", "role": "authenticated", "email": "test@altrans.local",
        "app_metadata": {"role": role},
    })
    cur.execute("SET LOCAL request.jwt.claims = %s", (claims,))


def clear_range() -> None:
    conn = psycopg2.connect(DATABASE_URL, connect_timeout=15)
    conn.autocommit = False
    cur = conn.cursor()
    _set_claims(cur)
    cur.execute("DELETE FROM messages_sent WHERE manifiesto BETWEEN %s AND %s", (MIN, MAX))
    cur.execute("DELETE FROM audit_log WHERE manifiesto BETWEEN %s AND %s", (MIN, MAX))
    cur.execute("DELETE FROM manifiestos_flat WHERE manifiesto BETWEEN %s AND %s", (MIN, MAX))
    conn.commit()
    conn.close()


def clear_messages_sent() -> None:
    conn = psycopg2.connect(DATABASE_URL, connect_timeout=15)
    conn.autocommit = False
    cur = conn.cursor()
    _set_claims(cur)
    cur.execute("DELETE FROM messages_sent WHERE manifiesto BETWEEN %s AND %s", (MIN, MAX))
    conn.commit()
    conn.close()


def insert_manifiesto(manif: int, **kw) -> None:
    defaults = {
        "manifiesto": manif,
        "archivo_origen": "TEST_E2E.xlsx",
        "mes": "AGOSTO", "año": 2026,
        "periodo": "2026-08-01", "semana": "Semana 32", "consecutivo_semanal": 1,
        "fecha_despacho": "2026-08-01",
        "origen": "CALI", "departamento_origen": "VALLE DEL CAUCA",
        "destino": "BOGOTA", "departamento_destino": "CUNDINAMARCA",
        "cliente": "TEST E2E", "remesas": "REM",
        "valor_remesa": 1000000, "flete_conductor": 500000, "anticipo": 100000,
        "placa": "TEST999",
        "conductor": "TEST E2E", "celular": CELULAR,
        "cedula_conductor": "12345678",
        "agencia_despachadora": "CALI", "estado_interno": "CUMPLIDO",
        "novedades": None, "factura_no": "F-TEST",
        "factura_electronica": "SI",
        "fecha_cumplido": None, "fecha_pago": None, "valor_pagado": None,
        "compromiso_pago": "PAGO A 15 DIAS",
    }
    defaults.update(kw)
    cols = ", ".join(defaults.keys())
    placeholders = ", ".join(["%s"] * len(defaults))
    values = list(defaults.values())

    conn = psycopg2.connect(DATABASE_URL, connect_timeout=15)
    conn.autocommit = False
    cur = conn.cursor()
    _set_claims(cur)
    cur.execute(f"INSERT INTO manifiestos_flat ({cols}) VALUES ({placeholders})", values)
    conn.commit()
    conn.close()


def update_manifiesto(manif: int, **kw) -> None:
    sets = ", ".join(f"{k} = %s" for k in kw)
    values = list(kw.values())
    conn = psycopg2.connect(DATABASE_URL, connect_timeout=15)
    conn.autocommit = False
    cur = conn.cursor()
    _set_claims(cur)
    cur.execute(f"UPDATE manifiestos_flat SET {sets} WHERE manifiesto = %s", values + [manif])
    conn.commit()
    conn.close()


def insert_pago_pending(manif: int, phone: str, sent_at: str | None = None) -> None:
    conn = psycopg2.connect(DATABASE_URL, connect_timeout=15)
    conn.autocommit = False
    cur = conn.cursor()
    _set_claims(cur)
    if sent_at:
        cur.execute(
            "INSERT INTO messages_sent (manifiesto, template_name, phone, status, sent_at) "
            "VALUES (%s, 'pago_realizado', %s, 'pending', %s)",
            (manif, phone, sent_at),
        )
    else:
        cur.execute(
            "INSERT INTO messages_sent (manifiesto, template_name, phone, status) "
            "VALUES (%s, 'pago_realizado', %s, 'pending')",
            (manif, phone),
        )
    conn.commit()
    conn.close()


def insert_sent(manif: int, template: str, phone: str, sent_at: str | None = None) -> None:
    conn = psycopg2.connect(DATABASE_URL, connect_timeout=15)
    conn.autocommit = False
    cur = conn.cursor()
    _set_claims(cur)
    if sent_at:
        cur.execute(
            "INSERT INTO messages_sent (manifiesto, template_name, phone, status, sent_at) "
            "VALUES (%s, %s, %s, 'sent', %s)",
            (manif, template, phone, sent_at),
        )
    else:
        cur.execute(
            "INSERT INTO messages_sent (manifiesto, template_name, phone, status) "
            "VALUES (%s, %s, %s, 'sent')",
            (manif, template, phone),
        )
    conn.commit()
    conn.close()


def get_msgs(manif: int) -> list:
    conn = psycopg2.connect(DATABASE_URL, connect_timeout=15)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("""
        SELECT id, manifiesto, template_name, phone, status, error, sent_at
        FROM messages_sent WHERE manifiesto = %s ORDER BY id
    """, (manif,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def rpc_pendientes() -> list:
    r = httpx.post(
        f"{SUPABASE_URL}/rest/v1/rpc/get_pendientes_notificacion",
        headers={
            "apikey":        SERVICE_KEY,
            "Authorization": f"Bearer {SERVICE_KEY}",
            "Content-Type":  "application/json",
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def call_auto_notify() -> dict:
    r = httpx.post(
        f"{NOTIF_URL}/admin/auto-notify",
        headers=NOTIF_HEADERS,
        json=None,
        timeout=60,
    )
    try:
        return r.json()
    except Exception:
        return {"raw": r.text, "status": r.status_code}


def step(label: str) -> None:
    print(f"\n{'='*62}")
    print(f"  {label}")
    print(f"{'='*62}")


def col(s: str, width: int = 20) -> str:
    return f"{s[:width]:<{width}}"


# ═══════════════════════════════════════════════════════════════════════════
# DATA SETUP
# ═══════════════════════════════════════════════════════════════════════════

def setup_data() -> None:
    hoy = datetime.now(TZ_COLOMBIA).date()
    d = lambda n: str(hoy - timedelta(days=n))

    clear_range()  # idempotente: limpia por si quedó data de una corrida anterior
    print(f"  Insertando data de prueba ({MIN}-{MAX})…")

    # ── saldo_novedad_pendiente ───────────────────────────────────────────
    insert_manifiesto(5001, novedades="AVERÍA EN MERCANCÍA",
                      factura_no="F5001", fecha_cumplido=d(5))
    insert_manifiesto(5005, novedades="REAJUSTE DE FLETE",
                      factura_no="F5005", fecha_cumplido=d(5))
    insert_manifiesto(5010, novedades="FALTANTE DE CAJAS",
                      factura_no="F5010", fecha_cumplido=d(5))

    # ── saldo_falta_factura ────────────────────────────────────────────────
    insert_manifiesto(5002, novedades=None, factura_no=None, fecha_cumplido=d(5))

    # ── saldo_falta_documentacion ──────────────────────────────────────────
    insert_manifiesto(5003, novedades=None, factura_no="F5003", fecha_cumplido=d(30))
    insert_manifiesto(5006, novedades=None, factura_no="F5006", fecha_cumplido=None)
    insert_manifiesto(5009, novedades=None, factura_no="F5009",
                      fecha_cumplido=d(25), compromiso_pago="PAGO A 20 DIAS")

    # ── saldo_plazo_vigente ────────────────────────────────────────────────
    insert_manifiesto(5004, novedades=None, factura_no="F5004", fecha_cumplido=d(5))
    insert_manifiesto(5007, novedades=None, factura_no="F5007",
                      fecha_cumplido=d(5), compromiso_pago="PAGO A 30 DIAS")
    insert_manifiesto(5008, novedades=None, factura_no="F5008",
                      fecha_cumplido=d(5), compromiso_pago="PAGO INMEDIATO")

    # ── pago_realizado (disparado por trigger trg_notify_pago_realizado) ───
    insert_manifiesto(5020, novedades=None, factura_no="F5020", fecha_cumplido=d(10))
    insert_manifiesto(5021, novedades=None, factura_no="F5021", fecha_cumplido=d(10))
    # 5022: sin valor_pagado → el trigger NO dispara (test de guardrail)
    insert_manifiesto(5022, novedades=None, factura_no="F5022", fecha_cumplido=d(10))

    # ── skip / noise cases ─────────────────────────────────────────────────
    # 5031: novedad ruido "TURBO" → RPC lo trata como no-novedad → plazo_vigente
    insert_manifiesto(5031, novedades="TURBO",
                      factura_no="F5031", fecha_cumplido=d(5))
    # 5032: novedad ruido "URBANOS" → no-novedad → plazo_vigente
    insert_manifiesto(5032, novedades="URBANOS",
                      factura_no="F5032", fecha_cumplido=d(5))
    # 5040: ya notificado (dedup) → no debe reaparecer
    insert_manifiesto(5040, novedades=None, factura_no="F5040", fecha_cumplido=d(5))

    # ── triggers: pago_realizado ───────────────────────────────────────────
    update_manifiesto(5020, fecha_pago=str(hoy), valor_pagado=800000)
    update_manifiesto(5021, fecha_pago=str(hoy - timedelta(days=2)), valor_pagado=450000)
    # 5022 queda sin fecha_pago/valor_pagado → no trigger

    # ── dedup: 5040 marcado como ya enviado hace 1 día ─────────────────────
    insert_sent(5040, "saldo_plazo_vigente", CELULAR,
                (datetime.now(TZ_COLOMBIA) - timedelta(days=1)).isoformat())

    # ── pago_realizado sin valor_pagado (para probar skip en Python) ───────
    # 5022 ya insertado arriba; solo creamos el pending manualmente
    insert_pago_pending(5022, CELULAR)

    print("  ✅ Data insertada")


# ═══════════════════════════════════════════════════════════════════════════
# VERIFICATIONS
# ═══════════════════════════════════════════════════════════════════════════

EXPECTED = {
    5001: "saldo_novedad_pendiente",
    5002: "saldo_falta_factura",
    5003: "saldo_falta_documentacion",
    5004: "saldo_plazo_vigente",
    5005: "saldo_novedad_pendiente",
    5006: "saldo_falta_documentacion",
    5007: "saldo_plazo_vigente",
    5008: "saldo_plazo_vigente",
    5009: "saldo_falta_documentacion",
    5010: "saldo_novedad_pendiente",
    5031: "saldo_plazo_vigente",   # TURBO → ruido → cae a plazo_vigente
    5032: "saldo_plazo_vigente",   # URBANOS → ruido → cae a plazo_vigente
}

EXPECTED_RPC = {
    **EXPECTED,
}


def verify_rpc() -> None:
    step("PHASE 2 — Verificar categorización del RPC")
    rows = rpc_pendientes()
    by_manif = {r["manifiesto"]: r for r in rows}
    all_ok = True
    for manif, expected in EXPECTED_RPC.items():
        got = by_manif.get(manif)
        if got is None:
            print(f"  {FAIL} manif {manif}: NO aparece (esperado {expected})")
            all_ok = False
        elif got["template_name"] != expected:
            print(f"  {FAIL} manif {manif}: {got['template_name']} (esperado {expected})")
            all_ok = False
        else:
            print(f"  {PASS} manif {manif}: {expected}")

    # skip cases: no deben aparecer (5040 ya notificado)
    for manif in (5040,):
        if manif in by_manif:
            print(f"  {FAIL} manif {manif}: NO debería aparecer en RPC")
            all_ok = False
        else:
            print(f"  {PASS} manif {manif}: correctamente excluido del RPC")

    # pago_realizado no viene del RPC
    if 5020 in by_manif or 5021 in by_manif:
        print(f"  {WARN} pago_realizado (5020/5021) no debería estar en RPC")

    print(f"\n  RPC: {'TODO OK' if all_ok else 'HUBO FALLOS'}")
    return all_ok


def verify_messages_sent(first_run: bool = True) -> bool:
    step("PHASE 4 — Verificar messages_sent")
    all_ok = True
    print(f"  {'manif':<6}{'template':<24}{'status':<8}{'error':<30}")
    print(f"  {'-'*6}{'-'*24}{'-'*8}{'-'*30}")

    seen = {}
    for manif in sorted([*EXPECTED.keys(), 5020, 5021, 5022, 5040]):
        rows = get_msgs(manif)
        seen[manif] = rows
        if not rows:
            print(f"  {col(str(manif),6)}{col('(ninguno)',24)}{col('—',8)}")
            continue
        for row in rows:
            _, m, tpl, phone, status, err, _ = row
            print(f"  {col(str(m),6)}{col(tpl,24)}{col(status,8)}{col(err or '',30)}")

    print()

    # EXPECTED: deben tener al menos un sent con el template correcto
    for manif, tpl in EXPECTED.items():
        rows = seen.get(manif) or []
        ok = any(r[4] == "sent" and r[2] == tpl for r in rows)
        if not ok:
            print(f"  {FAIL} manif {manif}: esperado {tpl} status sent, got {rows}")
            all_ok = False
        else:
            print(f"  {PASS} manif {manif}: {tpl} sent")

    # pago_realizado
    for manif in (5020, 5021):
        rows = seen.get(manif) or []
        ok = any(r[4] == "sent" and r[2] == "pago_realizado" for r in rows)
        if not ok:
            print(f"  {FAIL} manif {manif}: esperado pago_realizado sent, got {rows}")
            all_ok = False
        else:
            print(f"  {PASS} manif {manif}: pago_realizado sent")

    # 5022: pago_realizado sin valor_pagado → skip en Python.
    # (el manifiesto sí puede aparecer como saldo_plazo_vigente vía RPC)
    rows22 = seen.get(5022) or []
    pago22 = [r for r in rows22 if r[2] == "pago_realizado" and r[4] == "sent"]
    if pago22:
        print(f"  {FAIL} manif 5022: pago_realizado no debería haberse enviado (sin valor_pagado)")
        all_ok = False
    else:
        print(f"  {PASS} manif 5022: pago_realizado correctamente saltado (sin valor_pagado)")

    # 5040: ya notificado → no debe reaparecer
    rows40 = seen.get(5040) or []
    new40 = [r for r in rows40 if r[6] and r[6].date() == datetime.now(TZ_COLOMBIA).date()]
    if new40:
        print(f"  {FAIL} manif 5040: se reenvió a pesar de dedup: {new40}")
        all_ok = False
    else:
        print(f"  {PASS} manif 5040: dedup funciona (sin reenvío hoy)")

    print(f"\n  messages_sent: {'TODO OK' if all_ok else 'HUBO FALLOS'}")
    return all_ok


def verify_dedup() -> None:
    step("PHASE 5 — Deduplicación: 2da llamada no debe crear duplicados")
    before = {m: len(get_msgs(m)) for m in EXPECTED}
    result = call_auto_notify()
    print(f"  2da llamada → {json.dumps(result)}")
    time.sleep(3)
    after = {m: len(get_msgs(m)) for m in EXPECTED}
    dups = {m: (before[m], after[m]) for m in EXPECTED if after[m] > before[m]}
    if dups:
        print(f"  {FAIL} Duplicados creados: {dups}")
    else:
        print(f"  {PASS} No se crearon duplicados en la 2da llamada")


# ═══════════════════════════════════════════════════════════════════════════
# PHONE / DATE FORMAT (unit checks — reimplementa la lógica del servicio)
# ═══════════════════════════════════════════════════════════════════════════

_SPANISH_MONTHS = {
    1: "enero", 2: "febrero", 3: "marzo", 4: "abril",
    5: "mayo", 6: "junio", 7: "julio", 8: "agosto",
    9: "septiembre", 10: "octubre", 11: "noviembre", 12: "diciembre",
}


def _fmt(d: str | None) -> str | None:
    if not d:
        return None
    try:
        dt = datetime.strptime(d, "%Y-%m-%d")
        mes = _SPANISH_MONTHS.get(dt.month, str(dt.month))
        return f"{dt.day} de {mes} de {dt.year}"
    except ValueError:
        return d


def _format_phone(raw: str) -> str:
    cleaned = re.sub(r"[\+\-\s\(\)]", "", raw)
    if len(cleaned) == 10 and cleaned.isdigit():
        return f"57{cleaned}"
    if len(cleaned) == 12 and cleaned.startswith("57") and cleaned.isdigit():
        return cleaned
    if len(cleaned) > 10 and cleaned.isdigit():
        return cleaned
    return raw


def verify_formatting() -> None:
    step("PHASE 6 — Formateo de teléfono y fecha (reimplementa lógica del servicio)")
    ok = True

    phone_cases = [
        ("3001111111", "573001111111"),
        ("573001111111", "573001111111"),
        ("+57 300 111 1111", "573001111111"),
        ("(300) 111-1111", "573001111111"),
        ("573001234567", "573001234567"),
        ("300", "300"),
        ("", ""),
    ]
    for raw, expected in phone_cases:
        got = _format_phone(raw)
        status = PASS if got == expected else FAIL
        if got != expected:
            ok = False
        print(f"  {status} _format_phone({raw!r:22s}) → {got!r} (esp {expected!r})")

    date_cases = [
        ("2026-07-17", "17 de julio de 2026"),
        ("2026-01-05", "5 de enero de 2026"),
        ("2026-12-25", "25 de diciembre de 2026"),
        (None, None),
        ("no-es-fecha", "no-es-fecha"),
    ]
    for raw, expected in date_cases:
        got = _fmt(raw)
        status = PASS if got == expected else FAIL
        if got != expected:
            ok = False
        print(f"  {status} _fmt({raw!r:16s}) → {got!r} (esp {expected!r})")

    print(f"\n  Formateo: {'TODO OK' if ok else 'HUBO FALLOS'}")
    return ok


# ═══════════════════════════════════════════════════════════════════════════
# MAIN FLOW
# ═══════════════════════════════════════════════════════════════════════════

def run_e2e() -> None:
    print(f"\n🔔 E2E Notificaciones — {datetime.now(TZ_COLOMBIA):%Y-%m-%d %H:%M} Colombia")
    print(f"   Celular destino: {CELULAR} ({PHONE})")
    print(f"   Servicio: {NOTIF_URL}")

    step("PHASE 1 — Setup: insertar datos de prueba")
    setup_data()

    verify_rpc()

    step("PHASE 3 — Ejecutar auto-notify")
    result = call_auto_notify()
    print(f"  → {json.dumps(result)}")
    time.sleep(5)

    verify_messages_sent()

    verify_dedup()

    verify_formatting()

    print(f"\n{'='*62}")
    print("  E2E completado")
    print(f"{'='*62}")


def main() -> None:
    parser = argparse.ArgumentParser(description="E2E de notificaciones")
    parser.add_argument("--now", action="store_true",
                        help="Ejecutar la ronda completa ahora")
    parser.add_argument("--schedule", metavar="HH:MM",
                        help="Ejecutar a esa hora (Colombia, UTC-5)")
    parser.add_argument("--cleanup", action="store_true",
                        help="Borrar datos de prueba y salir")
    parser.add_argument("--dry-run", action="store_true",
                        help="Solo insertar data, verificar RPC y formateo, sin auto-notify")
    args = parser.parse_args()

    if args.cleanup:
        step("CLEANUP")
        clear_range()
        print("  ✅ Rango 5000-5099 limpiado")
        return

    if args.dry_run:
        step("DRY RUN — solo setup + verificación local")
        setup_data()
        verify_rpc()
        verify_formatting()
        print("\n  (sin llamar a auto-notify)")
        return

    if args.schedule:
        now = datetime.now(TZ_COLOMBIA)
        hh, mm = map(int, args.schedule.split(":"))
        target = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if target <= now:
            print(f"{WARN} Hora {args.schedule} ya pasó; programando para mañana a esa hora")
            target += timedelta(days=1)
        seconds = (target - now).total_seconds()
        print(f"\n⏰ Programado para {target:%Y-%m-%d %H:%M} Colombia "
              f"({seconds/60:.1f} min desde ahora)")
        print("   Insertando data de prueba ahora…")
        clear_range()
        setup_data()
        print(f"   Durmiendo {seconds/60:.1f} min hasta {target:%H:%M}…")
        time.sleep(seconds)
        print(f"\n🔔 Desperté a {datetime.now(TZ_COLOMBIA):%H:%M:%S} — ejecutando auto-notify")
        step("PHASE 3 — Ejecutar auto-notify")
        result = call_auto_notify()
        print(f"  → {json.dumps(result)}")
        time.sleep(5)
        verify_messages_sent()
        print(f"\n{'='*62}")
        print("  🎯 E2E programado completado — revisa WhatsApp")
        print(f"{'='*62}")
        return

    # default: --now
    run_e2e()


if __name__ == "__main__":
    main()
