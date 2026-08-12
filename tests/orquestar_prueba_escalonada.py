"""
Orquestación de prueba escalonada de notificaciones WhatsApp.

Inserta un manifiesto por escenario en momentos escalonados (cada N minutos)
y dispara POST /admin/auto-notify tras cada inserción, para ver llegar cada
plantilla por separado y comprobar que el sistema distingue cada escenario
bajo sus propias condiciones.

Secuencia por defecto:
  13:00 saldo_falta_factura
  13:10 saldo_falta_documentacion
  13:20 saldo_novedad_pendiente
  13:30 saldo_plazo_vigente
  13:40 pago_realizado

Uso:
  python -m tests.orquestar_prueba_escalonada --rango 6001 --start 13:00 --interval 10
  python -m tests.orquestar_prueba_escalonada --rango 6001 --cleanup --dry-run

Env: NOTIF_URL, ADMIN_TOKEN, DATABASE_URL, CELULAR_DEMO
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=False)

from tests.setup_datos_prueba import (
    SCENARIO_ORDER, clear_range, get_msgs, insert_scenario,
)

DATABASE_URL = os.environ["DATABASE_URL"]
ADMIN_TOKEN = os.environ["ADMIN_TOKEN"]
NOTIF_URL = os.environ.get(
    "NOTIF_URL", "https://notifications-altrans-production-5b04.up.railway.app"
)
CELULAR = os.environ.get("CELULAR_DEMO", "3145285119")

TZ_COLOMBIA = timezone(timedelta(hours=-5))

NOTIF_HEADERS = {"x-admin-token": ADMIN_TOKEN, "Content-Type": "application/json"}


def call_auto_notify() -> dict:
    r = httpx.post(f"{NOTIF_URL}/admin/auto-notify", headers=NOTIF_HEADERS, timeout=60)
    try:
        return r.json()
    except Exception:
        return {"raw": r.text, "status": r.status_code}


def _target_times(start: str, interval_min: int) -> list[datetime]:
    now = datetime.now(TZ_COLOMBIA)
    hh, mm = map(int, start.split(":"))
    first = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
    if first <= now:
        first += timedelta(days=1)
    return [first + timedelta(minutes=interval_min * i) for i in range(len(SCENARIO_ORDER))]


def _sleep_until(target: datetime) -> None:
    now = datetime.now(TZ_COLOMBIA)
    seconds = (target - now).total_seconds()
    if seconds <= 0:
        return
    print(f"   ⏳ Esperando {seconds/60:.1f} min hasta {target:%H:%M:%S} …")
    time.sleep(seconds)


def run(base: int, start: str, interval_min: int) -> None:
    targets = _target_times(start, interval_min)
    print(f"\n{'='*62}")
    print(f"  Prueba escalonada — {datetime.now(TZ_COLOMBIA):%Y-%m-%d %H:%M} Colombia")
    print(f"  Celular: {CELULAR} | Servicio: {NOTIF_URL}")
    print(f"{'='*62}")
    for i, esc in enumerate(SCENARIO_ORDER):
        manif = base + i
        print(f"  [{i+1}/5] {targets[i]:%H:%M} → manifiesto {manif} ({esc})")

    for i, esc in enumerate(SCENARIO_ORDER):
        manif = base + i
        target = targets[i]
        print(f"\n{'-'*62}")
        print(f"  {target:%H:%M} — Escenario {i+1}/5: {esc} (manifiesto {manif})")
        print(f"{'-'*62}")
        _sleep_until(target)

        insert_scenario(manif, esc, CELULAR)
        print(f"   ✅ Manifiesto {manif} insertado ({esc})")

        result = call_auto_notify()
        print(f"   → auto-notify: {json.dumps(result)}")
        time.sleep(6)

        rows = get_msgs(manif)
        if not rows:
            print(f"   ⚠️  Sin registro en messages_sent para {manif}")
        for row in rows:
            _, m, tpl, phone, status, err, _ = row
            print(f"   messages_sent: {tpl:<24} status={status:<6} phone={phone}"
                  + (f" error={err}" if err else ""))

    print(f"\n{'='*62}")
    print("  ✅ Prueba escalonada completada — revisa WhatsApp")
    print(f"{'='*62}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Prueba escalonada de notificaciones")
    parser.add_argument("--rango", type=int, required=True, metavar="NNNN",
                        help="Manifiesto base (usa NNNN..NNNN+4)")
    parser.add_argument("--start", default="13:00", metavar="HH:MM",
                        help="Hora del primer envío (Colombia). Default 13:00")
    parser.add_argument("--interval", type=int, default=10, metavar="MIN",
                        help="Minutos entre envíos. Default 10")
    parser.add_argument("--cleanup", action="store_true",
                        help="Limpiar el rango antes de empezar")
    parser.add_argument("--dry-run", action="store_true",
                        help="Solo imprimir el plan, sin insertar ni enviar")
    args = parser.parse_args()

    targets = _target_times(args.start, args.interval)

    if args.dry_run:
        print("Plan (dry-run):")
        for i, esc in enumerate(SCENARIO_ORDER):
            print(f"  {targets[i]:%Y-%m-%d %H:%M} → manifiesto {args.rango + i} ({esc})")
        return

    if args.cleanup:
        clear_range(args.rango, args.rango + 4)
        print(f"🧹 Rango {args.rango}-{args.rango + 4} limpiado")

    run(args.rango, args.start, args.interval)


if __name__ == "__main__":
    main()
