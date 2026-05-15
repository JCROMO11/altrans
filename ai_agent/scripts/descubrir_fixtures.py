"""
Descubre IDs reales para usar en test_agent.py:
  - un manifiesto ANULADO
  - un manifiesto PAGADO del conductor de prueba
  - otro conductor distinto del de prueba

Ejecutar desde ai_agent/:
    python scripts/descubrir_fixtures.py
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from db.queries import _get, TABLE
from scripts.test_agent import CONDUCTOR_CEDULA, CONDUCTOR_NOMBRE

def main():
    print("\n— Buscando un manifiesto ANULADO —")
    rows = _get(TABLE, {
        "estado_interno": "eq.ANULADO",
        "select":         "manifiesto,conductor,estado_interno",
        "limit":          "1",
    })
    if rows:
        print(f"  MANIFIESTO_ANULADO = {rows[0]['manifiesto']}  ({rows[0]['conductor']})")
    else:
        print("  Sin manifiestos anulados — no se pueden testear esos casos.")

    print(f"\n— Buscando un manifiesto PAGADO de {CONDUCTOR_NOMBRE} —")
    rows = _get(TABLE, {
        "cedula_conductor": f"eq.{CONDUCTOR_CEDULA}",
        "fecha_pago":       "not.is.null",
        "select":           "manifiesto,fecha_pago,valor_pagado,entidad_financiera",
        "limit":            "1",
        "order":            "fecha_pago.desc",
    })
    if rows:
        r = rows[0]
        print(f"  MANIFIESTO_PAGADO = {r['manifiesto']}  "
              f"(pagado {r['fecha_pago']} · ${r['valor_pagado']} · {r.get('entidad_financiera')})")
    else:
        print(f"  Sin manifiestos pagados para {CONDUCTOR_CEDULA}.")

    print(f"\n— Buscando OTRO conductor distinto de {CONDUCTOR_NOMBRE} —")
    rows = _get(TABLE, {
        "cedula_conductor": f"neq.{CONDUCTOR_CEDULA}",
        "estado_interno":   "neq.ANULADO",
        "select":           "conductor",
        "limit":            "1",
    })
    if rows:
        print(f'  OTRO_CONDUCTOR = "{rows[0]["conductor"]}"')
    else:
        print("  Sin otros conductores.")

    print("\nCopia los valores arriba a las variables al inicio de scripts/test_agent.py.")

if __name__ == "__main__":
    main()
