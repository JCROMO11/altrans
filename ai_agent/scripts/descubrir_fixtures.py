"""
Descubre IDs reales para usar en test_agent.py y test_webhook.py:
  - un manifiesto ANULADO
  - un manifiesto PAGADO del conductor de prueba
  - un manifiesto que NO es del conductor (cross-conductor test)
  - otro conductor distinto del de prueba
  - una placa válida para tests de propietario
  - un conductor SIN manifiestos (caso edge)

Ejecutar desde ai_agent/:
    python scripts/descubrir_fixtures.py
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from db.queries import _get, TABLE, VIEW
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
        "select":           "conductor,cedula_conductor",
        "limit":            "1",
    })
    if rows:
        print(f'  OTRO_CONDUCTOR = "{rows[0]["conductor"]}"')
        otro_cedula = rows[0]["cedula_conductor"]
    else:
        print("  Sin otros conductores.")
        otro_cedula = None

    if otro_cedula:
        print(f"\n— Buscando un manifiesto de OTRO conductor (para test cross-conductor) —")
        rows = _get(TABLE, {
            "cedula_conductor": f"eq.{otro_cedula}",
            "estado_interno":   "neq.ANULADO",
            "select":           "manifiesto,conductor",
            "limit":            "1",
        })
        if rows:
            print(f"  MANIFIESTO_OTRO_CONDUCTOR = {rows[0]['manifiesto']}  ({rows[0]['conductor']})")
        else:
            print("  Sin manifiestos del otro conductor.")

    print(f"\n— Buscando una placa con varios manifiestos (para test de propietario) —")
    rows = _get(TABLE, {
        "placa":          "not.is.null",
        "estado_interno": "neq.ANULADO",
        "select":         "placa,propietario,manifiesto",
        "limit":          "1",
    })
    if rows:
        prop_nombre = rows[0].get('propietario') or "DESCONOCIDO"
        print(f"  PLACA_TEST         = \"{rows[0]['placa']}\"")
        print(f"  PROPIETARIO_NOMBRE = \"{prop_nombre}\"")
        print(f"  MANIFIESTO_PLACA   = {rows[0]['manifiesto']}")
    else:
        print("  Sin placas registradas.")

    # ── Fixtures de compromiso_pago ───────────────────────────────────────────
    _COMPROMISOS = [
        ("PAGO A 15 DIAS",        "MANIFIESTO_PAGO_15"),
        ("PAGO A 20 DIAS",        "MANIFIESTO_PAGO_20"),
        ("PAGO A 30 DIAS",        "MANIFIESTO_PAGO_30"),
        ("PAGO A 5-8 DIAS",       "MANIFIESTO_PAGO_5_8"),
        ("PAGO INMEDIATO",        "MANIFIESTO_INMEDIATO"),
        ("CONTRAENTREGA",         "MANIFIESTO_CONTRAENTREGA"),
        ("CONTINGENCIA 20-25 DH", "MANIFIESTO_CONTINGENCIA"),
        ("URBANO",                "MANIFIESTO_URBANO"),
        ("OTROS",                 "MANIFIESTO_OTROS"),
    ]
    for compromiso, var in _COMPROMISOS:
        print(f"\n— Buscando manifiesto pendiente con compromiso_pago = {compromiso} —")
        rows = _get(VIEW, {
            "compromiso_pago": f"eq.{compromiso}",
            "estado_interno":  "neq.ANULADO",
            "fecha_pago":      "is.null",
            "fecha_cumplido":  "not.is.null",
            "select":          "manifiesto,conductor,compromiso_pago,fecha_cumplido,fecha_estimada_pago",
            "limit":           "1",
        })
        if rows:
            r = rows[0]
            print(f"  {var} = {r['manifiesto']}  "
                  f"({r['conductor']} · cumplido {r.get('fecha_cumplido')} · estimado {r.get('fecha_estimada_pago')})")
        else:
            print(f"  Sin manifiestos con '{compromiso}' pendientes y con fecha_cumplido.")

    print(f"\n— Buscando manifiesto pendiente sin compromiso_pago (NULL) —")
    rows = _get(VIEW, {
        "compromiso_pago": "is.null",
        "estado_interno":  "neq.ANULADO",
        "fecha_pago":      "is.null",
        "fecha_cumplido":  "not.is.null",
        "select":          "manifiesto,conductor,fecha_cumplido,fecha_estimada_pago",
        "limit":           "1",
    })
    if rows:
        r = rows[0]
        print(f"  MANIFIESTO_SIN_COMPROMISO = {r['manifiesto']}  "
              f"({r['conductor']} · cumplido {r.get('fecha_cumplido')} · estimado {r.get('fecha_estimada_pago')})")
    else:
        print("  Sin manifiestos sin compromiso pendientes con fecha_cumplido.")

    print(f"\n— Buscando manifiesto SIN fecha_cumplido (viaje no cerrado) —")
    rows = _get(VIEW, {
        "estado_interno": "neq.ANULADO",
        "fecha_pago":     "is.null",
        "fecha_cumplido": "is.null",
        "select":         "manifiesto,conductor,estado_interno,compromiso_pago",
        "limit":          "1",
    })
    if rows:
        r = rows[0]
        print(f"  MANIFIESTO_SIN_CUMPLIDO = {r['manifiesto']}  "
              f"({r['conductor']} · estado {r.get('estado_interno')} · compromiso {r.get('compromiso_pago')})")
    else:
        print("  Sin manifiestos sin fecha_cumplido.")

    print(f"\n— Buscando manifiesto pendiente con compromiso_pago = PRIORITARIO —")
    rows = _get(VIEW, {
        "compromiso_pago": "eq.PRIORITARIO",
        "estado_interno":  "neq.ANULADO",
        "fecha_pago":      "is.null",
        "fecha_cumplido":  "not.is.null",
        "select":          "manifiesto,conductor,compromiso_pago,fecha_cumplido,fecha_estimada_pago",
        "limit":           "1",
    })
    if rows:
        r = rows[0]
        print(f"  MANIFIESTO_PRIORITARIO = {r['manifiesto']}  "
              f"({r['conductor']} · cumplido {r.get('fecha_cumplido')} · estimado {r.get('fecha_estimada_pago')})")
    else:
        print("  Sin manifiestos PRIORITARIO pendientes con fecha_cumplido.")

    print(f"\n— Buscando manifiesto pendiente con compromiso_pago = PRONTO PAGO —")
    rows = _get(VIEW, {
        "compromiso_pago": "eq.PRONTO PAGO",
        "estado_interno":  "neq.ANULADO",
        "fecha_pago":      "is.null",
        "fecha_cumplido":  "not.is.null",
        "select":          "manifiesto,conductor,compromiso_pago,fecha_cumplido",
        "limit":           "1",
    })
    if rows:
        r = rows[0]
        print(f"  MANIFIESTO_PRONTO_PAGO = {r['manifiesto']}  "
              f"({r['conductor']} · cumplido {r.get('fecha_cumplido')})")
    else:
        print("  Sin manifiestos PRONTO PAGO pendientes con fecha_cumplido.")

    print("\nCopia los valores arriba a las variables al inicio de scripts/test_agent.py.")

if __name__ == "__main__":
    main()
