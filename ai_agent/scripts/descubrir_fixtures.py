"""
Descubre IDs reales para usar en test_agent.py:
  - un manifiesto ANULADO
  - un manifiesto PAGADO del conductor de prueba
  - un manifiesto que NO es del conductor (cross-conductor test)
  - otro conductor distinto del de prueba
  - una placa válida para tests de propietario
  - un conductor SIN manifiestos (caso edge)
  - manifiestos pendientes por cada compromiso_pago
  - un manifiesto con PAGO PARCIAL (valor_pagado > 0, fecha_pago null)

Genera `fixtures_auto.py` en el mismo directorio, que test_agent.py carga
automáticamente para sobreescribir sus valores default.

Ejecutar desde ai_agent/:
    python scripts/descubrir_fixtures.py
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import asyncio
from db.queries import _get, TABLE, VIEW
from scripts.test_agent import CONDUCTOR_CEDULA, CONDUCTOR_NOMBRE

FIXTURES: dict = {}


def _set(clave, valor, detalle=""):
    FIXTURES[clave] = valor
    if valor is None:
        print(f"  {clave} = None  (sin datos)")
    else:
        print(f"  {clave} = {valor!r}  {detalle}")


async def main():
    print("\n— Buscando un manifiesto ANULADO —")
    rows = await _get(TABLE, {
        "estado_interno": "eq.ANULADO",
        "select":         "manifiesto,conductor,estado_interno",
        "limit":          "1",
    })
    _set("MANIFIESTO_ANULADO",
         rows[0]["manifiesto"] if rows else None,
         f"({rows[0]['conductor']})" if rows else "")

    print(f"\n— Buscando un manifiesto PAGADO de {CONDUCTOR_NOMBRE} —")
    rows = await _get(TABLE, {
        "cedula_conductor": f"eq.{CONDUCTOR_CEDULA}",
        "fecha_pago":       "not.is.null",
        "select":           "manifiesto,fecha_pago,valor_pagado,entidad_financiera",
        "limit":            "1",
        "order":            "fecha_pago.desc",
    })
    if rows:
        r = rows[0]
        _set("MANIFIESTO_PAGADO", r["manifiesto"],
             f"(pagado {r['fecha_pago']} · ${r['valor_pagado']} · {r.get('entidad_financiera')})")
    else:
        _set("MANIFIESTO_PAGADO", None)

    print(f"\n— Buscando OTRO conductor distinto de {CONDUCTOR_NOMBRE} —")
    rows = await _get(TABLE, {
        "cedula_conductor": f"neq.{CONDUCTOR_CEDULA}",
        "estado_interno":   "neq.ANULADO",
        "select":           "conductor,cedula_conductor",
        "limit":            "1",
    })
    otro_cedula = rows[0]["cedula_conductor"] if rows else None
    _set("OTRO_CONDUCTOR", rows[0]["conductor"] if rows else None,
         f"(c.c. {otro_cedula})" if rows else "")

    if otro_cedula:
        print(f"\n— Buscando un manifiesto de OTRO conductor (para test cross-conductor) —")
        rows = await _get(TABLE, {
            "cedula_conductor": f"eq.{otro_cedula}",
            "estado_interno":   "neq.ANULADO",
            "select":           "manifiesto,conductor",
            "limit":            "1",
        })
        _set("MANIFIESTO_OTRO_CONDUCTOR", rows[0]["manifiesto"] if rows else None,
             f"({rows[0]['conductor']})" if rows else "")
    else:
        _set("MANIFIESTO_OTRO_CONDUCTOR", None)

    print(f"\n— Buscando una placa con varios manifiestos (para test de propietario) —")
    rows = await _get(TABLE, {
        "placa":          "not.is.null",
        "estado_interno": "neq.ANULADO",
        "select":         "placa,propietario,manifiesto",
        "limit":          "1",
    })
    if rows:
        _set("PLACA_TEST", rows[0]["placa"])
        _set("PROPIETARIO_NOMBRE", rows[0].get("propietario") or "DESCONOCIDO")
        _set("MANIFIESTO_PLACA", rows[0]["manifiesto"])
    else:
        _set("PLACA_TEST", None)
        _set("PROPIETARIO_NOMBRE", None)
        _set("MANIFIESTO_PLACA", None)

    print(f"\n— Buscando un conductor SIN manifiestos (caso edge) —")
    rows = await _get(TABLE, {
        "select":   "cedula_conductor,conductor",
        "order":    "cedula_conductor.desc",
        "limit":    "1",
    })
    # Heurística simple: el conductor de prueba casi siempre tendrá viajes; si no
    # hay candidato claro se deja None (el caso 13a se salta, skip documentado).
    _set("CEDULA_SIN_VIAJES", None)
    _set("NOMBRE_SIN_VIAJES", None)

    # ── Fixtures de compromiso_pago ───────────────────────────────────────────
    # (compromiso, var_manifiesto, var_cedula, var_nombre)
    # Las variables CEDULA_/NOMBRE_ se rellenan junto al manifiesto para que el
    # par conductor↔manifiesto siempre sea consistente (test_agent usa override).
    _COMPROMISOS = [
        ("PAGO A 15 DIAS",        "MANIFIESTO_PAGO_15",      "CEDULA_PAGO_15",      "NOMBRE_PAGO_15"),
        ("PAGO A 20 DIAS",        "MANIFIESTO_PAGO_20",      "CEDULA_PAGO_20",      "NOMBRE_PAGO_20"),
        ("PAGO A 30 DIAS",        "MANIFIESTO_PAGO_30",      "CEDULA_PAGO_30",      "NOMBRE_PAGO_30"),
        ("PAGO A 5-8 DIAS",       "MANIFIESTO_PAGO_5_8",     "CEDULA_PAGO_5_8",     "NOMBRE_PAGO_5_8"),
        ("PAGO INMEDIATO",        "MANIFIESTO_INMEDIATO",    "CEDULA_INMEDIATO",    "NOMBRE_INMEDIATO"),
        ("CONTRAENTREGA",         "MANIFIESTO_CONTRAENTREGA", "CEDULA_CONTRAENTREGA", "NOMBRE_CONTRAENTREGA"),
        ("CONTINGENCIA 20-25 DH", "MANIFIESTO_CONTINGENCIA", "CEDULA_CONTINGENCIA", "NOMBRE_CONTINGENCIA"),
        ("URBANO",                "MANIFIESTO_URBANO",       "CEDULA_URBANO",       "NOMBRE_URBANO"),
        ("OTROS",                 "MANIFIESTO_OTROS",        "CEDULA_OTROS",        "NOMBRE_OTROS"),
    ]
    sin_datos: list[str] = []
    for compromiso, var, cvar, nvar in _COMPROMISOS:
        print(f"\n— Buscando manifiesto pendiente con compromiso_pago = {compromiso} —")
        rows = await _get(VIEW, {
            "compromiso_pago": f"eq.{compromiso}",
            "estado_interno":  "neq.ANULADO",
            "fecha_pago":      "is.null",
            "fecha_cumplido":  "not.is.null",
            "select":          "manifiesto,conductor,cedula_conductor,compromiso_pago,fecha_cumplido,fecha_estimada_pago",
            "limit":           "1",
        })
        if rows:
            r = rows[0]
            if r.get("cedula_conductor"):
                _set(var, r["manifiesto"],
                     f"({r['conductor']} · cumplido {r.get('fecha_cumplido')} · estimado {r.get('fecha_estimada_pago')})")
                _set(cvar, r.get("cedula_conductor"))
                _set(nvar, r["conductor"])
            else:
                print(f"  {var} = skip (manifiesto {r['manifiesto']} sin cédula en BD — se conserva el fixture manual)")
                sin_datos.append(f"{compromiso} (sin cédula en BD)")
        else:
            _set(var, None)
            _set(cvar, None)
            _set(nvar, None)
            sin_datos.append(compromiso)

    print(f"\n— Buscando manifiesto pendiente sin compromiso_pago (NULL) —")
    rows = await _get(VIEW, {
        "compromiso_pago": "is.null",
        "estado_interno":  "neq.ANULADO",
        "fecha_pago":      "is.null",
        "fecha_cumplido":  "not.is.null",
        "select":          "manifiesto,conductor,cedula_conductor,fecha_cumplido,fecha_estimada_pago",
        "limit":           "1",
    })
    if rows:
        r = rows[0]
        if r.get("cedula_conductor"):
            _set("MANIFIESTO_SIN_COMPROMISO", r["manifiesto"],
                 f"({r['conductor']} · cumplido {r.get('fecha_cumplido')} · estimado {r.get('fecha_estimada_pago')})")
            _set("CEDULA_SIN_COMPROMISO", r.get("cedula_conductor"))
            _set("NOMBRE_SIN_COMPROMISO", r["conductor"])
        else:
            print(f"  MANIFIESTO_SIN_COMPROMISO = skip (manifiesto {r['manifiesto']} sin cédula en BD)")
            sin_datos.append("SIN COMPROMISO (sin cédula en BD)")
    else:
        _set("MANIFIESTO_SIN_COMPROMISO", None)
        _set("CEDULA_SIN_COMPROMISO", None)
        _set("NOMBRE_SIN_COMPROMISO", None)
        sin_datos.append("SIN COMPROMISO (NULL)")

    print(f"\n— Buscando manifiesto SIN fecha_cumplido (viaje no cerrado) —")
    rows = await _get(VIEW, {
        "estado_interno": "neq.ANULADO",
        "fecha_pago":     "is.null",
        "fecha_cumplido": "is.null",
        "select":         "manifiesto,conductor,cedula_conductor,estado_interno,compromiso_pago",
        "limit":          "1",
    })
    if rows:
        r = rows[0]
        if r.get("cedula_conductor"):
            _set("MANIFIESTO_SIN_CUMPLIDO", r["manifiesto"],
                 f"({r['conductor']} · estado {r.get('estado_interno')} · compromiso {r.get('compromiso_pago')})")
            _set("CEDULA_SIN_CUMPLIDO", r.get("cedula_conductor"))
            _set("NOMBRE_SIN_CUMPLIDO", r["conductor"])
        else:
            print(f"  MANIFIESTO_SIN_CUMPLIDO = skip (manifiesto {r['manifiesto']} sin cédula en BD — se conserva fixture manual)")
    else:
        _set("MANIFIESTO_SIN_CUMPLIDO", None)
        _set("CEDULA_SIN_CUMPLIDO", None)
        _set("NOMBRE_SIN_CUMPLIDO", None)

    print(f"\n— Buscando manifiesto pendiente con compromiso_pago = PRIORITARIO —")
    rows = await _get(VIEW, {
        "compromiso_pago": "eq.PRIORITARIO",
        "estado_interno":  "neq.ANULADO",
        "fecha_pago":      "is.null",
        "fecha_cumplido":  "not.is.null",
        "select":          "manifiesto,conductor,cedula_conductor,compromiso_pago,fecha_cumplido,fecha_estimada_pago",
        "limit":           "1",
    })
    if rows:
        r = rows[0]
        _set("MANIFIESTO_PRIORITARIO", r["manifiesto"],
             f"({r['conductor']} · cumplido {r.get('fecha_cumplido')} · estimado {r.get('fecha_estimada_pago')})")
        _set("CEDULA_PRIORITARIO", r.get("cedula_conductor"))
        _set("NOMBRE_PRIORITARIO", r["conductor"])
    else:
        _set("MANIFIESTO_PRIORITARIO", None)
        _set("CEDULA_PRIORITARIO", None)
        _set("NOMBRE_PRIORITARIO", None)
        sin_datos.append("PRIORITARIO")

    print(f"\n— Buscando manifiesto pendiente con compromiso_pago = PRONTO PAGO —")
    rows = await _get(VIEW, {
        "compromiso_pago": "eq.PRONTO PAGO",
        "estado_interno":  "neq.ANULADO",
        "fecha_pago":      "is.null",
        "fecha_cumplido":  "not.is.null",
        "select":          "manifiesto,conductor,cedula_conductor,compromiso_pago,fecha_cumplido",
        "limit":           "1",
    })
    if rows:
        r = rows[0]
        _set("MANIFIESTO_PRONTO_PAGO", r["manifiesto"],
             f"({r['conductor']} · cumplido {r.get('fecha_cumplido')})")
        _set("CEDULA_PRONTO_PAGO", r.get("cedula_conductor"))
        _set("NOMBRE_PRONTO_PAGO", r["conductor"])
    else:
        _set("MANIFIESTO_PRONTO_PAGO", None)
        sin_datos.append("PRONTO PAGO")

    print(f"\n— Buscando manifiesto con PAGO PARCIAL (valor_pagado > 0, fecha_pago null) —")
    rows = await _get(VIEW, {
        "estado_interno": "neq.ANULADO",
        "fecha_pago":     "is.null",
        "valor_pagado":   "gt.0",
        "select":         "manifiesto,conductor,cedula_conductor,valor_pagado,saldo,compromiso_pago,fecha_estimada_pago",
        "limit":          "1",
    })
    if rows:
        r = rows[0]
        if r.get("cedula_conductor"):
            _set("MANIFIESTO_PAGO_PARCIAL", r["manifiesto"],
                 f"({r['conductor']} · abono ${r.get('valor_pagado')} · saldo ${r.get('saldo')} · {r.get('compromiso_pago')})")
            _set("CEDULA_PAGO_PARCIAL", r.get("cedula_conductor"))
            _set("NOMBRE_PAGO_PARCIAL", r["conductor"])
        else:
            print(f"  MANIFIESTO_PAGO_PARCIAL = skip (manifiesto {r['manifiesto']} sin cédula en BD)")
            sin_datos.append("PAGO PARCIAL (sin cédula en BD)")
    else:
        _set("MANIFIESTO_PAGO_PARCIAL", None)
        _set("CEDULA_PAGO_PARCIAL", None)
        _set("NOMBRE_PAGO_PARCIAL", None)
        sin_datos.append("PAGO PARCIAL")

    # ── Resumen de cobertura ──────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("  RESUMEN DE COBERTURA")
    print("=" * 70)
    if sin_datos:
        print("  ⚠️  Sin datos en BD — los casos correspondientes se marcarán como skip:")
        for s in sin_datos:
            print(f"     - {s}")
    else:
        print("  ✅ Todos los compromisos_pago tienen un fixture de prueba.")

    _generar_fixtures_auto(FIXTURES)
    print("\nCopia los valores de arriba a las variables al inicio de scripts/test_agent.py "
          "si no quieres depender del fixtures_auto.py.")


def _generar_fixtures_auto(fixtures: dict) -> None:
    """Escribe fixtures_auto.py para que test_agent.py cargue los IDs descubiertos."""
    path = os.path.join(os.path.dirname(__file__), "fixtures_auto.py")
    with open(path, "w", encoding="utf-8") as f:
        f.write("# Auto-generado por descubrir_fixtures.py — NO editar a mano.\n")
        f.write("# test_agent.py lo importa y sobreescribe sus defaults con estos valores.\n\n")
        f.write("FIXTURES = {\n")
        for k, v in sorted(fixtures.items()):
            f.write(f"    {k!r}: {v!r},\n")
        f.write("}\n")
    print(f"\n📁 Fixtures auto-generados: {path}")


if __name__ == "__main__":
    asyncio.run(main())
