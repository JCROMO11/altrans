"""
Ejecutar desde ai_agent/:
    python scripts/test_agent.py

Rellena las constantes de la sección CONFIG antes de correr.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from agent.graph import run

# ── CONFIG ────────────────────────────────────────────────────────────────────
# Pon un conductor y manifiesto reales de tu DB

CONDUCTOR_NOMBRE = "HENRY RAMIREZ"  # ← Nombre completo tal como está en la DB
CONDUCTOR_CEDULA = "1130668182"       # ← Cédula del conductor
MANIFIESTO_OK    = 21001        # ← Número de manifiesto que pertenece a ese conductor
MANIFIESTO_BAD   = 99999    # ← Número que NO existe en la DB
MES              = "MARZO"  # ← Mes con datos reales (en mayúsculas)
AÑO              = 2024     # ← Año con datos reales

# ── Helpers ───────────────────────────────────────────────────────────────────

def caso(titulo: str, pregunta: str) -> None:
    print(f"\n{'='*60}")
    print(f"CASO: {titulo}")
    print(f"  → {pregunta}")
    print("-" * 60)
    try:
        respuesta = run(
            pregunta,
            [],
            conductor_nombre=CONDUCTOR_NOMBRE,
            conductor_cedula=CONDUCTOR_CEDULA,
        )
        print(respuesta)
    except Exception as e:
        print(f"[ERROR] {e}")


def caso_admin(titulo: str, pregunta: str) -> None:
    """Sin conductor autenticado → acceso admin completo."""
    print(f"\n{'='*60}")
    print(f"CASO ADMIN: {titulo}")
    print(f"  → {pregunta}")
    print("-" * 60)
    try:
        respuesta = run(pregunta, [])
        print(respuesta)
    except Exception as e:
        print(f"[ERROR] {e}")


# ── Casos de prueba ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    if not CONDUCTOR_NOMBRE or not CONDUCTOR_CEDULA or not MANIFIESTO_OK:
        print("ERROR: Rellena las constantes CONFIG antes de correr el script.")
        sys.exit(1)

    print(f"\nConductor de prueba: {CONDUCTOR_NOMBRE}")
    print(f"Período de prueba:   {MES} {AÑO}")

    # ── 1. consultar_manifiesto ───────────────────────────────────────────────
    caso(
        "Consulta manifiesto válido propio",
        f"Dame los datos del manifiesto {MANIFIESTO_OK}",
    )
    caso(
        "Consulta manifiesto inexistente",
        f"Dame los datos del manifiesto {MANIFIESTO_BAD}",
    )

    # ── 2. resumen_periodo ────────────────────────────────────────────────────
    caso(
        "Resumen del período con datos",
        f"¿Cuál es mi resumen de {MES} {AÑO}?",
    )
    caso(
        "Resumen de período sin datos",
        "¿Cuál es mi resumen de FEBRERO 2019?",
    )

    # ── 3. manifiestos_pendientes_pago ────────────────────────────────────────
    caso(
        "Pendientes de pago sin filtro",
        "¿Cuáles de mis manifiestos tienen pago pendiente?",
    )
    caso(
        "Pendientes de pago filtrado por mes",
        f"¿Qué manifiestos me deben pagar de {MES} {AÑO}?",
    )

    # ── 4. manifiestos_sin_factura ────────────────────────────────────────────
    caso(
        "Sin factura sin filtro",
        "¿Cuáles de mis manifiestos no tienen factura?",
    )
    caso(
        "Sin factura filtrado por período",
        f"¿Qué manifiestos de {MES} {AÑO} no tienen factura?",
    )

    # ── 5. manifiestos_con_novedad ────────────────────────────────────────────
    caso(
        "Novedades pendientes sin filtro",
        "¿Tengo manifiestos con novedades pendientes?",
    )
    caso(
        "Novedades filtradas por período",
        f"¿Hubo novedades en mis manifiestos de {MES} {AÑO}?",
    )

    # ── 6. conductor_info ─────────────────────────────────────────────────────
    caso(
        "Info del propio conductor (intento — debe bloquearse o redirigir)",
        f"Dame información sobre el conductor {CONDUCTOR_NOMBRE.split()[0]}",
    )

    # ── 7. Acceso a datos de otro conductor (debe rechazarse) ─────────────────
    caso(
        "Intento de ver datos de otro conductor",
        "Dame el resumen del conductor con más manifiestos este año",
    )

    # ── 8. Casos admin (sin restricción de conductor) ─────────────────────────
    caso_admin(
        "Top conductores del período",
        f"¿Quiénes son los 5 conductores con más manifiestos en {MES} {AÑO}?",
    )
    caso_admin(
        "Top clientes del período",
        f"¿Cuáles son los clientes con más remesas en {MES} {AÑO}?",
    )
    caso_admin(
        "Top rutas sin filtro",
        "¿Cuáles son las rutas más frecuentes?",
    )
    caso_admin(
        "Resumen general del período",
        f"Dame el resumen de operaciones de {MES} {AÑO}",
    )
    caso_admin(
        "Buscar conductor por nombre",
        f"Busca información del conductor {CONDUCTOR_NOMBRE.split()[0]}",
    )
    caso_admin(
        "Todos los pendientes de pago",
        "¿Qué manifiestos tienen pago pendiente este año?",
    )

    print(f"\n{'='*60}")
    print("FIN DE PRUEBAS")
    print("="*60)
