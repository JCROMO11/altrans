"""
Verifica que el schema esté completo después de db-reset.

Chequea que existen:
  - 5 tablas: manifiestos_flat, audit_log, chatbot_sesiones,
              processed_messages, jailbreak_log
  - 5 RPCs:   consulta_manifiestos, consulta_totales, tendencia_anual,
              get_catalogos, get_usuarios

Exit code 0 si todo está; 1 si falta algo.
"""
import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=False)

TABLAS = [
    "manifiestos_flat",
    "audit_log",
    "chatbot_sesiones",
    "processed_messages",
    "jailbreak_log",
]

RPCS = [
    "consulta_manifiestos",
    "consulta_totales",
    "tendencia_anual",
    "get_catalogos",
    "get_usuarios",
]


def main() -> int:
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL no definido en .env")
        return 1

    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    faltan_tablas = []
    for t in TABLAS:
        cur.execute("SELECT to_regclass(%s)", (f"public.{t}",))
        if cur.fetchone()[0] is None:
            faltan_tablas.append(t)

    faltan_rpcs = []
    for r in RPCS:
        cur.execute(
            "SELECT 1 FROM pg_proc p "
            "JOIN pg_namespace n ON n.oid = p.pronamespace "
            "WHERE n.nspname = 'public' AND p.proname = %s",
            (r,),
        )
        if cur.fetchone() is None:
            faltan_rpcs.append(r)

    conn.close()

    print("Tablas requeridas:")
    for t in TABLAS:
        mark = "✗" if t in faltan_tablas else "✓"
        print(f"  {mark} {t}")

    print("\nRPCs requeridas:")
    for r in RPCS:
        mark = "✗" if r in faltan_rpcs else "✓"
        print(f"  {mark} {r}")

    if faltan_tablas or faltan_rpcs:
        print("\n❌ Schema incompleto.")
        if faltan_tablas:
            print(f"   Tablas faltantes: {', '.join(faltan_tablas)}")
        if faltan_rpcs:
            print(f"   RPCs faltantes: {', '.join(faltan_rpcs)}")
        return 1

    print("\n✅ Schema completo")
    return 0


if __name__ == "__main__":
    sys.exit(main())
