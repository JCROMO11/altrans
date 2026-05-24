"""
Verifica la carga comparando manifiestos únicos en el CSV vs la DB.

El CSV puede tener varias filas por manifiesto (una por remesa); la DB consolida
esas filas en un único manifiesto. La comparación correcta es:
    manifiestos únicos en CSV  ==  COUNT(*) en manifiestos_flat

Uso:
    python -m etl_individual.verify_load
"""
import csv
import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_ROOT / ".env", override=False)

DB_URL   = os.environ.get("DATABASE_URL")
CSV_PATH = _ROOT / "cleaned_data" / "individual_cleaned.csv"


def main() -> int:
    if not DB_URL:
        print("ERROR: DATABASE_URL no definido en .env")
        return 1

    conn = psycopg2.connect(DB_URL)
    cur  = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM public.manifiestos_flat")
    n_db = cur.fetchone()[0]
    conn.close()

    print(f"manifiestos_flat (DB):        {n_db:,} manifiestos")

    if not CSV_PATH.exists():
        print("(CSV cleaned no encontrado — corre make etl primero)")
        return 1

    seen = set()
    total_rows = 0
    skipped_empty = 0
    with open(CSV_PATH, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            total_rows += 1
            m = row.get("manifiesto", "").strip()
            if m:
                seen.add(m)
            else:
                skipped_empty += 1

    n_csv_unique = len(seen)
    valid_rows = total_rows - skipped_empty
    multi = valid_rows - n_csv_unique  # filas con manifiesto repetido (remesas)

    print(f"individual_cleaned.csv filas: {total_rows:,} filas "
          f"({n_csv_unique:,} manifiestos únicos"
          + (f", {multi:,} filas de remesas adicionales" if multi else "")
          + (f", {skipped_empty} filas sin manifiesto ignoradas" if skipped_empty else "")
          + ")")

    if n_csv_unique != n_db:
        diff = abs(n_db - n_csv_unique)
        print(f"✕ DB vs CSV (únicos): difieren ({diff} de diferencia)")
        return 1

    print("✓ DB vs CSV (únicos): coinciden")

    # Dedup check
    conn2 = psycopg2.connect(DB_URL)
    cur2  = conn2.cursor()
    cur2.execute("""
        SELECT manifiesto, COUNT(*) AS n
        FROM public.manifiestos_flat
        GROUP BY 1 HAVING COUNT(*) > 1
        ORDER BY n DESC
    """)
    dups = cur2.fetchall()
    conn2.close()

    if dups:
        print("⚠️  Manifiestos duplicados en DB:")
        for m, n in dups:
            print(f"   {m}: {n} ocurrencias")
        return 1

    print("✅ Sin duplicados")
    print("✅ Carga verificada")
    return 0


if __name__ == "__main__":
    sys.exit(main())
