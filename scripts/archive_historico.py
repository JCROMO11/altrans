"""
Archiva (u desarchiva) manifiestos históricos en manifiestos_flat.

Los manifiestos archivados NO se notifican por WhatsApp y NO cuentan en las
alertas del dashboard (vencidos / por vencer / saldo vencido). Siguen
guardados y buscables.

Por defecto archiva todo lo despachado antes del 1 de enero del año en curso
(misma fecha que `public.notify_min_date()`) y los manifiestos sin fecha de
despacho. El ETL no toca la columna `archivado`, así que la marca persiste.

Uso:
    python -m scripts.archive_historico --dry-run          # ver qué haría
    python -m scripts.archive_historico                    # archivar
    python -m scripts.archive_historico --year 2026        # archivar < 2026-01-01
    python -m scripts.archive_historico --unarchive        # revertir
"""
import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent


def _connect():
    import psycopg2

    load_dotenv(ROOT / ".env", override=True)
    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        sys.exit("ERROR: DATABASE_URL no definido en .env")
    return psycopg2.connect(db_url)


def main() -> None:
    parser = argparse.ArgumentParser(description="Archiva manifiestos históricos")
    parser.add_argument("--year", type=int, help="Archivar despachos anteriores a este año (default: año en curso)")
    parser.add_argument("--dry-run", action="store_true", help="Solo mostrar el conteo, sin modificar")
    parser.add_argument("--unarchive", action="store_true", help="Revertir: desarchivar el mismo rango")
    parser.add_argument("--no-null", action="store_true", help="No incluir manifiestos sin fecha_despacho")
    args = parser.parse_args()

    cutoff = f"{args.year}-01-01" if args.year else None
    null_clause = "OR fecha_despacho IS NULL" if not args.no_null else ""

    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT date_trunc('year', CURRENT_DATE)::DATE")
            default_cutoff = cur.fetchone()[0]

            if cutoff:
                where = f"fecha_despacho < DATE '{cutoff}'"
                cutoff_txt = cutoff
            else:
                where = "fecha_despacho < date_trunc('year', CURRENT_DATE)"
                cutoff_txt = str(default_cutoff)
            if null_clause:
                where = f"({where} {null_clause})"

            target = "false" if args.unarchive else "true"
            accion = "desarchivar" if args.unarchive else "archivar"

            cur.execute(f"SELECT COUNT(*) FROM manifiestos_flat WHERE {where} AND archivado IS DISTINCT FROM {target}")
            n = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM manifiestos_flat WHERE archivado")
            ya = cur.fetchone()[0]

            print(f"Rango ({'≥' if args.unarchive else '<'}): {where}")
            print(f"Manifiestos a {accion}: {n:,}  |  ya archivados: {ya:,}")

            if args.dry_run:
                print("(dry-run) No se modificó nada.")
                return

            if n == 0:
                print("Nada por hacer.")
                return

            cur.execute(f"UPDATE manifiestos_flat SET archivado = {target}, actualizado_en = now() WHERE {where}")
            print(f"✅ {cur.rowcount:,} manifiestos actualizados a archivado={target}")
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
