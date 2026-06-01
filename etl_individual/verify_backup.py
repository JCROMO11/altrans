"""
Verifica integridad de un backup CSV contra la DB de producción.

Para cada tabla compara:
  1. Conteo de filas (CSV == DB)
  2. Claves primarias — detecta filas en CSV que no están en DB y viceversa
  3. Campos numéricos clave en manifiestos_flat (flete, valor_remesa, etc.)

Uso:
    python -m etl_individual.verify_backup                        # último backup
    python -m etl_individual.verify_backup --backup backups/backup_20260521_174627
"""
import argparse
import csv
import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_ROOT / ".env", override=False)

DB_URL = os.environ.get("DATABASE_URL")
if not DB_URL:
    sys.exit("ERROR: DATABASE_URL no definido en .env")

# Columna PK por tabla
_PK = {
    "manifiestos_flat":  "manifiesto",
    "audit_log":         "id",
    "chatbot_sesiones":  "wa_from",
    "processed_messages": "message_id",
    "jailbreak_log":     "id",
}

# Campos numéricos a verificar en manifiestos_flat (suma CSV == suma DB)
_NUMERIC_CHECKS = [
    "flete_conductor", "valor_remesa", "anticipo",
    "retencion_conductor", "saldo", "valor_pagado", "valor_factura",
]


def _latest_backup() -> Path:
    base = _ROOT / "backups"
    dirs = sorted([d for d in base.iterdir() if d.is_dir()], reverse=True)
    if not dirs:
        sys.exit("ERROR: no hay backups en backups/")
    return dirs[0]


def _read_csv_keys(path: Path, pk: str) -> set:
    with open(path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        return {row[pk] for row in reader if row.get(pk)}


def _read_csv_sums(path: Path, cols: list[str]) -> dict[str, float]:
    sums = {c: 0.0 for c in cols}
    with open(path, encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            for c in cols:
                val = row.get(c, "") or "0"
                try:
                    sums[c] += float(val)
                except ValueError:
                    pass
    return sums


def verify_tabla(conn, tabla: str, csv_path: Path) -> list[str]:
    issues = []
    pk = _PK.get(tabla)

    # 1. Conteo
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM public.{tabla}")
    n_db = cur.fetchone()[0]

    with open(csv_path, encoding="utf-8-sig", newline="") as f:
        n_csv = sum(1 for _ in csv.reader(f)) - 1

    if n_csv != n_db:
        issues.append(f"conteo: CSV={n_csv} vs DB={n_db} ({n_db - n_csv:+d})")

    if not pk:
        return issues

    # 2. Claves primarias
    csv_keys = _read_csv_keys(csv_path, pk)
    cur.execute(f"SELECT {pk}::text FROM public.{tabla}")
    db_keys = {r[0] for r in cur.fetchall()}

    solo_csv = csv_keys - db_keys
    solo_db  = db_keys - csv_keys

    if solo_csv:
        sample = sorted(solo_csv)[:5]
        issues.append(f"en CSV pero NO en DB ({len(solo_csv)} filas): {sample}")
    if solo_db:
        sample = sorted(solo_db)[:5]
        issues.append(f"en DB pero NO en CSV ({len(solo_db)} filas): {sample}")

    # 3. Sumas numéricas (solo manifiestos_flat)
    if tabla == "manifiestos_flat":
        csv_sums = _read_csv_sums(csv_path, _NUMERIC_CHECKS)
        for col in _NUMERIC_CHECKS:
            cur.execute(f"SELECT COALESCE(SUM({col}::numeric), 0) FROM public.{tabla}")
            db_sum = float(cur.fetchone()[0])
            csv_sum = csv_sums[col]
            diff = abs(db_sum - csv_sum)
            if diff > 1:  # tolerancia de $1 por redondeo
                issues.append(f"suma {col}: CSV=${csv_sum:,.0f} vs DB=${db_sum:,.0f} (diff=${diff:,.0f})")

    return issues


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--backup", type=Path, default=None,
                        help="Carpeta del backup (default: el más reciente)")
    args = parser.parse_args()

    backup_dir = args.backup or _latest_backup()
    print(f"Verificando backup: {backup_dir}\n")

    conn = psycopg2.connect(DB_URL)
    errores_totales = 0

    for tabla in _PK:
        csv_path = backup_dir / f"{tabla}.csv"
        if not csv_path.exists():
            print(f"  ⚠️  {tabla}: archivo no encontrado, saltando")
            continue

        issues = verify_tabla(conn, tabla, csv_path)
        if issues:
            print(f"  ❌ {tabla}:")
            for iss in issues:
                print(f"       · {iss}")
            errores_totales += len(issues)
        else:
            print(f"  ✅ {tabla}: OK")

    conn.close()
    print()
    if errores_totales:
        print(f"❌ {errores_totales} discrepancia(s) encontrada(s)")
        return 1
    print("✅ Backup íntegro — CSV == DB en todas las tablas")
    return 0


if __name__ == "__main__":
    sys.exit(main())
