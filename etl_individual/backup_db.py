"""
Backup defensivo de todas las tablas del schema public a CSV.

Fusiona la lógica de:
  - backup_now.py: dump multi-tabla (manifiestos_flat, audit_log, etc.)
  - export_db.py: export estructurado de manifiestos_flat con columnas calculadas

Uso:
    python -m etl_individual.backup_db                             # backup completo a backups/backup_<TS>/
    python -m etl_individual.backup_db --solo manifiestos_flat     # solo una tabla
    python -m etl_individual.backup_db --salida /ruta/custom        # destino custom
"""
import argparse
import datetime as _dt
import os
import sys
from pathlib import Path

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_ROOT / ".env", override=False)

DB_URL = os.environ.get("DATABASE_URL")
if not DB_URL:
    sys.exit("ERROR: DATABASE_URL no definido en .env")

# Tablas a respaldar por defecto
TABLES = [
    "manifiestos_flat",
    "audit_log",
    "chatbot_sesiones",
    "processed_messages",
    "jailbreak_log",
]

# Columnas estructuradas para manifiestos_flat (incluye dias_cumplido calculado)
_QUERY_MANIFIESTOS = """
    SELECT
        manifiesto, remesas, fecha_despacho,
        origen, departamento_origen, destino, departamento_destino,
        cliente, valor_remesa, flete_conductor, anticipo,
        placa, tipo_vehiculo, conductor, celular, cedula_conductor,
        propietario, agencia_despachadora, nombre_responsable,
        fecha_cumplido,
        CASE WHEN fecha_cumplido IS NOT NULL
             THEN CURRENT_DATE - fecha_cumplido
        END AS dias_cumplido,
        compromiso_pago, novedades, estado_interno, responsable_estado_interno,
        novedad_conductor, novedad_empresa,
        ajuste_positivo_flete, ajuste_negativo_flete, consignacion_a_terceros,
        flete_neto_conductor,
        fecha_pago, valor_pagado, entidad_financiera, responsable,
        factura_no, fecha_factura, factura_electronica, mes_facturacion,
        valor_factura, dias_para_facturar,
        mes, año, archivo_origen
    FROM public.manifiestos_flat
    ORDER BY manifiesto
"""


def backup_manifiestos_flat(db_url: str, out_path: Path) -> int:
    engine = create_engine(db_url, echo=False)
    with engine.connect() as conn:
        rows = conn.execute(text(_QUERY_MANIFIESTOS)).fetchall()
        cols = [c for c in conn.execute(text(_QUERY_MANIFIESTOS)).keys()]

    df = pd.DataFrame(rows, columns=cols)
    for col in ("valor_remesa", "flete_conductor", "anticipo", "valor_pagado",
                "ajuste_positivo_flete", "ajuste_negativo_flete",
                "consignacion_a_terceros", "flete_neto_conductor", "valor_factura"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").round(0).astype("Int64")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    return len(df)


def backup_tabla_simple(db_url: str, tabla: str, out_path: Path) -> int:
    """Backup crudo de una tabla cualquiera con COPY ... TO STDOUT."""
    conn = psycopg2.connect(db_url)
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT to_regclass('public.{tabla}')")
        if cur.fetchone()[0] is None:
            return -1
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            cur.copy_expert(f"COPY public.{tabla} TO STDOUT WITH CSV HEADER", f)
        cur.execute(f"SELECT COUNT(*) FROM public.{tabla}")
        n = cur.fetchone()[0]
        return n
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Backup de la DB Altrans a CSV")
    parser.add_argument("--solo", help="Solo una tabla (default: todas)")
    parser.add_argument("--salida", type=Path,
                        default=None, help="Carpeta destino (default: backups/backup_<TS>/)")
    args = parser.parse_args()

    ts = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = args.salida or (_ROOT / "backups" / f"backup_{ts}")
    out_dir.mkdir(parents=True, exist_ok=True)

    tablas = [args.solo] if args.solo else TABLES
    print(f"Backup → {out_dir}")

    # Conteos para verificación CSV vs DB
    discrepancias = []
    for t in tablas:
        path = out_dir / f"{t}.csv"
        if t == "manifiestos_flat":
            n_csv = backup_manifiestos_flat(DB_URL, path)
            print(f"  · {t}: {n_csv:,} filas (estructurado)")
        else:
            n_csv = backup_tabla_simple(DB_URL, t, path)
            if n_csv == -1:
                print(f"  · {t}: no existe, saltando")
                continue
            print(f"  · {t}: {n_csv:,} filas")

        # Verificar contra la DB — usa csv.reader para manejar campos con saltos de línea
        import csv
        with open(path, encoding="utf-8-sig", newline="") as f:
            filas_csv = sum(1 for _ in csv.reader(f)) - 1  # menos cabecera
        if filas_csv != n_csv:
            discrepancias.append(f"{t}: escritas={n_csv}, archivo={filas_csv}")

    if discrepancias:
        print("\n❌ Discrepancias detectadas:")
        for d in discrepancias:
            print(f"   {d}")
        return 1

    print("\n✅ Backup completo y verificado (CSV == DB en todas las tablas)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
