"""
Exporta toda la DB (manifiestos_flat) a un CSV plano.

Uso:
    python -m etl_individual.export_db
    python -m etl_individual.export_db --salida /ruta/custom.csv
"""

import os
import sys
import argparse
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text

_DIR    = Path(__file__).parent.parent
_DB_URL = os.environ.get("DATABASE_URL", "")

COLUMNAS = [
    "manifiesto",
    "remesas",
    "fecha_despacho",
    "origen",
    "departamento_origen",
    "destino",
    "departamento_destino",
    "cliente",
    "valor_remesa",
    "flete_conductor",
    "anticipo",
    "placa",
    "tipo_vehiculo",
    "conductor",
    "celular",
    "cedula_conductor",
    "propietario",
    "agencia_despachadora",
    "nombre_responsable",
    "fecha_cumplido",
    "dias_cumplido",
    "compromiso_pago",
    "novedades",
    "estado_interno",
    "responsable_estado_interno",
    "fecha_pago",
    "valor_pagado",
    "entidad_financiera",
    "responsable",
    "factura_no",
    "fecha_factura",
    "factura_electronica",
    "mes_facturacion",
    "dias_para_facturar",
    "mes",
    "año",
    "archivo_origen",
]


def export(db_url: str, salida: Path):
    engine = create_engine(db_url, echo=False)

    print("Consultando DB ...")
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT
                manifiesto,
                remesas,
                fecha_despacho,
                origen,
                departamento_origen,
                destino,
                departamento_destino,
                cliente,
                valor_remesa,
                flete_conductor,
                anticipo,
                placa,
                tipo_vehiculo,
                conductor,
                celular,
                cedula_conductor,
                propietario,
                agencia_despachadora,
                nombre_responsable,
                fecha_cumplido,
                CASE WHEN fecha_cumplido IS NOT NULL
                     THEN CURRENT_DATE - fecha_cumplido
                END AS dias_cumplido,
                compromiso_pago,
                novedades,
                estado_interno,
                responsable_estado_interno,
                fecha_pago,
                valor_pagado,
                entidad_financiera,
                responsable,
                factura_no,
                fecha_factura,
                factura_electronica,
                mes_facturacion,
                dias_para_facturar,
                mes,
                año,
                archivo_origen
            FROM public.manifiestos_flat
            ORDER BY manifiesto
        """)).fetchall()

    print(f"  {len(rows):,} manifiestos recuperados")

    df = pd.DataFrame(rows, columns=COLUMNAS)

    salida.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(salida, index=False, encoding="utf-8-sig")
    print(f"  Exportado → {salida}  ({salida.stat().st_size / 1_048_576:.1f} MB)")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv(_DIR / ".env", override=True)
    _DB_URL = os.environ.get("DATABASE_URL", "")

    if not _DB_URL:
        sys.exit("ERROR: DATABASE_URL no definido en .env")

    parser = argparse.ArgumentParser(description="Exporta manifiestos_flat a CSV plano")
    parser.add_argument("--salida", type=Path,
                        default=_DIR / "exports" / "db_export.csv",
                        help="Ruta del archivo de salida (default: exports/db_export.csv)")
    args = parser.parse_args()

    export(_DB_URL, args.salida)
