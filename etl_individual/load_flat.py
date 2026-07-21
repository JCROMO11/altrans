"""
Carga el cleaned CSV a manifiestos_flat (Big Table).

Estrategia: UPSERT por manifiesto.
  - Si el manifiesto no existe → INSERT.
  - Si ya existe → UPDATE solo los campos que vengan con valor en el CSV
    (los NULL del CSV no sobreescriben datos existentes).

Uso:
    python -m etl_individual.load_flat                        # carga todo el cleaned
    python -m etl_individual.load_flat --mes "JULIO 2024"     # recarga un mes
    python -m etl_individual.load_flat --dry-run              # muestra qué haría sin tocar DB
"""

import os
import sys
import argparse
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine

_DIR = Path(__file__).parent.parent

CLEANED_CSV = _DIR / "cleaned_data" / "individual_cleaned.csv"

# Columnas que van directamente al INSERT/UPDATE (excluye las computadas y de auditoría)
COLS = [
    "manifiesto", "archivo_origen", "mes", "año", "periodo", "semana",
    "consecutivo_semanal", "fecha_despacho", "origen", "departamento_origen",
    "destino", "departamento_destino", "cliente", "remesas",
    "valor_remesa", "flete_conductor", "anticipo",
    "placa", "tipo_vehiculo", "conductor", "celular", "cedula_conductor", "propietario",
    "agencia_despachadora", "nombre_responsable",
    "fecha_cumplido", "compromiso_pago", "novedades",
    "fecha_pago", "valor_pagado", "entidad_financiera", "responsable",
    "factura_no", "fecha_factura", "factura_electronica", "mes_facturacion",
    "estado_interno", "responsable_estado_interno",
]

# Campos de fecha para parseo
DATE_COLS    = ["fecha_despacho", "fecha_cumplido", "fecha_pago", "fecha_factura", "periodo"]
# Enteros (smallint/integer en DB) — deben ir sin decimales en el COPY
INT_COLS     = ["año", "consecutivo_semanal", "mes_facturacion"]
# Decimales (numeric en DB)
DECIMAL_COLS = ["valor_remesa", "flete_conductor", "anticipo", "valor_pagado"]
NUMERIC_COLS = INT_COLS + DECIMAL_COLS


def _prep(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza tipos del DataFrame para carga en DB."""
    df = df.copy()

    # Renombrar 'fecha' → 'fecha_factura' si viene del cleaned
    if "fecha" in df.columns and "fecha_factura" not in df.columns:
        df = df.rename(columns={"fecha": "fecha_factura"})

    # Conservar solo columnas que existen en COLS
    available = [c for c in COLS if c in df.columns]
    df = df[available].copy()

    # Manifiesto como int
    df["manifiesto"] = pd.to_numeric(df["manifiesto"], errors="coerce")
    df = df.dropna(subset=["manifiesto"])
    df["manifiesto"] = df["manifiesto"].astype(int)

    # Fechas
    for col in DATE_COLS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", format="mixed").dt.date

    # Enteros: usar Int64 nullable para evitar "1.0" en el COPY
    for col in INT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Decimales: float estándar
    for col in DECIMAL_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Strings: vacío y 'nan' → None
    str_cols = [c for c in available if c not in DATE_COLS + NUMERIC_COLS + ["manifiesto"]]
    for col in str_cols:
        df[col] = df[col].astype(object).where(df[col].notna(), None)
        df[col] = df[col].apply(
            lambda v: None if v is None or str(v).strip().upper() in ("NAN", "NONE", "") else str(v).strip()
        )

    # Conversión final: reemplazar cualquier float NaN residual por None en todo el df
    df = df.astype(object).where(df.notna(), other=None)

    # Colapsar filas del mismo manifiesto: las remesas se unen con coma,
    # el resto de columnas toma el primer valor no-nulo del grupo.
    if df["manifiesto"].duplicated().any():
        def _first_notnull(s):
            vals = s.dropna()
            return vals.iloc[0] if len(vals) else None

        agg: dict = {"remesas": lambda s: ",".join(v for v in s if v is not None)}
        for col in [c for c in df.columns if c not in ("manifiesto", "remesas")]:
            agg[col] = _first_notnull

        n_before = len(df)
        df = df.groupby("manifiesto", sort=False).agg(agg).reset_index()
        print(f"  [info] {n_before} filas → {len(df)} manifiestos (remesas consolidadas)")

        # groupby resetea los tipos; restaurar Int64 para columnas enteras
        for col in INT_COLS:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    return df


def load_flat(df: pd.DataFrame, engine, dry_run: bool = False) -> dict:
    """
    UPSERT masivo en manifiestos_flat usando tabla temporal + COPY.
    Mucho más rápido que executemany fila a fila porque hace una sola
    transferencia de red en lugar de un round-trip por fila.
    """
    import io
    df = _prep(df)
    available_cols = [c for c in COLS if c in df.columns]
    IMMUTABLE_RELOAD_COLS = {"conductor", "cedula_conductor", "propietario"}
    update_cols    = [c for c in available_cols if c != "manifiesto" and c not in IMMUTABLE_RELOAD_COLS]

    update_clause = ",\n            ".join(
        f"{c} = COALESCE(EXCLUDED.{c}, manifiestos_flat.{c})"
        for c in update_cols
    )
    col_list = ", ".join(available_cols)

    if dry_run:
        print(f"  [dry-run] {len(df):,} filas listas para UPSERT (sin tocar DB)")
        return {"upserted": 0, "total": len(df)}

    raw = engine.raw_connection()
    try:
        with raw.cursor() as cur:
            # 1. Tabla temporal con la misma estructura (sin constraints ni columnas computadas)
            cur.execute(f"""
                CREATE TEMP TABLE _tmp_flat
                (LIKE manifiestos_flat INCLUDING DEFAULTS)
                ON COMMIT DROP
            """)
            # Quitar columnas generadas de la temp (no se puede insertar en ellas)
            cur.execute("""
                ALTER TABLE _tmp_flat
                DROP COLUMN IF EXISTS dias_para_facturar,
                DROP COLUMN IF EXISTS retencion_conductor,
                DROP COLUMN IF EXISTS saldo,
                DROP COLUMN IF EXISTS dias_cumplido,
                DROP COLUMN IF EXISTS cargado_en,
                DROP COLUMN IF EXISTS actualizado_en
            """)

            # 2. COPY masivo — una sola transferencia
            buf = io.StringIO()
            df[available_cols].to_csv(buf, index=False, header=False, na_rep="\\N")
            buf.seek(0)
            cur.copy_expert(
                f"COPY _tmp_flat ({col_list}) FROM STDIN WITH CSV NULL '\\N'",
                buf
            )

            # 3. UPSERT desde temporal a tabla real — una sola query en servidor
            cur.execute(f"""
                INSERT INTO manifiestos_flat ({col_list}, actualizado_en)
                SELECT {col_list}, now() FROM _tmp_flat
                ON CONFLICT (manifiesto) DO UPDATE SET
                    {update_clause},
                    actualizado_en = now()
            """)
            n = cur.rowcount

        raw.commit()
    finally:
        raw.close()

    return {"upserted": n, "total": len(df)}


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    load_dotenv(_DIR / ".env", override=True)
    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        sys.exit("ERROR: DATABASE_URL no definido en .env")

    parser = argparse.ArgumentParser()
    parser.add_argument("--mes",     help='Filtrar por archivo_origen, ej: "JULIO 2024"')
    parser.add_argument("--dry-run", action="store_true", help="No modifica la DB")
    args = parser.parse_args()

    df = pd.read_csv(CLEANED_CSV, dtype=str)
    total_orig = len(df)

    if args.mes:
        df = df[df["archivo_origen"] == args.mes]
        if df.empty:
            sys.exit(f"ERROR: no se encontraron filas para '{args.mes}'")
        print(f"Filtrando por mes: {args.mes} ({len(df):,} filas)")

    print(f"Cargando {len(df):,} filas de {total_orig:,} en cleaned ...")

    engine = create_engine(db_url, echo=False)
    result = load_flat(df, engine, dry_run=args.dry_run)

    print(f"  UPSERT completado: {result['upserted']:,} filas procesadas")
