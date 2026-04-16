"""
validations.py
Analiza cleaned_data/individual_cleaned.csv y genera informes/validaciones.xlsx
con perfil por columna para definir restricciones CHECK, NOT NULL y Enums en PostgreSQL.

Hojas:
  perfil_columnas    — una fila por columna con métricas completas
  valores_categoricos — distribución de valores para columnas con ≤ ENUM_THRESHOLD únicos
"""

import re
import datetime
from pathlib import Path

import pandas as pd


CSV_PATH    = Path("cleaned_data/individual_cleaned.csv")
OUTPUT_PATH = Path("informes/validaciones.xlsx")

ENUM_THRESHOLD     = 100   # columnas con ≤ N únicos → candidatas a Enum/CHECK
NOT_NULL_THRESHOLD = 5.0   # % de nulos bajo el cual sugerir NOT NULL

# Columnas que son identificadores de texto aunque tengan muchos únicos
ID_COLS = {"manifiesto", "consecutivo_semanal", "celular", "cedula_conductor",
           "factura_no", "factura_electronica", "remesas", "placa"}

# Columnas de fecha conocidas (YYYY-MM-DD en el CSV)
DATE_COLS = {"periodo", "fecha_despacho", "fecha_cumplido", "fecha_pago", "fecha"}


def _infer_type(col: str, series: pd.Series) -> str:
    if col in DATE_COLS:
        return "fecha"
    if col in ID_COLS:
        return "id_texto"

    s = series.dropna()
    if s.empty:
        return "vacio"

    if pd.api.types.is_bool_dtype(series):
        return "booleano"

    if pd.api.types.is_integer_dtype(series):
        return "entero"

    if pd.api.types.is_float_dtype(series):
        # Entero almacenado como float (sin decimales reales)
        if s.apply(lambda x: float(x) == int(x)).all():
            return "entero"
        return "decimal"

    if pd.api.types.is_datetime64_any_dtype(series):
        return "fecha"

    # Object dtype: decidir por cardinalidad
    n_unicos = s.nunique()
    if n_unicos <= ENUM_THRESHOLD:
        return "categorico"

    return "texto_libre"


def _profile_column(col: str, series: pd.Series) -> dict:
    n_total   = len(series)
    n_nulos   = int(series.isna().sum())
    pct_nulos = round(n_nulos / n_total * 100, 2) if n_total else 0.0
    s         = series.dropna()
    n_unicos  = int(s.nunique())
    tipo      = _infer_type(col, series)

    row: dict = {
        "columna":            col,
        "tipo_inferido":      tipo,
        "dtype_pandas":       str(series.dtype),
        "n_total":            n_total,
        "n_nulos":            n_nulos,
        "pct_nulos":          pct_nulos,
        "not_null_sugerido":  pct_nulos < NOT_NULL_THRESHOLD,
        "n_unicos":           n_unicos,
        "enum_candidato":     tipo == "categorico",
        "min":                None,
        "max":                None,
        "media":              None,
        "len_min":            None,
        "len_max":            None,
        "len_promedio":       None,
        "valores_muestra":    "",
    }

    if s.empty:
        return row

    # ── Numéricos / enteros ───────────────────────────────────────
    if tipo in ("entero", "decimal"):
        row["min"]   = round(float(s.min()), 4)
        row["max"]   = round(float(s.max()), 4)
        row["media"] = round(float(s.mean()), 4)

    # ── Fechas ────────────────────────────────────────────────────
    elif tipo == "fecha":
        vals_sorted = sorted(v for v in s if pd.notna(v))
        if vals_sorted:
            row["min"] = str(vals_sorted[0])
            row["max"] = str(vals_sorted[-1])

    # ── Strings: longitud ─────────────────────────────────────────
    if tipo in ("categorico", "texto_libre", "id_texto"):
        lengths = s.astype(str).str.len()
        row["len_min"]      = int(lengths.min())
        row["len_max"]      = int(lengths.max())
        row["len_promedio"] = round(float(lengths.mean()), 1)

    # ── Muestra de valores únicos ──────────────────────────────────
    unique_vals = sorted(str(v) for v in s.unique() if str(v).strip())
    if len(unique_vals) <= ENUM_THRESHOLD:
        row["valores_muestra"] = ", ".join(unique_vals)
    else:
        row["valores_muestra"] = (
            ", ".join(unique_vals[:20]) + f"  … (+{len(unique_vals) - 20} más)"
        )

    return row


def _build_enum_sheet(df: pd.DataFrame, tipo_map: dict[str, str]) -> pd.DataFrame:
    """Distribución completa de valores para columnas categóricas (≤ ENUM_THRESHOLD únicos)."""
    parts = []
    n_total = len(df)

    for col in df.columns:
        if tipo_map.get(col) != "categorico":
            continue

        counts = (
            df[col].dropna()
            .astype(str)
            .value_counts()
            .reset_index()
        )
        counts.columns = ["valor", "frecuencia"]
        counts["columna"]       = col
        counts["pct_no_nulo"]   = (counts["frecuencia"] / df[col].notna().sum() * 100).round(2)
        counts["pct_del_total"] = (counts["frecuencia"] / n_total * 100).round(2)
        parts.append(counts[["columna", "valor", "frecuencia", "pct_no_nulo", "pct_del_total"]])

    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def main() -> None:
    print(f"Leyendo {CSV_PATH} …")
    df = pd.read_csv(CSV_PATH, low_memory=False)
    print(f"  {len(df):,} filas × {len(df.columns)} columnas")

    # Parsear fechas conocidas
    for col in DATE_COLS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    # ── Perfil ────────────────────────────────────────────────────
    print("Generando perfil de columnas …")
    profile_rows = [_profile_column(col, df[col]) for col in df.columns]
    profile_df   = pd.DataFrame(profile_rows)

    tipo_map = dict(zip(profile_df["columna"], profile_df["tipo_inferido"]))

    # ── Distribución de valores categóricos ───────────────────────
    print("Generando distribución de valores categóricos …")
    enum_df = _build_enum_sheet(df, tipo_map)

    # ── Excel ─────────────────────────────────────────────────────
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    print(f"Escribiendo {OUTPUT_PATH} …")

    with pd.ExcelWriter(OUTPUT_PATH, engine="openpyxl") as writer:
        profile_df.to_excel(writer, sheet_name="perfil_columnas",     index=False)
        if not enum_df.empty:
            enum_df.to_excel(writer, sheet_name="valores_categoricos", index=False)

    # ── Resumen consola ───────────────────────────────────────────
    n_enum     = int(profile_df["enum_candidato"].sum())
    n_not_null = int(profile_df["not_null_sugerido"].sum())
    n_vacio    = int((profile_df["tipo_inferido"] == "vacio").sum())

    print(f"\nResumen:")
    print(f"  Columnas analizadas:        {len(profile_df)}")
    print(f"  Candidatas a Enum/CHECK:    {n_enum}")
    print(f"  Candidatas a NOT NULL (<{NOT_NULL_THRESHOLD:.0f}% nulos): {n_not_null}")
    if n_vacio:
        print(f"  Columnas 100% vacías:       {n_vacio}")
    print(f"\n  → {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
