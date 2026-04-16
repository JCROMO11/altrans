import re
import unicodedata
import pandas as pd
from pathlib import Path


def normalize_col(name: str) -> str:
    """Lowercase + strip para comparación de columnas."""
    return str(name).strip().lower()


def find_header_row(file, max_rows=20):
    preview = pd.read_csv(file, header=None, nrows=max_rows)
    
    # Ignora filas que contengan "HOLA" en cualquier celda
    hola_mask = preview.apply(
        lambda row: row.astype(str).str.contains('HOLA', case=False, na=False).any(),
        axis=1
    )
    valid_rows = preview[~hola_mask]
    
    if valid_rows.empty:
        valid_rows = preview  # fallback por si acaso
    
    non_null_counts = valid_rows.notna().sum(axis=1)
    return int(non_null_counts.idxmax())

def week_col(df):
    """
    Detecta filas 'SEMANA X' en la primera columna, crea la columna 'semana'
    con el número correspondiente para cada fila de datos, y elimina las filas de semana.
    Si el DataFrame no tiene filas de semana, asigna 'N/A' a toda la columna.
    """
    import re

    first_col = df.columns[0]
    week_pattern = re.compile(r'^\s*SEMANA\s+(\d+)\s*$', re.IGNORECASE)

    semana_mask = df[first_col].astype(str).str.match(week_pattern)

    if not semana_mask.any():
        df = df.copy()
        df.insert(0, 'semana', 'N/A')
        return df

    semana_labels = []
    current_week = 'N/A'
    for val, is_semana in zip(df[first_col].astype(str), semana_mask):
        if is_semana:
            match = week_pattern.match(val)
            current_week = f"Semana {match.group(1)}"
        semana_labels.append(current_week)

    df = df.copy()
    df.insert(0, 'semana', semana_labels)
    df = df[~semana_mask.values].reset_index(drop=True)
    return df

def read_csv_with_header_detection(file, unnamed_null_threshold=80):
    df = pd.read_csv(file, header=find_header_row(file))
    df = week_col(df)
    
    # Renombra Unnamed conocidos antes de eliminar (usa nombres raw del CSV)
    cols = list(df.columns)
    for i, col in enumerate(cols):
        if col == 'Unnamed: 12' and i > 0 and cols[i - 1] == 'PLACA ':
            cols[i] = 'TIPO DE VEHICULO '
    df.columns = cols

    # Renombra Unnamed: 0 a MANIFIESTO si tiene pocos nulos y no hay manifiesto ya
    cols = list(df.columns)
    if 'Unnamed: 0' in cols and 'MANIFIESTO ' not in cols and 'MANIFIESTO' not in cols:
        null_pct = df['Unnamed: 0'].isna().mean() * 100
        if null_pct < 20:
            cols[cols.index('Unnamed: 0')] = 'MANIFIESTO'
            df.columns = cols

    # Elimina columnas Unnamed: siempre la 0 (índice de Excel) y las demás si tienen >80% nulos
    unnamed_cols = df.columns[df.columns.str.match(r'^Unnamed: \d+$')]
    basura = [
        col for col in unnamed_cols
        if col == 'Unnamed: 0' or df[col].isna().mean() * 100 >= unnamed_null_threshold
    ]
    if basura:
        print(f"  Eliminando {len(basura)} columnas vacías en '{file.name}': {basura}")
        df = df.drop(columns=basura)

    # Normalizar nombres de columnas: lowercase + strip
    df.columns = [normalize_col(c) for c in df.columns]

    # Renombra columnas con nombres incorrectos (ya normalizados)
    df = df.rename(columns={
        'columna 1': 'responsable estado interno',
        'm,':        'manifiesto',   # FEBRERO 2025
    })

    if len(df.columns) < 5:
        print(f"Warning: '{file.name}' tiene solo {len(df.columns)} columnas: {list(df.columns)}")

    print(f"'{file.name}' cargado con {len(df)} filas y {len(df.columns)} columnas.")
    return df

def columns_schema(df_list: list[pd.DataFrame], files: list, similarity_threshold: int = 80) -> dict:
    """
    Analiza las columnas de todos los DataFrames y devuelve un dict con tres secciones:

    - 'universal':  columnas normalizadas presentes en TODOS los archivos.
    - 'parciales':  columnas normalizadas presentes en ALGUNOS archivos,
                    con la lista de archivos y dtype por archivo.
    - 'similares':  grupos de columnas con nombres distintos pero parecidos
                    (similaridad >= similarity_threshold), candidatos a ser la misma.
    - 'unicas':     columnas que solo aparecen en un único archivo.
    """
    from rapidfuzz import fuzz
    from collections import defaultdict

    n_files = len(df_list)

    # Construir índice: col_norm → {file: dtype_original}
    col_index = defaultdict(dict)  # {col_norm: {filename: dtype}}
    col_raw   = defaultdict(set)   # {col_norm: set of raw names seen}

    for df, f in zip(df_list, files):
        for col in df.columns:
            norm = normalize_col(col)
            col_index[norm][f.name] = str(df[col].dtype)
            col_raw[norm].add(col.strip())

    # Clasificar por frecuencia
    universal = {}
    parciales  = {}
    unicas     = {}

    for norm, file_dtype in col_index.items():
        count = len(file_dtype)
        entry = {
            "archivos":    list(file_dtype.keys()),
            "n_archivos":  count,
            "raw_names":   sorted(col_raw[norm]),
            "dtypes":      file_dtype,
        }
        if count == n_files:
            universal[norm] = entry
        elif count == 1:
            unicas[norm] = entry
        else:
            parciales[norm] = entry

    # Detectar columnas similares (nombre distinto, significado posiblemente igual)
    all_norms = list(col_index.keys())
    visited   = set()
    similares = []

    for i, a in enumerate(all_norms):
        if a in visited:
            continue
        grupo = [a]
        for b in all_norms[i + 1:]:
            if b in visited:
                continue
            score = fuzz.ratio(a, b)
            if score >= similarity_threshold:
                grupo.append(b)
                visited.add(b)
        if len(grupo) > 1:
            visited.add(a)
            similares.append({
                "columnas":   grupo,
                "raw_names":  {g: sorted(col_raw[g]) for g in grupo},
                "n_archivos": {g: len(col_index[g]) for g in grupo},
            })

    return {
        "universal": universal,
        "parciales":  parciales,
        "unicas":     unicas,
        "similares":  similares,
    }


MESES = {
    "ENERO": 1, "FEBRERO": 2, "MARZO": 3, "ABRIL": 4,
    "MAYO": 5, "JUNIO": 6, "JULIO": 7, "AGOSTO": 8,
    "SEPTIEMBRE": 9, "OCTUBRE": 10, "NOVIEMBRE": 11, "DICIEMBRE": 12,
}

def _parse_periodo(stem: str):
    """
    Extrae mes, año y periodo (date) del nombre del archivo.
    Ej: 'ENERO 2025' → ('ENERO', 2025, date(2025, 1, 1))
    """
    from datetime import date
    parts = stem.strip().upper().split()
    mes  = next((p for p in parts if p in MESES), None)
    año  = next((int(p) for p in parts if re.fullmatch(r"\d{4}", p)), None)
    periodo = date(año, MESES[mes], 1) if mes and año else None
    return mes, año, periodo


RENAME_MAP = {
    # Universales
    "f. despacho":                                       "fecha_despacho",
    "tipo de vehiculo":                                  "tipo_vehiculo",
    "cedula conductor":                                  "cedula_conductor",
    "nombre responsable":                                "nombre_responsable",
    "fecha de pago":                                     "fecha_pago",
    "entidad financiera":                                "entidad_financiera",
    "factura no":                                        "factura_no",
    "valor pagado":                                      "valor_pagado",
    # Parciales
    "agencia despachadora":                              "agencia_despachadora",
    "valor remesa":                                      "valor_remesa",
    "flete conductor":                                   "flete_conductor",
    "flete":                                             "flete_conductor",
    "dias de cumplido":                                  "dias_cumplido",
    "dias cumplidos":                                    "dias_cumplido",
    "dias en cumplir":                                   "dias_cumplido",
    "fecha  cumplido":                                   "fecha_cumplido",
    "fecha cumplido":                                    "fecha_cumplido",
    "factura electronica del mc / propietario vehiculo": "factura_electronica",
    "factura electronica del mc":                        "factura_electronica",
    "dias tomados para facturar":                        "dias_para_facturar",
    "condicion de pago":                                 "condicion_pago",
    "estado interno":                                    "estado_interno",
    "responsable estado interno":                        "responsable_estado_interno",
    "remesa":                                            "remesas",
    "conseccutivo mensual":                              "consecutivo_semanal",
    "consecutivo semanal":                               "consecutivo_semanal",
    "tiemp. lg cargue":                                  "tiempo_lg_cargue",
    "tiemp. lg descargue":                               "tiempo_lg_descargue",
    "fecha 2":                                           "fecha",        # fix 3: faltaba
}

DROP_COLS = {
    "column 33",
    "mes (campo automatico",    # fix 2: nombre original, no el renombrado
    "g", "m,", "we", "0", "2", "23338",
}


def normalize_columns(df: pd.DataFrame, source_file: str = "") -> pd.DataFrame:
    """
    Normaliza las columnas de un DataFrame mensual:
      1. Strip + colapsa espacios múltiples internos en los nombres
      2. Descarta columnas sin nombre (NaN) y las de DROP_COLS
      3. Aplica RENAME_MAP (lookup por nombre en minúsculas)
      4. Agrega columna `archivo_origen` para trazabilidad
    """
    # 1. Limpiar nombres (NaN o vacío → "__drop__")
    def _clean_colname(c):
        if pd.isna(c):
            return "__drop__"
        cleaned = re.sub(r"\s+", " ", str(c)).strip()
        return cleaned if cleaned else "__drop__"

    df.columns = [_clean_colname(c) for c in df.columns]

    # 2. Identificar columnas a dropear
    def should_drop(col: str) -> bool:
        if col == "__drop__":
            return True
        cleaned = re.sub(r"\s+", " ", col.strip()).lower()
        if cleaned in DROP_COLS:
            return True
        if re.fullmatch(r"\d+", cleaned):           # puramente numérico
            return True
        if re.fullmatch(r"[a-z,]{1,2}", cleaned):   # 1-2 chars basura
            return True
        if re.match(r"^unnamed:\s*\d+$", cleaned):  # Unnamed: X residuales
            return True
        return False

    df = df.drop(columns=[c for c in df.columns if should_drop(c)], errors="ignore")

    # 3. Renombrar según RENAME_MAP
    rename = {}
    for col in df.columns:
        key = re.sub(r"\s+", " ", col.strip()).lower()
        if key in RENAME_MAP:
            rename[col] = RENAME_MAP[key]
    df = df.rename(columns=rename)

    # 4. Trazabilidad
    if source_file:
        stem = Path(source_file).stem.strip()          # nombre sin extensión
        mes, año, periodo = _parse_periodo(stem)

        df.insert(0, "periodo",        periodo)
        df.insert(0, "año",            año)
        df.insert(0, "mes",            mes)
        df.insert(0, "archivo_origen", stem)

    return df


def load_all(folder: str | Path) -> pd.DataFrame:
    """
    Lee todos los CSV de una carpeta usando read_csv_with_header_detection,
    normaliza cada uno y devuelve un único DataFrame concatenado.
    """
    folder = Path(folder)
    frames = []

    for path in sorted(folder.glob("*.csv")):
        df = read_csv_with_header_detection(path)
        df = normalize_columns(df, source_file=str(path))
        # Eliminar columnas duplicadas conservando la primera aparición
        df = df.loc[:, ~df.columns.duplicated()]
        frames.append(df)
        print(f"  {path.name}: {df.shape[0]} filas, {df.shape[1]} cols")

    if not frames:
        raise FileNotFoundError(f"No se encontraron CSVs en {folder}")

    combined = pd.concat(frames, ignore_index=True, sort=False)

    # Columnas calculadas post-merge
    combined = _add_calculated_cols(combined)

    # Clasificar filas
    combined, review, dropped = classify_rows(combined)

    print(f"\n  Filas conservadas (con manifiesto): {len(combined)}")
    print(f"  Filas para revisión (sin manifiesto, con facturación): {len(review)}")
    print(f"  Filas eliminadas (sin manifiesto, sin datos): {len(dropped)}")

    print(f"\nTotal tras limpieza: {combined.shape[0]} filas, {combined.shape[1]} columnas únicas")
    return combined, review, dropped


CORE_COLS    = ["manifiesto", "fecha_despacho", "origen", "destino", "cliente", "placa", "conductor"]
BILLING_COLS = ["entidad_financiera", "factura_electronica", "factura_no", "valor_pagado", "fecha_pago", "responsable"]

def classify_rows(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Clasifica las filas en tres grupos:
      - keep:   tiene manifiesto → conservar siempre
      - review: sin manifiesto pero con datos de facturación → separar para revisión
      - drop:   sin manifiesto y sin datos relevantes → eliminar

    Retorna (df_keep, df_review, df_drop)
    """
    man_col = "manifiesto"
    has_manifiesto = (
        df[man_col].notna() &
        ~df[man_col].astype(str).str.upper().str.strip().eq("ANULADO") &
        ~df[man_col].astype(str).str.strip().eq("")
    ) if man_col in df.columns else pd.Series(False, index=df.index)

    billing_present = [c for c in BILLING_COLS if c in df.columns]
    has_billing = df[billing_present].notna().any(axis=1) if billing_present else pd.Series(False, index=df.index)

    keep_mask   = has_manifiesto
    review_mask = ~has_manifiesto & has_billing
    drop_mask   = ~has_manifiesto & ~has_billing

    return (
        df[keep_mask].reset_index(drop=True),
        df[review_mask].copy(),
        df[drop_mask].copy(),
    )


def validate_manifiestos(cleaned: pd.DataFrame, folder: str | Path) -> pd.DataFrame:
    """
    Compara los manifiestos del DataFrame limpio contra los CSV originales.
    Retorna un DataFrame resumen por archivo con:
      - total_raw: manifiestos en el CSV original (excl. ANULADO)
      - total_clean: manifiestos en el cleaned
      - anulados: filas con ANULADO en el CSV original
      - perdidos: manifiestos que estaban en raw pero no en cleaned
    """
    folder = Path(folder)
    results = []

    clean_manifiestos = set(
        cleaned["manifiesto"].dropna().astype(str).str.strip().str.upper()
    )

    for path in sorted(folder.glob("*.csv")):
        raw = read_csv_with_header_detection(path)
        # Buscar columna manifiesto (puede tener espacios)
        man_col = next((c for c in raw.columns if "manifiesto" in c.lower()), None)
        if man_col is None:
            results.append({"archivo": path.name, "total_raw": None,
                            "total_clean": None, "anulados": None, "perdidos": None})
            continue

        valores = raw[man_col].astype(str).str.strip().str.upper()
        anulados = valores.eq("ANULADO").sum()
        validos  = valores[~valores.eq("ANULADO") & valores.notna() & ~valores.eq("NAN")]
        perdidos = validos[~validos.isin(clean_manifiestos)].tolist()

        file_clean = cleaned[cleaned["archivo_origen"] == path.stem.strip()]["manifiesto"]
        total_clean = file_clean.dropna().count()

        results.append({
            "archivo":     path.name,
            "total_raw":   len(validos),
            "total_clean": int(total_clean),
            "anulados":    int(anulados),
            "perdidos":    len(perdidos),
            "manifiestos_perdidos": ", ".join(perdidos[:20]),  # primeros 20
        })

    return pd.DataFrame(results)


DATE_COLS     = ["fecha_despacho", "fecha_cumplido", "fecha_pago", "fecha"]
MONEY_COLS    = ["valor_remesa", "flete_conductor", "anticipo", "valor_pagado"]
ID_COLS       = ["manifiesto", "consecutivo_semanal"]
DAYS_COLS     = ["dias_cumplido"]
# Columnas con demasiados nulos o datos ya cubiertos por otra columna
DEAD_COLS     = {"tiempo_lg_cargue", "tiempo_lg_descargue", "agencia"}

# ── Entidad financiera ────────────────────────────────────────────────────────

def _clean_entidad_financiera(val) -> tuple[str | None, str | None]:
    """
    Normaliza entidad_financiera. Devuelve (canónico, nota_novedades).

    Canónicos:
      TRANSF BANCOLOMBIA · TRANSF DAVIVIENDA · TRANSF BANCO DE BOGOTA
      CHEQUE BANCOLOMBIA · CHEQUE DAVIVIENDA · CHEQUE BANCO DE BOGOTA
      CHEQUE · TRANSF/CHEQUE · ANULADO · OTRO
    nota_novedades: texto a escribir en novedades si esa celda está vacía (o None).
    """
    if pd.isna(val):
        return None, None
    s = str(val).strip()
    if not s or re.fullmatch(r'[\s\t\n]+', s):
        return None, None
    if _PERSON_DATE_RE.search(s):
        return None, None
    if re.fullmatch(r'[\d\s]+', s):
        return None, None

    s_up = re.sub(r'[\s\t\n]+', ' ', _strip_accents(s).upper()).strip()

    # Personas coladas → descartar
    if re.search(r'\b(JOHANA|KAROL|MILENA|ARCINIEGAS|UNIGARRO)\b', s_up):
        return None, None

    # ANULADO
    if _PERSON_ANULADO_RE.match(_strip_accents(s_up.lower())):
        return "ANULADO", None

    # Detectar método y banco
    has_transf = bool(re.search(r'TRANS', s_up))
    has_cheque = bool(re.search(r'CHE|CHQ', s_up))
    has_col    = bool(re.search(r'B[^A-Z0-9]?COL|BCOL|BANCOLOMBIA|/COL\b', s_up))
    has_dav    = bool(re.search(r'(?<![A-Z])DAV|B[^A-Z0-9]?DA|DAVIVIENDA', s_up))
    has_bog    = bool(re.search(r'B[^A-Z0-9]?(?:BOG|BGTA|BTGA|GTA)|(?<![A-Z])(?:BOG|BGTA|BTGA|BOGOT)', s_up))

    if has_transf and has_cheque:
        # Banco se registra en novedades para no perder la info
        if has_col: return "TRANSF/CHEQUE", "BANCOLOMBIA"
        if has_bog: return "TRANSF/CHEQUE", "BANCO DE BOGOTA"
        return "TRANSF/CHEQUE", None

    if has_transf:
        if has_col: return "TRANSF BANCOLOMBIA", None
        if has_dav: return "TRANSF DAVIVIENDA", None
        if has_bog: return "TRANSF BANCO DE BOGOTA", None
        # Códigos truncados tipo TRANSF/B-, TRANSF/B-C, TRANSF/B-CO → Bancolombia
        if re.search(r'/B', s_up):
            return "TRANSF BANCOLOMBIA", None
        return None, None   # banco no identificable

    if has_cheque:
        if has_col: return "CHEQUE BANCOLOMBIA", None
        if has_dav: return "CHEQUE DAVIVIENDA", None
        if has_bog: return "CHEQUE BANCO DE BOGOTA", None
        # CHEQUE + número de cheque (ej. CHEQUE 142) → canónico CHEQUE, novedades: original
        if re.search(r'\d', s_up):
            return "CHEQUE", s_up
        return None, None

    # Valores que pasan a OTRO con la nota en novedades
    if re.search(r'CRUCE|INFORMATIVO|MULA SAL|YA PAGADO|RNDC', s_up):
        return "OTRO", s_up

    # Códigos cortos sin significado → descartar
    if re.fullmatch(r'[A-Z0-9][A-Z0-9/.\-]{0,9}', s_up):
        return None, None

    return None, None


# ── Departamentos ─────────────────────────────────────────────────────────────

_DEPT_ABBREV: dict[str, str] = {
    "Anti": "Antioquia",
    "Atla": "Atlántico",
    "Bogo": "Bogotá D.C.",
    "Boli": "Bolívar",
    "Boya": "Boyacá",
    "Cald": "Caldas",
    "Casa": "Casanare",
    "Cauc": "Cauca",
    "Cesa": "Cesar",
    "Cord": "Córdoba",
    "Cund": "Cundinamarca",
    "Huil": "Huila",
    "La G": "La Guajira",
    "Magd": "Magdalena",
    "Meta": "Meta",
    "Nari": "Nariño",
    "Nort": "Norte de Santander",
    "Quin": "Quindío",
    "Risa": "Risaralda",
    "Sant": "Santander",
    "Toli": "Tolima",
    "Vall": "Valle del Cauca",
    "Arau": "Arauca",
    "Caqu": "Caquetá",
    "Guav": "Guaviare",
    "Putu": "Putumayo",
    "Sucr": "Sucre",
}

# Palabras de departamento que pueden aparecer en el nombre de la ciudad
_DEPT_KEYWORDS: dict[str, str] = {
    "ANTIOQUIA":             "Antioquia",
    "ATLANTICO":             "Atlántico",
    "BOLIVAR":               "Bolívar",
    "BOYACA":                "Boyacá",
    "CALDAS":                "Caldas",
    "CASANARE":              "Casanare",
    "CAUCA":                 "Cauca",
    "CESAR":                 "Cesar",
    "CORDOBA":               "Córdoba",
    "CUNDINAMARCA":          "Cundinamarca",
    "HUILA":                 "Huila",
    "GUAJIRA":               "La Guajira",
    "MAGDALENA":             "Magdalena",
    "META":                  "Meta",
    "NARINO":                "Nariño",
    "NORTE DE SANTANDER":    "Norte de Santander",
    "QUINDIO":               "Quindío",
    "RISARALDA":             "Risaralda",
    "SANTANDER":             "Santander",
    "SUCRE":                 "Sucre",
    "TOLIMA":                "Tolima",
    "VALLE":                 "Valle del Cauca",
    "CAQUETA":               "Caquetá",
}

# Ciudades sin paréntesis cuyo departamento no se puede inferir del nombre
_CITY_DEPT_FALLBACK: dict[str, str] = {
    "IPIALES":              "Nariño",
    "PASTO":                "Nariño",
    "RIOHACHA":             "La Guajira",
    # Ciudades con nombre simple sin abreviatura de departamento
    "AGUSTIN CODAZZI":      "Cesar",
    "BELLO":                "Antioquia",
    "BOGOTA BOGOTA D. C.":  "Bogotá D.C.",
    "CALI":                 "Valle del Cauca",
    "CARTAGENA":            "Bolívar",
    "ESPINAL":              "Tolima",
    "GARZON":               "Huila",
    "GIRARDOTA":            "Antioquia",
    "GUACHUCAL":            "Nariño",
    "IBAGUE":               "Tolima",
    "LA PLATA":             "Huila",
    "MONTELIBANO":          "Córdoba",
    "MOSQUERA":             "Cundinamarca",
    "PEREIRA":              "Risaralda",
    "RIONEGRO":             "Antioquia",
    "TOTORO":               "Cauca",
}


# Nombres canónicos de municipios (clave: uppercase sin acentos, valor: nombre oficial)
_CITY_NAME_MAP: dict[str, str] = {
    "CALI":                 "SANTIAGO DE CALI",
    "PASTO":                "SAN JUAN DE PASTO",
    "CARTAGENA":            "CARTAGENA DE INDIAS",
    "BOGOTA":               "BOGOTÁ D.C.",
    "BOGOTA D.C.":          "BOGOTÁ D.C.",
    "BOGOTA D. C.":         "BOGOTÁ D.C.",
    "BOGOTA BOGOTA D. C.":  "BOGOTÁ D.C.",
    "SANTA FE DE BOGOTA":   "BOGOTÁ D.C.",
    "MEDELLIN":             "MEDELLÍN",
    "BARRANQUILLA":         "BARRANQUILLA",
    "BUCARAMANGA":          "BUCARAMANGA",
    "CUCUTA":               "CÚCUTA",
    "PEREIRA":              "PEREIRA",
    "MANIZALES":            "MANIZALES",
    "ARMENIA":              "ARMENIA",
    "IBAGUE":               "IBAGUÉ",
    "NEIVA":                "NEIVA",
    "VILLAVICENCIO":        "VILLAVICENCIO",
    "MONTERIA":             "MONTERÍA",
    "SINCELEJO":            "SINCELEJO",
    "VALLEDUPAR":           "VALLEDUPAR",
    "RIOHACHA":             "RIOHACHA",
    "SANTA MARTA":          "SANTA MARTA",
    "POPAYAN":              "POPAYÁN",
    "TUNJA":                "TUNJA",
    "FLORENCIA":            "FLORENCIA",
    "QUIBDO":               "QUIBDÓ",
    "MOCOA":                "MOCOA",
    "YOPAL":                "YOPAL",
    "ARAUCA":               "ARAUCA",
    "LETICIA":              "LETICIA",
    "MITU":                 "MITÚ",
    "PUERTO INIRIDA":       "PUERTO INÍRIDA",
    "SAN JOSE DEL GUAVIARE": "SAN JOSÉ DEL GUAVIARE",
    "PUERTO CARRENO":       "PUERTO CARREÑO",
}


def _normalize_city_name(val) -> str:
    """Estandariza nombre de municipio al nombre oficial colombiano."""
    if pd.isna(val) or not str(val).strip():
        return val
    key = re.sub(r'\s+', ' ', _strip_accents(str(val).strip().upper()))
    return _CITY_NAME_MAP.get(key, str(val).strip())


def _extract_departamento(ciudad_val) -> str | None:
    """Extrae el departamento de una celda de origen/destino."""
    if pd.isna(ciudad_val):
        return None
    s = str(ciudad_val).strip()

    # 1. Formato CIUDAD(Abrev)
    m = re.search(r'\(([^)]+)\)', s)
    if m:
        abbrev = m.group(1).strip()
        return _DEPT_ABBREV.get(abbrev, abbrev)   # si no está en el dict, devuelve la abrev tal cual

    # 2. Buscar palabras clave de departamento en el nombre (orden: más específico primero)
    s_norm = _strip_accents(s.upper())
    for keyword in sorted(_DEPT_KEYWORDS, key=len, reverse=True):
        if re.search(r'\b' + keyword + r'\b', s_norm):
            return _DEPT_KEYWORDS[keyword]

    # 3. Fallback por ciudad conocida
    city_key = _strip_accents(s.strip().upper())
    return _CITY_DEPT_FALLBACK.get(city_key)

# ── Limpieza de columnas de responsables ──────────────────────────────────────

_PERSON_ANULADO_RE = re.compile(
    r'^(anulad[ao]s?|anulaci[oó]n|cons\.?\s*anulado|cancelad[ao])$', re.IGNORECASE
)
_PERSON_DATE_RE = re.compile(
    r'\d{4}-\d{2}-\d{2}|\d{2}/\d{2}/\d{4}|\d{1,2}-[A-Za-z]{3}-\d{4}'
)

# Correcciones explícitas por columna (se aplican DESPUÉS de normalización base).
# Claves y valores ya en forma normalizada: UPPERCASE, sin acentos, espacios colapsados.
# None = valor inválido que debe descartarse.
_PERSON_COL_FIXES: dict[str, dict[str, str | None]] = {
    "responsable_estado_interno": {
        "M,ARCELA":         "MARCELA",
        "MARC ELA":         "MARCELA",
        "LILANA":           "LILIANA",
        "LILIANA OBREGON":  "LILIANA",
        "LILIANA  OBREGON": "LILIANA",
        "DAVIID":           "DAVID",
        "DAVIDF":           "DAVID",
        "DAVIF":            "DAVID",
        "CATHERN":          "CATHERIN",
        "KATTY":            "KATY",
        "VANESA":           "VANESSA",
        "VANESSA C":        "VANESSA",
        "VANESSA C.":       "VANESSA",
        # URBANO y numéricos se manejan en clean_person_cols, no aquí
    },
    "nombre_responsable": {
        "LILIANAOBREGON":   "LILIANA OBREGON",
        "OPERATIVO3":       "OPERATIVO 3",
        "OPERAIVO 3":       "OPERATIVO 3",
        "VANESA":           "VANESSA",
        ",":                None,   # basura → null
    },
    "responsable": {
        # Solo correcciones de typos; los valores raros se dejan con strip+upper
        "JOHANA UNIGARROI":  "JOHANA UNIGARRO",
        "KAROL ARCIBIEGAS":  "KAROL ARCINIEGAS",
        "KAROL ARCINIGAS":   "KAROL ARCINIEGAS",
    },
}

# Umbral de similitud fuzzy para agrupar variantes residuales (0-100).
# 92 > 90.9 (OPERATIVO 1 vs 2) evita fusionarlos pero < 93.8 (KAROL ARCIBIEGAS) los une.
_PERSON_FUZZY_THRESHOLD = 92

ESTADO_RULES = [
    (r'15\s*d[iíï]?[ai]', "PAGO A 15 DIAS"),
    (r'20\s*d[iíï]?[ai]', "PAGO A 20 DIAS"),
    (r'30\s*d[iíï]?[ai]', "PAGO A 30 DIAS"),
    (r'[58]\s*d[iíï]?[ai]', "PAGO A 5-8 DIAS"),
    # Contraentrega: cubre typos con letras faltantes/dobles/intercambiadas
    # COMTRAENTREGA, COTRAENTREGA, CNTRAENTREGA, CONTRANTREGA, CONTRAENTEGA,
    # CONTRAENTRAG, CONTREAENTREGA, CONTREANTREGA, CONTRAENTRTEGA, CONTRAAENTREGA
    (r'c.{0,3}tr.{0,6}(entr|ntrega|trega|rega)|entrega.*contra', "CONTRAENTREGA"),
    (r'pronto\s*(de\s*)?pago', "PRONTO PAGO"),
    (r'pago\s*normal|pago\s*oportuno|pago\s*normal', "PAGO NORMAL"),
    (r'^urbano$', "URBANO"),
    (r'^anulado$|^cons\.?\s*anulado$|^anulaci[oó]n$', "ANULADO"),
    (r'^pagado$', "PAGADO"),
    (r'inmediato', "PAGO INMEDIATO"),
    (r'prioritario', "PRIORITARIO"),
    (r'rndc', "RNDC"),
    # Nuevas reglas — mover valor original a novedades cuando se indica (tercer elemento True)
    # PAGAR + fecha → PAGO NORMAL
    (r'pagar\s+\d+\s+de\s+\w+|pagar\s+el\s+\d+|pagar\s+\d+\s+\w+', "PAGO NORMAL", True),
    # Paga apenas carga → CONTRAENTREGA
    (r'paga\s+apenas\s+carga', "CONTRAENTREGA"),
    # Typos de CONTRAENTREGA no cubiertos por la regla principal:
    # CONRA (solo), CONTRAENTEGA, CONRAENTREGA (falta 't'), etc.
    (r'^conra$|contra\s*en?t+ega|conra\s*entrega', "CONTRAENTREGA"),
    # NO PAGAR → ANULADO
    (r'^no\s+pagar$', "ANULADO", True),
    # Abonos y pagos parciales → PAGO NORMAL
    (r'\babono\b|pago\s+de\s+saldo|pago\s+en\s+mc|pago.*reemplazo', "PAGO NORMAL", True),
    # Indefinidos legítimos → OTROS (sin novedades)
    (r'^con\s+novedad$|^informativo$', "OTROS"),
]

# Enum canónico de estado (para validación DB)
ESTADO_ENUM = {
    "PAGO A 15 DIAS", "PAGO A 20 DIAS", "PAGO A 30 DIAS", "PAGO A 5-8 DIAS",
    "CONTRAENTREGA", "PRONTO PAGO", "PAGO NORMAL", "URBANO", "ANULADO",
    "PAGADO", "PAGO INMEDIATO", "PRIORITARIO", "RNDC", "OTROS",
}


def _clean_money(val: str) -> float | None:
    """Convierte string monetario colombiano a float. Ej: '$1.080.750' → 1080750.0"""
    if pd.isna(val):
        return None
    s = str(val).strip().upper()
    if s in ("ANULADO", "X", "", "NAN"):
        return None
    s = s.replace("$", "").replace(".", "").replace(",", "").strip()
    try:
        return float(s)
    except ValueError:
        return None


def _strip_accents(s: str) -> str:
    """Elimina acentos/diacríticos: PÁGO → PAGO, DÍAS → DIAS."""
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )


def _normalize_estado(val) -> tuple[str | None, str | None]:
    """
    Mapea variantes de estado a categoría canónica.
    Retorna (canonical, nota_novedades) donde nota_novedades es el valor
    original cuando la regla indica moverlo a novedades (tercer elemento True).
    """
    if pd.isna(val):
        return None, None
    s_raw = str(val).strip()
    # Fecha colada (ej. '2024-02-13 00:00:00') → descartada
    if re.match(r'\d{4}-\d{2}-\d{2}', s_raw):
        return None, None
    # Un solo carácter basura (ej. ',') → descartado
    if len(s_raw) <= 1:
        return None, None
    s = _strip_accents(s_raw.lower())
    s = re.sub(r'\s+', ' ', s)
    for rule in ESTADO_RULES:
        pattern, canonical = rule[0], rule[1]
        move_to_novedades = rule[2] if len(rule) > 2 else False
        if re.search(pattern, s, re.IGNORECASE):
            nota = s_raw if move_to_novedades else None
            return canonical, nota
    return "OTROS", None  # valores no reconocidos → enum OTROS (se reportan para reunión)


def _clean_dias(val) -> float | None:
    """Limpia dias_cumplido: elimina texto, fechas y valores absurdos."""
    if pd.isna(val):
        return None
    s = str(val).strip()
    if not s or s.upper() in ("ANULADO", "X", "NAN", "#VALUE!", "#¡VALOR!"):
        return None
    # Fecha colada
    if re.search(r'\d{4}-\d{2}-\d{2}|\d{2}/\d{2}/\d{4}', s):
        return None
    try:
        n = float(s)
        # Negativos absurdos (más de un año negativo) o más de 10 años positivos
        if n < -365 or n > 3650:
            return None
        return n
    except ValueError:
        return None


CONDICION_PAGO_RULES = [
    # C. CONTRAENTREGA, C.CONTRAENTREGA, etc.
    (r'^c\.?\s*contra', "CONTRAENTREGA"),
    # CONTING: 20-25 DH / CONTIG. PAGO 20-25 DIAS HABIL.
    (r'conti[gn]+', "CONTINGENCIA 20-25 DH"),
    # PAGO NORMAL(15DH) → PAGO NORMAL
    (r'pago\s*normal', "PAGO NORMAL"),
    (r'pronto\s*(de\s*)?pago', "PRONTO PAGO"),
    (r'contra.*entrega|c\.?\s*contra', "CONTRAENTREGA"),
]


def _clean_condicion_pago(val) -> str | None:
    """Normaliza condicion_pago: elimina numéricos sueltos y estandariza variantes."""
    if pd.isna(val):
        return None
    s = str(val).strip()
    if not s:
        return None
    if re.fullmatch(r'\d+', s):   # número suelto → dato de otra columna
        return None
    s_norm = _strip_accents(re.sub(r'\s+', ' ', s.lower()))
    for pattern, canonical in CONDICION_PAGO_RULES:
        if re.search(pattern, s_norm, re.IGNORECASE):
            return canonical
    return s.upper()


def _normalize_person_base(val) -> str | None:
    """
    Normalización base para columnas de responsables.

    - Fechas, char único de basura (',', '-', etc.)  → None
    - Variantes de ANULADO                           → "ANULADO"
    - Números puros y cadenas largas / con '//' / '$' → None
      (en responsable_estado_interno se sobreescriben a "OTROS" en clean_person_cols)
    - Resto                                          → UPPERCASE sin acentos, sin parentéticos
    """
    if pd.isna(val):
        return None
    s = str(val).strip()
    if not s or s in ("-", ".", ",", "x", "X", "/"):
        return None
    if _PERSON_DATE_RE.search(s):
        return None
    if re.fullmatch(r'[\d\s.,/-]+', s):
        return None
    s_lower = _strip_accents(re.sub(r'\s+', ' ', s.lower()).strip())
    if _PERSON_ANULADO_RE.match(s_lower):
        return "ANULADO"                          # estandarizar, no descartar
    if len(s) > 45 or '//' in s or '$' in s:      # notas/comentarios largos
        return None
    s = re.sub(r'\([^)]*\)', '', s).strip()        # quita "(RNDC)", "(RNDC)" etc.
    return re.sub(r'\s+', ' ', _strip_accents(s).upper()).strip() or None


def _fuzzy_canonical_map(
    freq: dict[str, int], threshold: int = _PERSON_FUZZY_THRESHOLD
) -> dict[str, str]:
    """
    Agrupa nombres similares por fuzzy-matching.
    El más frecuente en cada cluster se convierte en el valor canónico.
    Devuelve {nombre_normalizado → canónico}.
    """
    from rapidfuzz import fuzz

    names = sorted(freq, key=lambda x: -freq[x])   # más frecuentes primero
    cluster_map: dict[str, str] = {}

    for canonical in names:
        if canonical in cluster_map:
            continue
        cluster_map[canonical] = canonical
        for other in names:
            if other in cluster_map:
                continue
            if fuzz.token_sort_ratio(canonical, other) >= threshold:
                cluster_map[other] = canonical

    return cluster_map


def clean_person_cols(
    df: pd.DataFrame,
    cols: list[str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Limpia y estandariza las columnas de responsables.

    Pasos por columna:
      1. Normalización base (strip, uppercase, sin acentos, fechas/basura → None)
      2. Correcciones explícitas de ``_PERSON_COL_FIXES``
      3. Fuzzy-clustering de variantes residuales
      4. Aplica el mapa final al DataFrame

    Devuelve (df_limpio, report_df) donde ``report_df`` detalla
    cada valor original → canónico con su frecuencia y tipo de cambio.
    """
    from collections import Counter

    if cols is None:
        cols = ["responsable_estado_interno", "nombre_responsable", "responsable"]

    df = df.copy()
    report_rows: list[dict] = []

    for col in cols:
        if col not in df.columns:
            continue

        fixes = _PERSON_COL_FIXES.get(col, {})
        raw_series = df[col].astype(object)

        # --- paso 1+2: normalización base + correcciones explícitas --------
        base_map: dict[str, str | None] = {}
        for orig in raw_series.dropna().astype(str).unique():
            normed = _normalize_person_base(orig)
            # Correcciones explícitas usan la clave en forma normalizada
            if normed is not None and normed != "ANULADO":
                normed = fixes.get(normed, normed)
            base_map[orig] = normed

        # --- paso especial para responsable_estado_interno ------------------
        # Los valores que quedaron None por ser numéricos o demasiado largos
        # se mapean a "OTROS"; el texto original va a la columna novedades.
        novedades_originals: set[str] = set()
        if col == "responsable_estado_interno":
            for orig, normed in list(base_map.items()):
                if normed is None:
                    s = str(orig).strip()
                    if re.fullmatch(r'[\d\s.,/-]+', s) or len(s) > 30:
                        base_map[orig] = "OTROS"
                        novedades_originals.add(orig)

        # --- paso 3: fuzzy-clustering sobre valores válidos restantes -------
        # "ANULADO" y "OTROS" son canónicos fijos; no entran en el clustering
        _FIXED_CANONICALS = {"ANULADO", "OTROS"}
        normed_freq: Counter = Counter(
            v for v in raw_series.astype(str).map(
                lambda x: base_map.get(x) if pd.notna(x) else None  # type: ignore[arg-type]
            )
            if v is not None and v not in _FIXED_CANONICALS
        )
        fuzzy_map = _fuzzy_canonical_map(dict(normed_freq))

        # --- mapa final: original → canónico --------------------------------
        final_map: dict[str, str | None] = {
            orig: (
                normed if normed in _FIXED_CANONICALS
                else fuzzy_map.get(normed, normed) if normed is not None
                else None
            )
            for orig, normed in base_map.items()
        }

        # --- construir reporte ----------------------------------------------
        raw_counts: Counter = Counter(
            str(v) for v in raw_series if pd.notna(v)
        )
        for orig, canon in sorted(final_map.items(), key=lambda x: (x[1] or "", x[0])):
            n = raw_counts.get(orig, 0)
            if canon is None:
                tipo = "celda → nulo (no es nombre)"
            elif orig == canon:
                tipo = "sin cambio"
            else:
                tipo = "normalizado"
            report_rows.append({
                "columna":        col,
                "valor_original": orig,
                "valor_canonico": canon if canon is not None else "(nulo — fila conservada)",
                "n_registros":    n,
                "tipo_cambio":    tipo,
            })

        # --- aplicar al DataFrame -------------------------------------------
        original_series = df[col].copy()   # guardar antes de transformar
        df[col] = df[col].apply(
            lambda x: final_map.get(str(x), None) if pd.notna(x) else None
        )

        # --- columna novedades (solo responsable_estado_interno) ------------
        # Solo rellena donde novedades está vacío para no sobreescribir datos existentes
        if novedades_originals:
            if "novedades" not in df.columns:
                df["novedades"] = pd.NA
            mask = original_series.astype(str).isin(novedades_originals)
            fill_mask = mask & df["novedades"].isna()
            df.loc[fill_mask, "novedades"] = original_series[fill_mask]

    report = pd.DataFrame(report_rows)
    return df, report


_ESTADO_INTERNO_MAP: dict[str, str] = {
    "SOLO ESTABA PENDIENTE FACTURA ELECTRONICA PERO YA LLEGO": "FACTURA RECIBIDA",
    "NOVEDAD PENDIENTE POR RESOLVER":                          "NOVEDAD PENDIENTE",
}


def _normalize_estado_interno(val) -> str | None:
    """Normaliza estado_interno a los 6 valores canónicos del Enum."""
    if pd.isna(val):
        return None
    s = str(val).strip()
    if not s:
        return None
    key = _strip_accents(re.sub(r'\s+', ' ', s).upper()).strip()
    return _ESTADO_INTERNO_MAP.get(key, s.strip().upper())


def _apply_novedades(df: pd.DataFrame, notas: pd.Series) -> pd.DataFrame:
    """Escribe notas en novedades: rellena si vacío, concatena con | si ya hay valor."""
    if not notas.notna().any():
        return df
    if "novedades" not in df.columns:
        df["novedades"] = pd.NA
    fill_mask   = notas.notna() & df["novedades"].isna()
    concat_mask = notas.notna() & df["novedades"].notna()
    df.loc[fill_mask,   "novedades"] = notas[fill_mask]
    df.loc[concat_mask, "novedades"] = (
        df.loc[concat_mask, "novedades"].astype(str) + " | " + notas[concat_mask].astype(str)
    )
    return df


_RESPONSABLE_BLACKLIST = {
    "TRANSF/B-COL", "MULA SAL 511", "INFORMATIVO", "RNDC", "BANCOLOMBIA",
}

_AGENCIA_BLACKLIST = {"INFORMATIVO", "RNDC"}

# Nombres de una sola palabra y < 8 caracteres que son válidos
_RESPONSABLE_WHITELIST = {
    "JULIAN", "ELIANA", "HECTOR", "DAVID", "ANGIE", "KATY", "LILIANA",
    "MARCELA", "VANESSA", "CATHERIN", "INGRID", "YANETH", "ANGELA",
    "DIANA", "HAIR",
}


def _clean_responsable_col(val) -> tuple[str | None, str | None]:
    """
    Lista negra o contiene / → NULL + nota.
    Inusual (longitud < 4, $, dígitos) → conservar + nota.
    Una sola palabra, < 8 chars y no en whitelist → conservar + nota.
    """
    if pd.isna(val):
        return None, None
    s = str(val).strip()
    if not s:
        return None, None
    s_up = _strip_accents(re.sub(r'\s+', ' ', s).upper())
    if s_up in _RESPONSABLE_BLACKLIST or '/' in s:
        return None, f"[RESPONSABLE INUSUAL: {s}]"
    if len(s) < 4 or '$' in s or re.search(r'\d', s):
        return s, f"[RESPONSABLE INUSUAL: {s}]"
    if ' ' not in s_up and len(s_up) < 8 and s_up not in _RESPONSABLE_WHITELIST:
        return s, f"[RESPONSABLE INUSUAL: {s}]"
    return s, None


def _clean_agencia_desp(val) -> tuple[str | None, str | None]:
    """
    ANULADO (variantes) → 'ANULADO'.
    LETRAS+NÚMEROS (ej. CALI22866) → solo letras + nota.
    Lista negra (INFORMATIVO, RNDC) → NULL + nota.
    """
    if pd.isna(val):
        return None, None
    s = str(val).strip()
    if not s:
        return None, None
    s_up = _strip_accents(re.sub(r'\s+', ' ', s).upper())
    if _PERSON_ANULADO_RE.match(s_up.lower()):
        return "ANULADO", None
    if s_up in _AGENCIA_BLACKLIST:
        return None, f"[AGENCIA INUSUAL: {s}]"
    m = re.match(r'^([A-Z]+)\d', s_up)
    if m:
        return m.group(1), f"[AGENCIA INUSUAL: {s}]"
    return s_up, None


def _cedula(val) -> str | None:
    """Limpia ID numérico: elimina .0 residual y convierte a string entero."""
    if pd.isna(val):
        return None
    s = str(val).strip()
    if not s or s in (",", "-"):
        return None
    try:
        return str(int(float(s)))
    except ValueError:
        digits = re.sub(r'\D', '', s)
        return digits if digits else None


def _clean_celular(val) -> tuple[str | None, str | None]:
    """10 dígitos exactos → válido. Resto → NULL + nota."""
    base = _cedula(val)
    if base is None:
        return None, None
    if re.fullmatch(r'\d{10}', base):
        return base, None
    return None, f"[CELULAR INUSUAL: {str(val).strip()}]"


def _clean_cedula_conductor(val) -> tuple[str | None, str | None]:
    """6–12 dígitos → válido. Resto → NULL + nota."""
    base = _cedula(val)
    if base is None:
        return None, None
    if re.fullmatch(r'\d{6,12}', base):
        return base, None
    return None, f"[CEDULA INUSUAL: {str(val).strip()}]"


def clean_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpieza de valores del DataFrame combinado:
      1. Fechas → date (sin hora)
      2. Monetarios → float
      3. Identificadores → int string sin decimales
      4. Días → numérico con filtro de texto/absurdos
      5. Estado → categorías canónicas
      6. Condición de pago → elimina numéricos sueltos
      7. mes_facturacion → entero nullable
      8. Elimina columnas Unnamed residuales y columnas muertas
    """
    df = df.copy()

    # 1. Fechas
    for col in DATE_COLS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    # 2. Monetarios
    for col in MONEY_COLS:
        if col in df.columns:
            df[col] = df[col].apply(_clean_money)

    # 3. Identificadores: quitar .0 y convertir a string limpio
    for col in ID_COLS:
        if col in df.columns:
            df[col] = (
                pd.to_numeric(df[col], errors="coerce")
                .apply(lambda x: str(int(x)) if pd.notna(x) else None)
            )

    # 4. Días cumplido: texto y valores absurdos → None
    for col in DAYS_COLS:
        if col in df.columns:
            df[col] = df[col].apply(_clean_dias)

    # 5. Estado
    if "estado" in df.columns:
        parsed_estado = df["estado"].apply(_normalize_estado)
        df["estado"] = parsed_estado.apply(lambda t: t[0])
        notas_estado = parsed_estado.apply(lambda t: t[1])
        if notas_estado.notna().any():
            if "novedades" not in df.columns:
                df["novedades"] = pd.NA
            fill_mask   = notas_estado.notna() & df["novedades"].isna()
            concat_mask = notas_estado.notna() & df["novedades"].notna()
            df.loc[fill_mask, "novedades"] = notas_estado[fill_mask]
            df.loc[concat_mask, "novedades"] = (
                df.loc[concat_mask, "novedades"].astype(str)
                + " | "
                + notas_estado[concat_mask].astype(str)
            )

    # 6. Condición de pago
    if "condicion_pago" in df.columns:
        df["condicion_pago"] = df["condicion_pago"].apply(_clean_condicion_pago)

    # 6b. Agencia despachadora: ANULADO, LETRAS+NÚMEROS, lista negra
    if "agencia_despachadora" in df.columns:
        parsed = df["agencia_despachadora"].apply(_clean_agencia_desp)
        df["agencia_despachadora"] = parsed.apply(lambda t: t[0])
        df = _apply_novedades(df, parsed.apply(lambda t: t[1]))

    # 6c. Entidad financiera → categorías canónicas + novedades para casos especiales
    if "entidad_financiera" in df.columns:
        parsed = df["entidad_financiera"].apply(_clean_entidad_financiera)
        df["entidad_financiera"] = parsed.apply(lambda t: t[0])
        df = _apply_novedades(df, parsed.apply(lambda t: t[1]))

    # 6e. Estado interno → 6 valores canónicos
    if "estado_interno" in df.columns:
        df["estado_interno"] = df["estado_interno"].apply(_normalize_estado_interno)

    # 6f. Responsable: lista negra y valores inusuales
    if "responsable" in df.columns:
        parsed = df["responsable"].apply(_clean_responsable_col)
        df["responsable"] = parsed.apply(lambda t: t[0])
        df = _apply_novedades(df, parsed.apply(lambda t: t[1]))

    # 6g. Celular: limpiar .0, validar 10 dígitos
    if "celular" in df.columns:
        parsed = df["celular"].apply(_clean_celular)
        df["celular"] = parsed.apply(lambda t: t[0])
        df = _apply_novedades(df, parsed.apply(lambda t: t[1]))

    # 6h. Cédula conductor: limpiar .0, validar 6–12 dígitos
    if "cedula_conductor" in df.columns:
        parsed = df["cedula_conductor"].apply(_clean_cedula_conductor)
        df["cedula_conductor"] = parsed.apply(lambda t: t[0])
        df = _apply_novedades(df, parsed.apply(lambda t: t[1]))

    # 6d. Columnas de departamento junto a origen y destino
    for city_col, dept_col in [("origen", "departamento_origen"), ("destino", "departamento_destino")]:
        if city_col in df.columns:
            dept_values = df[city_col].apply(_extract_departamento)
            idx = df.columns.get_loc(city_col) + 1
            df.insert(idx, dept_col, dept_values)
            df[city_col] = df[city_col].str.replace(r'\s*\([^)]*\)', '', regex=True).str.strip()
            df[city_col] = df[city_col].apply(_normalize_city_name)

    # 7. mes_facturacion como entero nullable (evita '4.0')
    if "mes_facturacion" in df.columns:
        df["mes_facturacion"] = pd.to_numeric(df["mes_facturacion"], errors="coerce").astype("Int64")

    # 8a. Eliminar columnas Unnamed residuales
    unnamed = [c for c in df.columns if re.match(r'^unnamed', c, re.IGNORECASE)]
    if unnamed:
        df = df.drop(columns=unnamed)

    # 8b. Columnas muertas (100 % nulos o cubiertas por otra columna)
    # Antes de eliminar 'agencia': propagar a agencia_despachadora donde esté vacía
    if "agencia" in df.columns and "agencia_despachadora" in df.columns:
        fill_mask = df["agencia"].notna() & df["agencia_despachadora"].isna()
        df.loc[fill_mask, "agencia_despachadora"] = (
            df.loc[fill_mask, "agencia"].apply(lambda v: _clean_agencia_desp(v)[0])
        )
        remaining = int(df["agencia"].notna().sum())
        if remaining:
            print(f"  Advertencia: {remaining} filas con 'agencia' no nula tras copiar → agencia_despachadora ya tenía valor en esas filas")

    dead = [c for c in df.columns if c in DEAD_COLS]
    if dead:
        df = df.drop(columns=dead)

    return df


def expand_remesas(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Expande filas con múltiples remesas separadas por ';'.
    - Filas simples: no cambian.
    - Filas multi-remesa: se expanden repitiendo todos los demás campos.
    Retorna (df_expandido, df_multi_report) donde df_multi_report
    contiene las filas originales antes de expandir para la reunión.
    """
    df = df.copy()
    df["remesas"] = df["remesas"].astype(str).str.strip()

    multi_mask = df["remesas"].str.contains(";", na=False)
    df_multi_report = df[multi_mask].copy()

    df["remesas"] = df["remesas"].str.split(r"\s*;\s*")
    df = df.explode("remesas").reset_index(drop=True)
    df["remesas"] = df["remesas"].str.strip()

    return df, df_multi_report


def _add_calculated_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega columnas derivadas que se calculan automáticamente:
      - mes_facturacion: mes numérico de la fecha de factura
      - dias_para_facturar: días entre despacho y emisión de factura
    """
    fecha_factura  = pd.to_datetime(df.get("fecha"),          errors="coerce")
    fecha_despacho = pd.to_datetime(df.get("fecha_despacho"), errors="coerce")

    df["mes_facturacion"]   = fecha_factura.dt.month
    df["dias_para_facturar"] = (fecha_factura - fecha_despacho).dt.days

    return df


months_files = list(Path('data_sheets/').glob('*.csv'))

def schema_to_df(schema: dict) -> pd.DataFrame:
    """Convierte el resultado de columns_schema() a un DataFrame."""
    rows = []

    for col, info in schema['universal'].items():
        rows.append({
            "categoria":  "universal",
            "columna":    col,
            "n_archivos": info["n_archivos"],
            "archivos":   "",
            "raw_names":  ", ".join(info["raw_names"]),
            "dtypes":     ", ".join(sorted(set(info["dtypes"].values()))),
            "similares":  "",
        })
    for col, info in sorted(schema['parciales'].items(), key=lambda x: -x[1]['n_archivos']):
        rows.append({
            "categoria":  "parcial",
            "columna":    col,
            "n_archivos": info["n_archivos"],
            "archivos":   ", ".join(info["archivos"]),
            "raw_names":  ", ".join(info["raw_names"]),
            "dtypes":     ", ".join(sorted(set(info["dtypes"].values()))),
            "similares":  "",
        })
    for grupo in schema['similares']:
        for col in grupo['columnas']:
            rows.append({
                "categoria":  "similar",
                "columna":    col,
                "n_archivos": grupo["n_archivos"][col],
                "archivos":   "",
                "raw_names":  ", ".join(grupo["raw_names"][col]),
                "dtypes":     "",
                "similares":  " | ".join(c for c in grupo["columnas"] if c != col),
            })
    for col, info in schema['unicas'].items():
        rows.append({
            "categoria":  "unica",
            "columna":    col,
            "n_archivos": 1,
            "archivos":   ", ".join(info["archivos"]),
            "raw_names":  ", ".join(info["raw_names"]),
            "dtypes":     ", ".join(sorted(set(info["dtypes"].values()))),
            "similares":  "",
        })

    return pd.DataFrame(rows).sort_values("n_archivos", ascending=False).reset_index(drop=True)


def summary_from_df(df: pd.DataFrame, label: str) -> pd.DataFrame:
    """Genera un summary de nulos y dtypes desde un DataFrame ya cargado."""
    rows = []
    for i in range(len(df.columns)):
        col    = df.columns[i]
        series = df.iloc[:, i]
        rows.append({
            "etapa":      label,
            "column":     col,
            "dtype":      str(series.dtype),
            "null_count": int(series.isna().sum()),
            "null_pct":   round(series.isna().mean() * 100, 2),
        })
    return pd.DataFrame(rows)


def summary_before_per_file(df_list: list[pd.DataFrame], files: list) -> pd.DataFrame:
    """
    Genera un resumen por columna y archivo de los CSVs originales.
    Cada fila representa una columna de un archivo específico con su dtype,
    conteo de nulos y porcentaje de nulos.
    """
    rows = []
    for df, f in zip(df_list, files):
        n_filas = len(df)
        for col in df.columns:
            series = df[col]
            null_count = int(series.isna().sum())
            null_pct = round(series.isna().mean() * 100, 2) if n_filas > 0 else 0.0
            rows.append({
                "columna":    col,
                "archivo":    f.name,
                "dtype":      str(series.dtype),
                "null_count": null_count,
                "null_pct":   null_pct,
            })
    return pd.DataFrame(rows)


_DISCRETE_MAX_UNIQUE = 60   # umbral: si n_unicos <= esto y no es numérico → discreto


def _make_enum_suggestion(col: str, values: list) -> str:
    """Genera un bloque Python Enum para una columna categórica."""
    class_name = "".join(w.capitalize() for w in re.split(r"[_\s]+", col))
    lines = [f"class {class_name}(str, Enum):"]
    for v in sorted(str(x) for x in values if x is not None and str(x).strip()):
        member = re.sub(r"[^A-Z0-9_]", "_", str(v).strip().upper())
        member = re.sub(r"_+", "_", member).strip("_")
        if not member:
            continue
        if member[0].isdigit():
            member = "V_" + member
        lines.append(f'    {member} = "{v}"')
    return "\n".join(lines)


def column_value_profile(df: pd.DataFrame) -> pd.DataFrame:
    """
    Analiza cada columna del DataFrame limpio y devuelve un perfil con:
    - Columnas discretas/categóricas: valores únicos y definición Enum sugerida
    - Columnas numéricas: min, max, media, mediana, p25, p75, p95
    - Columnas de fecha: min y max
    """
    import datetime

    rows = []
    for col in df.columns:
        series = df[col].dropna()
        n_total   = len(df[col])
        n_nulos   = int(df[col].isna().sum())
        pct_nulos = round(n_nulos / n_total * 100, 1) if n_total else 0.0
        n_unicos  = int(series.nunique())

        row = {
            "columna":      col,
            "n_total":      n_total,
            "n_nulos":      n_nulos,
            "pct_nulos":    pct_nulos,
            "n_unicos":     n_unicos,
            "tipo_inferido": "",
            # Numéricos
            "min":     None, "max":     None, "media":   None,
            "mediana": None, "p25":     None, "p75":     None, "p95": None,
            # Discretos
            "valores_unicos":  "",
            "enum_sugerido":   "",
        }

        # ── Fechas ────────────────────────────────────────────────
        if pd.api.types.is_datetime64_any_dtype(series):
            row["tipo_inferido"] = "fecha"
            row["min"] = str(series.min().date())
            row["max"] = str(series.max().date())

        elif series.apply(lambda x: isinstance(x, datetime.date)).any():
            row["tipo_inferido"] = "fecha"
            try:
                row["min"] = str(series.min())
                row["max"] = str(series.max())
            except Exception:
                pass

        # ── Numéricos ─────────────────────────────────────────────
        elif pd.api.types.is_numeric_dtype(series):
            row["tipo_inferido"] = "numerico"
            row["min"]    = round(float(series.min()),    2)
            row["max"]    = round(float(series.max()),    2)
            row["media"]  = round(float(series.mean()),   2)
            row["mediana"]= round(float(series.median()), 2)
            row["p25"]    = round(float(series.quantile(0.25)), 2)
            row["p75"]    = round(float(series.quantile(0.75)), 2)
            row["p95"]    = round(float(series.quantile(0.95)), 2)
            # Pocos únicos numéricos → también anotar como posible discreto
            if n_unicos <= 20:
                vals = sorted(series.unique())
                row["valores_unicos"] = ", ".join(str(v) for v in vals)

        # ── Discretos / categóricos ───────────────────────────────
        elif n_unicos <= _DISCRETE_MAX_UNIQUE:
            row["tipo_inferido"] = "discreto"
            vals = sorted(str(v) for v in series.unique() if str(v).strip())
            row["valores_unicos"] = ", ".join(vals)
            row["enum_sugerido"]  = _make_enum_suggestion(col, vals)

        # ── Texto libre ───────────────────────────────────────────
        else:
            row["tipo_inferido"] = "texto_libre"
            sample = series.dropna().head(5).tolist()
            row["valores_unicos"] = " | ".join(str(v) for v in sample) + " …"

        rows.append(row)

    return pd.DataFrame(rows)


def fix_review_rows(
    combined: pd.DataFrame,
    review: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Post-procesa las filas de revisión tras classify_rows():

    2a. Elimina las 77 filas de NOVIEMBRE 2025 (datos incorrectos conocidos).
    2b. Filas con manifiesto='ANULADO' y consecutivo_semanal válido:
        estima el manifiesto sumando 1 al anterior dentro del mismo
        archivo+semana y mueve la fila a combined con estado='ANULADO'.
        Las que no se pueden estimar se descartan.
    2c. Elimina filas sin ningún dato útil en CORE_COLS (excl. manifiesto).
    2d. Elimina filas con solo datos de factura pero sin información operacional.

    Retorna (combined_actualizado, review_actualizado).
    """
    man_col    = "manifiesto"
    consec_col = "consecutivo_semanal"

    # ── 2a ───────────────────────────────────────────────────────────────────
    review = review[review.get("archivo_origen", pd.Series(dtype=str)) != "NOVIEMBRE 2025"].copy()

    # ── 2b ───────────────────────────────────────────────────────────────────
    if man_col in review.columns and consec_col in review.columns:
        anulado_mask = review[man_col].astype(str).str.strip().str.upper() == "ANULADO"

        if anulado_mask.any():
            # Preparar tabla de lookup en combined con valores numéricos
            lookup = combined.copy()
            lookup["_consec_num"] = pd.to_numeric(lookup[consec_col], errors="coerce")
            lookup["_manif_num"]  = pd.to_numeric(lookup[man_col],    errors="coerce")
            lookup = lookup.dropna(subset=["_consec_num", "_manif_num"])
            lookup = lookup.sort_values(["archivo_origen", "semana", "_consec_num"])

            anulado_rows = review[anulado_mask].copy()
            anulado_rows["_consec_num"] = pd.to_numeric(
                anulado_rows[consec_col], errors="coerce"
            )

            recovered_indices: list = []
            recovered_rows:    list = []

            for idx, row in anulado_rows.iterrows():
                consec  = row.get("_consec_num")
                archivo = row.get("archivo_origen")
                semana  = row.get("semana")

                if pd.isna(consec):
                    continue  # sin consecutivo válido → se descarta en el drop al final

                cand_mask = (
                    (lookup["archivo_origen"] == archivo) &
                    (lookup["_consec_num"]    == consec - 1)
                )
                if semana is not None and "semana" in lookup.columns:
                    cand_mask &= lookup["semana"] == semana

                candidates = lookup[cand_mask]
                if not candidates.empty:
                    prev_manif = candidates.iloc[-1]["_manif_num"]
                    new_row = row.drop(labels=["_consec_num"], errors="ignore").copy()
                    new_row[man_col]  = str(int(prev_manif + 1))
                    new_row["estado"] = "ANULADO"
                    recovered_indices.append(idx)
                    recovered_rows.append(new_row)

            if recovered_rows:
                recovered_df = pd.DataFrame(recovered_rows)
                combined = pd.concat([combined, recovered_df], ignore_index=True)
                print(f"  fix_review: {len(recovered_rows)} filas ANULADO recuperadas con manifiesto estimado")

            n_dropped_anulado = anulado_mask.sum() - len(recovered_rows)
            if n_dropped_anulado:
                print(f"  fix_review: {n_dropped_anulado} filas ANULADO sin manifiesto estimable → eliminadas")

            # Todas las filas ANULADO salen de review (recuperadas o eliminadas)
            review = review[~anulado_mask].copy()

    # ── 2c — filas sin ningún dato útil en CORE_COLS ─────────────────────────
    core_check = [c for c in CORE_COLS if c != "manifiesto" and c in review.columns]
    if core_check:
        all_core_null = review[core_check].isna().all(axis=1)
        n_2c = int(all_core_null.sum())
        if n_2c:
            print(f"  fix_review: {n_2c} filas sin datos útiles (CORE_COLS vacíos) → eliminadas")
        review = review[~all_core_null].copy()

    # ── 2d — filas con factura pero sin datos operacionales ──────────────────
    factura_cols = [c for c in ["factura_no", "factura_electronica", "valor_pagado"] if c in review.columns]
    if factura_cols and core_check:
        has_factura_only = (
            review[factura_cols].notna().any(axis=1) &
            review[core_check].isna().all(axis=1)
        )
        n_2d = int(has_factura_only.sum())
        if n_2d:
            print(f"  fix_review: {n_2d} filas con solo factura y sin datos operacionales → eliminadas")
        review = review[~has_factura_only].copy()

    return combined, review.reset_index(drop=True)


if __name__ == "__main__":
    INFORME_PATH = Path("informes/informe_etl.xlsx")
    INFORME_PATH.parent.mkdir(parents=True, exist_ok=True)

    # ── FASE 1 — Análisis ANTES de limpieza ──────────────────────
    print("=" * 60)
    print("FASE 1 — Análisis ANTES de limpieza")
    print("=" * 60)

    raw_dfs = [read_csv_with_header_detection(f) for f in months_files]
    summary_before = summary_before_per_file(raw_dfs, months_files)
    schema_before_df = schema_to_df(columns_schema(raw_dfs, months_files))

    # ── FASE 2 — Limpieza y clasificación ────────────────────────
    print()
    print("=" * 60)
    print("FASE 2 — Limpieza y clasificación")
    print("=" * 60)

    combined, review, dropped = load_all('data_sheets/')

    # ── Post-proceso de filas de revisión ────────────────────────
    combined, review = fix_review_rows(combined, review)

    if len(review):
        review_report = (
            review.groupby("archivo_origen")
            .size().reset_index(name="filas_revision")
            .sort_values("filas_revision", ascending=False)
        )
        print("\n  Filas para revisión por archivo:")
        print(review_report.to_string(index=False))

    if len(dropped):
        dropped_report = (
            dropped.groupby("archivo_origen")
            .size().reset_index(name="filas_eliminadas")
            .sort_values("filas_eliminadas", ascending=False)
        )

    # ── FASE 3 — Validación de manifiestos ───────────────────────
    print()
    print("=" * 60)
    print("FASE 3 — Validación de manifiestos")
    print("=" * 60)

    validation = validate_manifiestos(combined, 'data_sheets/')

    total_raw   = validation["total_raw"].sum()
    total_clean = validation["total_clean"].sum()
    diferencia  = total_raw - total_clean
    total_perdidos = validation["perdidos"].sum()

    print(f"\n  {'Manifiestos en raw:':<45} {int(total_raw):>6}")
    print(f"  {'Manifiestos en cleaned:':<45} {int(total_clean):>6}")
    print(f"  {'Diferencia:':<45} {int(diferencia):>6}  {'OK' if diferencia == 0 else 'HAY PERDIDOS'}")
    print(f"  {'No encontrados en cleaned:':<45} {int(total_perdidos):>6}")

    if total_perdidos:
        print("\n  Archivos con pérdidas:")
        print(validation[validation["perdidos"] > 0][
            ["archivo", "total_raw", "total_clean", "anulados", "perdidos"]
        ].to_string(index=False))

    # ── FASE 4 — Limpieza de valores y expansión ─────────────────
    print()
    print("=" * 60)
    print("FASE 4 — Limpieza de valores y expansión")
    print("=" * 60)

    if "estado" in combined.columns:
        combined["_estado_original"] = combined["estado"].copy()

    combined = clean_values(combined)

    otros_report = pd.DataFrame()
    if "_estado_original" in combined.columns:
        otros_mask = combined["estado"] == "OTROS"
        if otros_mask.any():
            report_cols = [c for c in [
                "manifiesto", "archivo_origen", "mes", "año",
                "fecha_despacho", "cliente", "origen", "destino",
                "_estado_original", "estado",
            ] if c in combined.columns]
            otros_report = combined[otros_mask][report_cols].copy()
            otros_report = otros_report.rename(columns={"_estado_original": "estado_original"})
            print(f"  Estados 'OTROS' (para reunión): {otros_mask.sum()} filas")
        combined = combined.drop(columns=["_estado_original"])

    summary_after = summary_from_df(combined, "despues")

    combined, multi_remesas_report = expand_remesas(combined)
    if len(multi_remesas_report):
        print(f"  Filas con multi-remesa (para reunión): {len(multi_remesas_report)}")

    # ── FASE 4b — Limpieza de columnas de responsables ───────────────
    print()
    print("=" * 60)
    print("FASE 4b — Normalización de responsables")
    print("=" * 60)

    combined, person_report = clean_person_cols(combined)

    for col in ["responsable_estado_interno", "nombre_responsable", "responsable"]:
        sub = person_report[person_report["columna"] == col]
        if sub.empty:
            continue
        normalizados = sub[sub["tipo_cambio"] == "normalizado"]["n_registros"].sum()
        eliminados   = sub[sub["tipo_cambio"] == "eliminado"]["n_registros"].sum()
        n_canon      = sub[sub["tipo_cambio"] != "eliminado"]["valor_canonico"].nunique()
        print(f"  {col}: {n_canon} valores canónicos | "
              f"{normalizados} registros normalizados | {eliminados} eliminados")

    # ── FASE 5 — Perfil de columnas (discretas / numéricas / fechas) ──
    print()
    print("=" * 60)
    print("FASE 5 — Perfil de columnas (discretas / numéricas / fechas)")
    print("=" * 60)

    col_profile = column_value_profile(combined)
    discretas = col_profile[col_profile["tipo_inferido"] == "discreto"]
    numericas = col_profile[col_profile["tipo_inferido"] == "numerico"]
    print(f"  Columnas discretas (Enum): {len(discretas)}")
    print(f"  Columnas numéricas (rango): {len(numericas)}")

    # ── EXPORTAR ─────────────────────────────────────────────────
    # 1. Datos limpios → CSV
    Path("cleaned_data").mkdir(exist_ok=True)
    combined.to_csv('cleaned_data/individual_cleaned.csv', index=False)

    # 2. Todos los informes → un solo Excel con hojas
    with pd.ExcelWriter(INFORME_PATH, engine="openpyxl") as writer:
        summary_before.to_excel(writer, sheet_name="resumen_antes", index=False)
        summary_after.to_excel(writer, sheet_name="resumen_despues", index=False)
        schema_before_df.to_excel(writer, sheet_name="esquema_antes", index=False)
        validation.to_excel(writer, sheet_name="validacion_manifiestos", index=False)
        if len(review):
            review.to_excel(writer, sheet_name="filas_revision", index=False)
        if len(dropped):
            dropped_report.to_excel(writer, sheet_name="filas_eliminadas", index=False)
        if len(otros_report):
            otros_report.to_excel(writer, sheet_name="estado_otros", index=False)
        if len(multi_remesas_report):
            multi_remesas_report.to_excel(writer, sheet_name="multi_remesas", index=False)
        col_profile.to_excel(writer, sheet_name="perfil_columnas", index=False)
        if len(person_report):
            # Hoja detallada: una fila por (valor_original, valor_canonico)
            person_report.sort_values(
                ["columna", "valor_canonico", "n_registros"],
                ascending=[True, True, False],
            ).to_excel(writer, sheet_name="normalizacion_responsables", index=False)

    print()
    print("Archivos generados:")
    print(f"  cleaned_data/individual_cleaned.csv  ({len(combined)} filas)")
    print(f"  {INFORME_PATH}  (todos los informes)")