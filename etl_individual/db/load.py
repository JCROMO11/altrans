"""
Carga el CSV individual_cleaned.csv a PostgreSQL.

Flujo:
  1. Conectar y verificar la conexión
  2. create_all(checkfirst=True) → crea tablas que faltan, salta las existentes
  3. Cargar catálogos (propietarios → conductores → vehiculos → clientes
                       → lugares → agencias → responsables)
  4. Cargar hechos (manifiestos → remesas → pagos_conductor → facturacion)

Cada INSERT usa ON CONFLICT DO NOTHING: el script es idempotente
y puede correrse múltiples veces sin duplicar datos.

Uso:
    DATABASE_URL=postgresql://user:pass@localhost:5432/altrans python -m etl_individual.db.load
"""

import os
import re
import pandas as pd
from pathlib import Path

from sqlalchemy import create_engine, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from .models import (
    Base, Agencia, Cliente, Conductor, Facturacion,
    Lugar, Manifiesto, PagoConductor, Propietario,
    Remesa, Responsable, Vehiculo,
)

# ── Coerciones de tipo ────────────────────────────────────────────────

def _str(val) -> str | None:
    if pd.isna(val):
        return None
    s = str(val).strip()
    if s.upper() in ('NAN', 'NONE', '-'):
        return None
    return s or None

def _date(val):
    if pd.isna(val):
        return None
    try:
        return pd.to_datetime(val).date()
    except Exception:
        return None


def _int(val) -> int | None:
    if pd.isna(val):
        return None
    try:
        return int(float(val))
    except Exception:
        return None


def _float(val) -> float | None:
    if pd.isna(val):
        return None
    try:
        return float(val)
    except Exception:
        return None


def _cedula(val) -> str | None:
    """Normaliza cédulas: '12345.0' y '12345' → '12345'. Solo dígitos; cualquier otra cosa → None."""
    if pd.isna(val):
        return None
    try:
        result = str(int(float(val))).strip()
        return result if re.fullmatch(r'\d+', result) else None
    except Exception:
        result = str(val).strip()
        return result if re.fullmatch(r'\d+', result) else None


# ── Helpers de dominio ────────────────────────────────────────────────

_EMPRESA_RE = re.compile(
    r'\b(s\.?a\.?s?\.?|ltda\.?|s\.?c\.?a\.?|e\.?u\.?|s\.?p\.?a\.?|'
    r'corp\.?|cia\.?|transportes?|log[ií]stica|express|camiones?)\b',
    re.IGNORECASE,
)

def _tipo_propietario(nombre: str) -> str:
    return "EMPRESA" if _EMPRESA_RE.search(nombre) else "PERSONA"


def _parse_lugar(raw: str) -> tuple[str, str | None]:
    """'PALMIRA(Vall)' → ('PALMIRA(Vall)', 'PALMIRA')"""
    m = re.match(r'^(.+?)\((.+?)\)$', raw.strip())
    if m:
        return raw.strip(), m.group(1).strip()
    return raw.strip(), raw.strip()


def _clean_cliente(val) -> str | None:
    """'PISOTRANS S.A; PISOTRANS S.A; ...' → 'PISOTRANS S.A'"""
    if pd.isna(val):
        return None
    return str(val).split(";")[0].strip() or None


# ── Helper genérico para catálogos ────────────────────────────────────

def _insert_catalog(session: Session, model, rows: list[dict], conflict_col: str) -> dict[str, int]:
    """
    Inserta filas con ON CONFLICT DO NOTHING en conflict_col,
    luego devuelve un mapa {valor_conflict_col → id} con todos los registros.
    """
    if rows:
        stmt = pg_insert(model).values(rows).on_conflict_do_nothing(index_elements=[conflict_col])
        session.execute(stmt)
        session.commit()
    all_rows = session.execute(select(model)).scalars().all()
    return {getattr(r, conflict_col): r.id for r in all_rows}


# ── Catálogos ─────────────────────────────────────────────────────────

def _load_propietarios(df: pd.DataFrame, session: Session) -> dict[str, int]:
    nombres = df["propietario"].dropna().str.strip().unique()
    rows = [{"tipo": _tipo_propietario(n), "nombre": n} for n in nombres if n]
    return _insert_catalog(session, Propietario, rows, "nombre")


def _load_conductores(df: pd.DataFrame, session: Session) -> tuple[dict[str, int], dict[str, int]]:
    """
    Retorna (cedula→id, nombre→id).
    Conductores sin cédula se deduplicaron por nombre dentro del lote,
    pero no están protegidos contra re-inserciones entre ejecuciones.
    """
    sub = df[["conductor", "cedula_conductor", "celular"]].dropna(subset=["conductor"]).copy()
    sub["cedula_conductor"] = sub["cedula_conductor"].apply(_cedula)
    sub["celular"]          = sub["celular"].apply(_cedula)

    with_cedula = sub[sub["cedula_conductor"].notna()].drop_duplicates("cedula_conductor")
    no_cedula   = sub[sub["cedula_conductor"].isna()].drop_duplicates("conductor")
    dedup = pd.concat([with_cedula, no_cedula], ignore_index=True)

    rows = [
        {
            "cedula":  r.cedula_conductor,
            "nombre":  _str(r.conductor),
            "celular": _str(r.celular),
        }
        for r in dedup.itertuples()
        if _str(r.conductor)
    ]

    if rows:
        # ON CONFLICT solo actúa sobre cedula no-nula (NULL != NULL en Postgres)
        stmt = pg_insert(Conductor).values(rows).on_conflict_do_nothing(index_elements=["cedula"])
        session.execute(stmt)
        session.commit()

    all_rows = session.execute(select(Conductor)).scalars().all()
    cedula_map = {r.cedula: r.id for r in all_rows if r.cedula}
    nombre_map = {r.nombre: r.id for r in all_rows}
    return cedula_map, nombre_map


_PLACA_RE = re.compile(r'^[A-Z0-9]{5,8}$', re.IGNORECASE)

def _is_valid_placa(val: str | None) -> bool:
    return bool(val and _PLACA_RE.match(val))


def _load_vehiculos(df: pd.DataFrame, session: Session, prop_map: dict[str, int]) -> None:
    cols = [c for c in ["placa", "tipo_vehiculo", "propietario"] if c in df.columns]
    sub = df[cols].dropna(subset=["placa"]).drop_duplicates("placa")
    rows = []
    skipped = 0
    for r in sub.itertuples():
        placa = _str(r.placa)
        if not _is_valid_placa(placa):
            skipped += 1
            continue
        remolque = _str(r.tipo_vehiculo) if hasattr(r, "tipo_vehiculo") else None
        rows.append({
            "placa":          placa,
            "placa_remolque": remolque if _is_valid_placa(remolque) else None,
            "propietario_id": prop_map.get(_str(r.propietario)) if hasattr(r, "propietario") else None,
        })
    if skipped:
        print(f"  vehiculos: {skipped} placas inválidas descartadas")
    if rows:
        stmt = pg_insert(Vehiculo).values(rows).on_conflict_do_nothing(index_elements=["placa"])
        session.execute(stmt)
        session.commit()


def _load_clientes(df: pd.DataFrame, session: Session) -> dict[str, int]:
    nombres = df["cliente"].apply(_clean_cliente).dropna().unique()
    rows = [{"nombre": n} for n in nombres if n]
    return _insert_catalog(session, Cliente, rows, "nombre")


def _load_lugares(df: pd.DataFrame, session: Session) -> dict[str, int]:
    # Construir mapa ciudad_raw → departamento_completo desde las nuevas columnas
    dept_map: dict[str, str] = {}
    for city_col, dept_col in [("origen", "departamento_origen"), ("destino", "departamento_destino")]:
        if city_col in df.columns and dept_col in df.columns:
            for raw, dept in zip(df[city_col], df[dept_col]):
                if pd.notna(raw) and pd.notna(dept):
                    dept_map[str(raw).strip()] = str(dept).strip()

    raws = pd.concat([df["origen"].dropna(), df["destino"].dropna()]).str.strip().unique()
    rows = [
        {"nombre": nombre, "municipio": mun, "departamento": dept_map.get(nombre)}
        for raw in raws
        for nombre, mun in [_parse_lugar(raw)]
    ]
    return _insert_catalog(session, Lugar, rows, "nombre")


def _load_agencias(df: pd.DataFrame, session: Session) -> dict[str, int]:
    nombres = df["agencia_despachadora"].dropna().str.strip().unique()
    rows = [{"nombre": n} for n in nombres if n]
    return _insert_catalog(session, Agencia, rows, "nombre")


def _load_responsables(df: pd.DataFrame, session: Session) -> dict[str, int]:
    _SKIP = {"ANULADO", "0", ""}
    cols = [c for c in ["nombre_responsable", "responsable", "responsable_estado_interno"] if c in df.columns]
    nombres = pd.concat([df[c].dropna().str.strip() for c in cols]).unique()
    rows = [{"nombre": n} for n in nombres if n and n.upper() not in _SKIP]
    return _insert_catalog(session, Responsable, rows, "nombre")


# ── Tablas de hechos ──────────────────────────────────────────────────

def _load_manifiestos(df: pd.DataFrame, session: Session, maps: dict) -> int:
    """Una fila por manifiesto (deduplica el CSV expandido)."""
    dedup = df.drop_duplicates(subset=["manifiesto"])
    cedula_map, nombre_map = maps["conductores"]

    rows = []
    for r in dedup.itertuples():
        man = _int(r.manifiesto)
        if man is None:
            continue
        conductor_id = (
            cedula_map.get(_cedula(r.cedula_conductor))
            or nombre_map.get(_str(r.conductor))
        )
        fecha_d = _date(r.fecha_despacho) or _date(r.periodo)   # fallback a periodo si falta
        if fecha_d is None:
            continue   # sin fecha no se puede insertar (NOT NULL)
        rows.append({
            "manifiesto":          man,
            "periodo":             _date(r.periodo),
            "año":                 _int(r.año),
            "mes":                 _str(r.mes),
            "consecutivo_semanal": _int(r.consecutivo_semanal),
            "fecha_despacho":      fecha_d,
            "conductor_id":        conductor_id,
            "placa":               _str(r.placa) if _is_valid_placa(_str(r.placa)) else None,
            "placa_remolque":      _str(r.tipo_vehiculo) if hasattr(r, "tipo_vehiculo") and _is_valid_placa(_str(r.tipo_vehiculo)) else None,
            "cliente_id":          maps["clientes"].get(_clean_cliente(r.cliente)),
            "origen_id":           maps["lugares"].get(_str(r.origen)) or maps["lugares"].get("DESCONOCIDO"),
            "destino_id":          maps["lugares"].get(_str(r.destino)) or maps["lugares"].get("DESCONOCIDO"),
            "agencia_id":          maps["agencias"].get(_str(r.agencia_despachadora)) or maps["agencias"].get("ANULADO"),
            "responsable_id":      maps["responsables"].get(_str(r.nombre_responsable)),
            "valor_remesa":        _float(r.valor_remesa),
            "flete_conductor":     _float(r.flete_conductor) or 0.0,
            "anticipo":            _float(r.anticipo),
        })

    if rows:
        stmt = pg_insert(Manifiesto).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["manifiesto"],
            set_={
                "fecha_despacho":      stmt.excluded.fecha_despacho,
                "conductor_id":        stmt.excluded.conductor_id,
                "placa":               stmt.excluded.placa,
                "placa_remolque":      stmt.excluded.placa_remolque,
                "cliente_id":          stmt.excluded.cliente_id,
                "origen_id":           stmt.excluded.origen_id,
                "destino_id":          stmt.excluded.destino_id,
                "agencia_id":          stmt.excluded.agencia_id,
                "responsable_id":      stmt.excluded.responsable_id,
                "valor_remesa":        stmt.excluded.valor_remesa,
                "flete_conductor":     stmt.excluded.flete_conductor,
                "anticipo":            stmt.excluded.anticipo,
            },
        )
        session.execute(stmt)
        session.commit()
    return len(rows)


def _load_remesas(df: pd.DataFrame, session: Session) -> int:
    """
    Expande el campo remesas (puede contener varios códigos separados por ';')
    e inserta una fila por código.  Solo acepta códigos de 5-6 dígitos numéricos.
    Deduplica por (manifiesto_id, codigo_remesa) antes de insertar.
    """
    seen: set[tuple] = set()
    rows = []
    for r in df.itertuples():
        man = _int(r.manifiesto)
        if man is None:
            continue
        raw = _str(r.remesas)
        if not raw or raw.upper() in ("NAN", "NONE", "ANULADO"):
            continue
        for codigo in raw.split(";"):
            codigo = codigo.strip()
            if not codigo or not re.fullmatch(r'[0-9]{5,6}', codigo):
                continue
            key = (man, codigo)
            if key not in seen:
                seen.add(key)
                rows.append({"manifiesto_id": man, "codigo_remesa": codigo})

    if rows:
        stmt = pg_insert(Remesa).values(rows).on_conflict_do_nothing(
            index_elements=["manifiesto_id", "codigo_remesa"]
        )
        session.execute(stmt)
        session.commit()
    return len(rows)


def _load_pagos_conductor(df: pd.DataFrame, session: Session, resp_map: dict[str, int]) -> int:
    """Una fila por manifiesto (deduplica el CSV expandido)."""
    dedup = df.drop_duplicates(subset=["manifiesto"])
    rows = []
    for r in dedup.itertuples():
        man = _int(r.manifiesto)
        if man is None:
            continue
        rows.append({
            "manifiesto_id":      man,
            "fecha_cumplido":     _date(r.fecha_cumplido),
            # dias_cumplido eliminado: campo calculado en DB → CURRENT_DATE - fecha_cumplido
            "estado":             _str(r.estado),
            "condicion_pago":     _str(r.condicion_pago),
            "novedades":          _str(r.novedades),
            "fecha_pago":         _date(r.fecha_pago),
            "valor_pagado":       _float(r.valor_pagado),
            "entidad_financiera": _str(r.entidad_financiera),
            "responsable_id":     resp_map.get(_str(r.responsable)),
        })

    if rows:
        stmt = pg_insert(PagoConductor).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["manifiesto_id"],
            set_={
                "fecha_cumplido":     stmt.excluded.fecha_cumplido,
                "estado":             stmt.excluded.estado,
                "condicion_pago":     stmt.excluded.condicion_pago,
                "novedades":          stmt.excluded.novedades,
                "fecha_pago":         stmt.excluded.fecha_pago,
                "valor_pagado":       stmt.excluded.valor_pagado,
                "entidad_financiera": stmt.excluded.entidad_financiera,
                "responsable_id":     stmt.excluded.responsable_id,
            },
        )
        session.execute(stmt)
        session.commit()
    return len(rows)


def _load_facturacion(df: pd.DataFrame, session: Session, resp_map: dict[str, int]) -> int:
    """Una fila por manifiesto (deduplica el CSV expandido)."""
    dedup = df.drop_duplicates(subset=["manifiesto"])
    rows = []
    for r in dedup.itertuples():
        man = _int(r.manifiesto)
        if man is None:
            continue
        resp_ei = _str(r.responsable_estado_interno) if hasattr(r, "responsable_estado_interno") else None
        rows.append({
            "manifiesto_id":       man,
            "factura_no":          _str(r.factura_no),
            "fecha_factura":       _date(r.fecha),
            "factura_electronica": _str(r.factura_electronica),
            "mes_facturacion":     _int(r.mes_facturacion),
            "estado_interno":      _str(r.estado_interno),
            "responsable_id":      resp_map.get(resp_ei),
        })

    if rows:
        stmt = pg_insert(Facturacion).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["manifiesto_id"],
            set_={
                "factura_no":          stmt.excluded.factura_no,
                "fecha_factura":       stmt.excluded.fecha_factura,
                "factura_electronica": stmt.excluded.factura_electronica,
                "mes_facturacion":     stmt.excluded.mes_facturacion,
                "estado_interno":      stmt.excluded.estado_interno,
                "responsable_id":      stmt.excluded.responsable_id,
            },
        )
        session.execute(stmt)
        session.commit()
    return len(rows)


# ── Entry point ───────────────────────────────────────────────────────

def run(csv_path: str | Path, db_url: str) -> None:
    csv_path = Path(csv_path)

    print("Conectando a la base de datos...")
    engine = create_engine(db_url, echo=False)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("  OK")

    print("Creando tablas (checkfirst=True)...")
    Base.metadata.create_all(engine, checkfirst=True)
    print("  OK")

    print(f"\nLeyendo {csv_path.name}...")
    df = pd.read_csv(csv_path, low_memory=False)
    print(f"  {len(df):,} filas, {len(df.columns)} columnas")

    with Session(engine) as session:
        print("\n── Catálogos ─────────────────────────────────")
        prop_map    = _load_propietarios(df, session)
        print(f"  propietarios:  {len(prop_map):>5}")

        cond_maps   = _load_conductores(df, session)
        print(f"  conductores:   {len(cond_maps[1]):>5}")

        _load_vehiculos(df, session, prop_map)
        n_veh = session.execute(select(Vehiculo)).scalars().all()
        print(f"  vehiculos:     {len(n_veh):>5}")

        cliente_map = _load_clientes(df, session)
        print(f"  clientes:      {len(cliente_map):>5}")

        lugar_map   = _load_lugares(df, session)
        print(f"  lugares:       {len(lugar_map):>5}")

        agencia_map = _load_agencias(df, session)
        print(f"  agencias:      {len(agencia_map):>5}")

        resp_map    = _load_responsables(df, session)
        print(f"  responsables:  {len(resp_map):>5}")

        maps = {
            "conductores":  cond_maps,
            "clientes":     cliente_map,
            "lugares":      lugar_map,
            "agencias":     agencia_map,
            "responsables": resp_map,
        }

        print("\n── Hechos ────────────────────────────────────")
        n = _load_manifiestos(df, session, maps)
        print(f"  manifiestos:       {n:>5}")

        n = _load_remesas(df, session)
        print(f"  remesas:           {n:>5}")

        n = _load_pagos_conductor(df, session, resp_map)
        print(f"  pagos_conductor:   {n:>5}")

        n = _load_facturacion(df, session, resp_map)
        print(f"  facturacion:       {n:>5}")

    print("\nCarga completada.")


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parents[2] / ".env")

    _DB_URL = os.environ.get("DATABASE_URL")
    if not _DB_URL:
        raise RuntimeError("Falta DATABASE_URL en el .env o en las variables de entorno.")

    _CSV = Path(__file__).resolve().parents[2] / "cleaned_data" / "individual_cleaned.csv"
    run(_CSV, _DB_URL)
