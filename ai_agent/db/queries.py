import httpx
from config import get_settings

_cfg = get_settings()
_BASE = _cfg["supabase_url"] + "/rest/v1"
_KEY  = _cfg["supabase_service_key"]
_HEADERS = {
    "apikey":        _KEY,
    "Authorization": f"Bearer {_KEY}",
    "Content-Type":  "application/json",
}

TABLE = "manifiestos_flat"


def _get(path: str, params: dict = None) -> list[dict]:
    qs = "&".join(f"{k}={v}" for k, v in (params or {}).items())
    url = f"{_BASE}/{path}?{qs}" if qs else f"{_BASE}/{path}"
    r = httpx.get(url, headers=_HEADERS, timeout=15)
    r.raise_for_status()
    return r.json()


# ── Helpers de filtro ─────────────────────────────────────────────────────────

def _apply_periodo(params: dict, mes: str = None, año: int = None):
    if mes:
        params["mes"] = f"eq.{mes.upper()}"
    if año:
        params["año"] = f"eq.{año}"

def _apply_conductor(params: dict, cedula: str = None):
    if cedula:
        params["cedula_conductor"] = f"eq.{cedula}"


# ── Queries ───────────────────────────────────────────────────────────────────

def consultar_manifiesto(numero: int, cedula: str = None) -> dict | None:
    params = {
        "manifiesto": f"eq.{numero}",
        "select": (
            "manifiesto,fecha_despacho,origen,destino,cliente,"
            "conductor,cedula_conductor,celular,placa,tipo_vehiculo,propietario,"
            "agencia_despachadora,remesas,valor_remesa,"
            "flete_conductor,ajuste_positivo_flete,ajuste_negativo_flete,flete_neto_conductor,anticipo,"
            "fecha_cumplido,compromiso_pago,novedades,novedad_conductor,novedad_empresa,"
            "estado_interno,responsable_estado_interno,"
            "fecha_pago,valor_pagado,entidad_financiera,"
            "factura_no,fecha_factura,factura_electronica,mes,año"
        ),
    }
    _apply_conductor(params, cedula)
    rows = _get(TABLE, params)
    return rows[0] if rows else None


def resumen_periodo(mes: str, año: int, cedula: str = None) -> dict:
    params = {
        "select": "manifiesto,valor_remesa,flete_conductor,flete_neto_conductor,anticipo,valor_pagado,estado_interno,fecha_pago",
    }
    _apply_periodo(params, mes, año)
    _apply_conductor(params, cedula)

    rows = _get(TABLE, params)

    total        = len(rows)
    anulados     = sum(1 for r in rows if r.get("estado_interno") == "ANULADO")
    total_remesa = sum(r.get("valor_remesa") or 0 for r in rows)
    total_flete  = sum(r.get("flete_conductor") or 0 for r in rows)
    pendiente    = sum(
        (r.get("flete_neto_conductor") or r.get("flete_conductor") or 0) - (r.get("valor_pagado") or 0)
        for r in rows
        if not r.get("fecha_pago") and r.get("estado_interno") != "ANULADO"
    )

    return {
        "periodo":        f"{mes} {año}",
        "total":          total,
        "anulados":       anulados,
        "activos":        total - anulados,
        "total_remesa":   total_remesa,
        "total_flete":    total_flete,
        "pendiente_pago": pendiente,
    }


def manifiestos_pendientes_pago(mes: str = None, año: int = None, cedula: str = None) -> list[dict]:
    params = {
        "select": "manifiesto,fecha_despacho,conductor,flete_neto_conductor,flete_conductor,anticipo,valor_pagado,compromiso_pago,estado_interno",
        "fecha_pago": "is.null",
        "or":         "(estado_interno.neq.ANULADO,estado_interno.is.null)",
    }
    _apply_periodo(params, mes, año)
    _apply_conductor(params, cedula)

    rows = _get(TABLE, params)
    return rows[:50]


def manifiestos_sin_factura(mes: str = None, año: int = None, cedula: str = None) -> list[dict]:
    params = {
        "select": "manifiesto,fecha_despacho,cliente,conductor,responsable_estado_interno,estado_interno",
        "factura_no": "is.null",
        "or":         "(estado_interno.neq.ANULADO,estado_interno.is.null)",
    }
    _apply_periodo(params, mes, año)
    _apply_conductor(params, cedula)

    rows = _get(TABLE, params)
    return rows[:50]


def top_conductores(mes: str = None, año: int = None, limite: int = 10) -> list[dict]:
    params = {"select": "conductor,estado_interno"}
    _apply_periodo(params, mes, año)

    rows = _get(TABLE, params)
    conteo: dict[str, int] = {}
    for r in rows:
        if r.get("estado_interno") == "ANULADO":
            continue
        nombre = (r.get("conductor") or "").strip()
        if not nombre or nombre.upper() == "ANULADO":
            continue
        conteo[nombre] = conteo.get(nombre, 0) + 1

    return [{"conductor": k, "manifiestos": v}
            for k, v in sorted(conteo.items(), key=lambda x: -x[1])[:limite]]


def top_clientes(mes: str = None, año: int = None, limite: int = 10) -> list[dict]:
    params = {"select": "cliente,valor_remesa"}
    _apply_periodo(params, mes, año)

    rows = _get(TABLE, params)
    conteo: dict[str, int]   = {}
    remesas: dict[str, float] = {}
    for r in rows:
        nombre = r.get("cliente") or "Sin cliente"
        conteo[nombre]  = conteo.get(nombre, 0) + 1
        remesas[nombre] = remesas.get(nombre, 0) + (r.get("valor_remesa") or 0)

    return [{"cliente": k, "manifiestos": conteo[k], "total_remesa": remesas[k]}
            for k in sorted(conteo, key=lambda x: -conteo[x])[:limite]]


def top_rutas(mes: str = None, año: int = None, limite: int = 10) -> list[dict]:
    params = {"select": "origen,destino"}
    _apply_periodo(params, mes, año)

    rows = _get(TABLE, params)
    conteo: dict[str, int] = {}
    for r in rows:
        ruta = f"{r.get('origen', '?')} → {r.get('destino', '?')}"
        conteo[ruta] = conteo.get(ruta, 0) + 1

    return [{"ruta": k, "viajes": v}
            for k, v in sorted(conteo.items(), key=lambda x: -x[1])[:limite]]


def manifiestos_con_novedad(mes: str = None, año: int = None, cedula: str = None) -> list[dict]:
    params = {
        "select": "manifiesto,fecha_despacho,conductor,cliente,novedades,novedad_conductor,novedad_empresa,estado_interno",
        "novedades": "not.is.null",
        "or":        "(estado_interno.neq.ANULADO,estado_interno.is.null)",
    }
    _apply_periodo(params, mes, año)
    _apply_conductor(params, cedula)

    rows = _get(TABLE, params)
    return [r for r in rows if (r.get("novedades") or "").strip()][:50]


def conductor_info(nombre: str = None, cedula: str = None, cedula_auth: str = None) -> list[dict]:
    """Si cedula_auth está presente (conductor autenticado), solo devuelve su propia info."""
    if cedula_auth:
        cedula = cedula_auth

    if cedula:
        params = {"cedula_conductor": f"eq.{cedula}", "select": "conductor,cedula_conductor,celular"}
    elif nombre:
        params = {"conductor": f"ilike.*{nombre.upper()}*", "select": "conductor,cedula_conductor,celular"}
    else:
        return []

    rows = _get(TABLE, params)
    if not rows:
        return []

    seen: dict[str, dict] = {}
    for r in rows:
        key = r.get("conductor") or ""
        if key not in seen:
            seen[key] = {
                "nombre":            key,
                "celular":           r.get("celular"),
                "total_manifiestos": 0,
            }
            if not cedula_auth:
                seen[key]["cedula"] = r.get("cedula_conductor")
        seen[key]["total_manifiestos"] += 1

    return list(seen.values())


# ── Auth ──────────────────────────────────────────────────────────────────────

def get_conductor_by_cedula(cedula: str) -> dict | None:
    """Devuelve {nombre, cedula} si la cédula existe en manifiestos_flat."""
    rows = _get(TABLE, {
        "cedula_conductor": f"eq.{cedula}",
        "select":           "conductor,cedula_conductor",
        "limit":            "1",
    })
    if not rows:
        return None
    return {"nombre": rows[0]["conductor"], "cedula": rows[0]["cedula_conductor"]}


def verificar_manifiesto_conductor(manifiesto: int, cedula: str) -> bool:
    """Confirma que el manifiesto pertenece a esa cédula."""
    rows = _get(TABLE, {
        "manifiesto":       f"eq.{manifiesto}",
        "cedula_conductor": f"eq.{cedula}",
        "select":           "manifiesto",
    })
    return len(rows) > 0
