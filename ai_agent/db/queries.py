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

# ── Cliente HTTP reutilizado (HTTP keep-alive + pool) ────────────────────────
# Antes: cada request abría una conexión TCP+TLS nueva (~200ms overhead).
# Ahora: pool de 20 conexiones mantenidas vivas → throughput ~3-5x mejor
# bajo carga concurrente. Esencial cuando varios conductores escriben a la vez.
_CLIENT = httpx.Client(
    base_url=_BASE,
    headers=_HEADERS,
    timeout=15.0,
    limits=httpx.Limits(
        max_connections=20,
        max_keepalive_connections=20,
        keepalive_expiry=30.0,
    ),
)


def _get(path: str, params: dict = None) -> list[dict]:
    qs = "&".join(f"{k}={v}" for k, v in (params or {}).items())
    url = f"/{path}?{qs}" if qs else f"/{path}"
    r = _CLIENT.get(url)
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

def listar_manifiestos(cedula: str, mes: str = None, año: int = None) -> list[dict]:
    params = {
        "select": "manifiesto,fecha_despacho,origen,destino,cliente,flete_neto_conductor,flete_conductor,fecha_pago,estado_interno,mes,año",
        "order":  "manifiesto.desc",
        "limit":  "50",
        # Manifiestos ANULADOS no existen para el conductor (decisión de negocio)
        "or":     "(estado_interno.neq.ANULADO,estado_interno.is.null)",
    }
    _apply_conductor(params, cedula)
    _apply_periodo(params, mes, año)
    return _get(TABLE, params)


def consultar_manifiesto(numero: int, cedula: str = None) -> dict | None:
    params = {
        "manifiesto": f"eq.{numero}",
        "select": (
            "manifiesto,fecha_despacho,origen,destino,cliente,"
            "conductor,cedula_conductor,celular,placa,tipo_vehiculo,propietario,"
            "agencia_despachadora,remesas,valor_remesa,"
            "flete_conductor,ajuste_positivo_flete,ajuste_negativo_flete,consignacion_a_terceros,flete_neto_conductor,anticipo,"
            "fecha_cumplido,compromiso_pago,novedades,novedad_conductor,novedad_empresa,"
            "estado_interno,responsable_estado_interno,"
            "fecha_pago,valor_pagado,entidad_financiera,"
            "factura_no,fecha_factura,factura_electronica,valor_factura,mes,año"
        ),
    }
    _apply_conductor(params, cedula)
    rows = _get(TABLE, params)
    if not rows:
        return None
    # Si está anulado, ocultarlo: para el conductor el manifiesto no existe.
    if rows[0].get("estado_interno") == "ANULADO":
        return None
    return rows[0]


def resumen_periodo(mes: str = None, año: int = None, cedula: str = None) -> dict:
    params = {
        "select": "manifiesto,valor_remesa,flete_conductor,flete_neto_conductor,anticipo,valor_pagado,estado_interno,fecha_pago",
        # Excluir ANULADOS desde la query: para el conductor no existen.
        "or":     "(estado_interno.neq.ANULADO,estado_interno.is.null)",
    }
    _apply_periodo(params, mes, año)
    _apply_conductor(params, cedula)

    rows = _get(TABLE, params)

    total        = len(rows)
    total_remesa = sum(r.get("valor_remesa") or 0 for r in rows)
    total_flete  = sum(r.get("flete_conductor") or 0 for r in rows)
    pendiente    = sum(
        (r.get("flete_neto_conductor") or r.get("flete_conductor") or 0) - (r.get("valor_pagado") or 0)
        for r in rows
        if not r.get("fecha_pago")
    )

    if mes and año:
        periodo = f"{mes.upper()} {año}"
    elif año:
        periodo = str(año)
    elif mes:
        periodo = mes.upper()
    else:
        periodo = "todos los registros"

    return {
        "periodo":        periodo,
        "total":          total,
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
    """Si cedula_auth está presente (conductor autenticado), solo devuelve su propia info.
    Cuenta total_manifiestos excluyendo los ANULADOS (no existen para el conductor)."""
    if cedula_auth:
        cedula = cedula_auth

    base_select = "conductor,cedula_conductor,celular,estado_interno"

    if cedula:
        params = {"cedula_conductor": f"eq.{cedula}", "select": base_select}
    elif nombre:
        params = {"conductor": f"ilike.*{nombre.upper()}*", "select": base_select}
    else:
        return []

    rows = _get(TABLE, params)
    if not rows:
        return []

    seen: dict[str, dict] = {}
    for r in rows:
        # No contar manifiestos anulados
        if r.get("estado_interno") == "ANULADO":
            continue
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
    """Devuelve {nombre, cedula} si la cédula existe en algún manifiesto NO anulado."""
    rows = _get(TABLE, {
        "cedula_conductor": f"eq.{cedula}",
        "or":               "(estado_interno.neq.ANULADO,estado_interno.is.null)",
        "select":           "conductor,cedula_conductor",
        "limit":            "1",
    })
    if not rows:
        return None
    return {"nombre": rows[0]["conductor"], "cedula": rows[0]["cedula_conductor"]}


def verificar_manifiesto_conductor(manifiesto: int, cedula: str) -> bool:
    """Confirma que el manifiesto pertenece a esa cédula y no está anulado."""
    rows = _get(TABLE, {
        "manifiesto":       f"eq.{manifiesto}",
        "cedula_conductor": f"eq.{cedula}",
        "or":               "(estado_interno.neq.ANULADO,estado_interno.is.null)",
        "select":           "manifiesto",
    })
    return len(rows) > 0


# ── Sesiones del chatbot (persistencia en Supabase) ──────────────────────────

def _request(method: str, path: str, params: dict = None, json_body=None, headers_extra: dict = None):
    qs = "&".join(f"{k}={v}" for k, v in (params or {}).items())
    url = f"/{path}?{qs}" if qs else f"/{path}"
    r = _CLIENT.request(method, url, headers=headers_extra, json=json_body)
    r.raise_for_status()
    if r.status_code == 204 or not r.content:
        return None
    return r.json()


def get_session(wa_from: str) -> dict | None:
    rows = _get("chatbot_sesiones", {"wa_from": f"eq.{wa_from}", "select": "*"})
    return rows[0] if rows else None


def upsert_session(session: dict) -> None:
    """Inserta o actualiza la sesión. La PK es wa_from."""
    _request(
        "POST",
        "chatbot_sesiones",
        json_body=session,
        headers_extra={"Prefer": "resolution=merge-duplicates,return=minimal"},
    )


def delete_session(wa_from: str) -> None:
    _request("DELETE", "chatbot_sesiones", params={"wa_from": f"eq.{wa_from}"})


def mark_message_processed(message_id: str) -> bool:
    """Inserta el message_id. Devuelve True si era nuevo, False si ya estaba."""
    r = _CLIENT.post(
        "/processed_messages",
        headers={"Prefer": "return=minimal"},
        json={"message_id": message_id},
    )
    if r.status_code in (200, 201, 204):
        return True
    if r.status_code == 409:
        return False
    r.raise_for_status()
    return True


def log_jailbreak(wa_from: str | None, cedula: str | None, mensaje: str, motivo: str) -> None:
    try:
        _request(
            "POST",
            "jailbreak_log",
            json_body={
                "wa_from": wa_from,
                "cedula":  cedula,
                "mensaje": mensaje[:500],
                "motivo":  motivo,
            },
            headers_extra={"Prefer": "return=minimal"},
        )
    except Exception:
        pass  # no bloquear el flujo principal por un fallo de auditoría
