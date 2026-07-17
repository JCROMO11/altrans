import time as _time
from datetime import date, datetime

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
VIEW = "v_manifiestos"
CHATBOT_VIEW = "v_chatbot_manifiestos"


def _add_dias_restantes(rows: list[dict]) -> list[dict]:
    hoy = date.today()
    for r in rows:
        fep = r.get("fecha_estimada_pago")
        if fep:
            try:
                fep_date = datetime.fromisoformat(fep).date() if isinstance(fep, str) else fep
                r["dias_restantes_pago"] = (fep_date - hoy).days
            except (ValueError, TypeError):
                r["dias_restantes_pago"] = None
        else:
            r["dias_restantes_pago"] = None
    return rows


_CLIENT = httpx.AsyncClient(
    base_url=_BASE,
    headers=_HEADERS,
    timeout=15.0,
    limits=httpx.Limits(
        max_connections=20,
        max_keepalive_connections=20,
        keepalive_expiry=30.0,
    ),
)


async def _get(path: str, params: dict = None) -> list[dict]:
    qs = "&".join(f"{k}={v}" for k, v in (params or {}).items())
    url = f"/{path}?{qs}" if qs else f"/{path}"
    r = await _CLIENT.get(url)
    r.raise_for_status()
    return r.json()


def _apply_periodo(params: dict, mes: str = None, año: int = None):
    if mes:
        params["mes"] = f"eq.{mes.upper()}"
    if año:
        params["año"] = f"eq.{año}"

def _apply_conductor(params: dict, cedula: str = None):
    if cedula:
        params["cedula_conductor"] = f"eq.{cedula}"

def _apply_placa(params: dict, placa: str = None):
    if placa:
        params["placa"] = f"eq.{placa.upper()}"

def _apply_identificador(params: dict, identificador: str = None, tipo_usuario: str = None):
    if not identificador or not tipo_usuario:
        return
    if tipo_usuario == "propietario":
        _apply_placa(params, identificador)
    else:
        _apply_conductor(params, identificador)


async def listar_manifiestos(cedula: str = None, mes: str = None, año: int = None,
                             placa: str = None) -> list[dict]:
    if placa:
        select = ("manifiesto,fecha_despacho,origen,destino,cliente,conductor,placa,"
                  "saldo,flete_conductor,fecha_pago,estado_interno,mes,año")
    else:
        select = ("manifiesto,fecha_despacho,origen,destino,cliente,"
                  "saldo,flete_conductor,fecha_pago,estado_interno,mes,año")
    params = {
        "select": select,
        "order":  "manifiesto.desc",
        "limit":  "50",
        "or":     "(estado_interno.neq.ANULADO,estado_interno.is.null)",
    }
    _apply_conductor(params, cedula)
    _apply_placa(params, placa)
    _apply_periodo(params, mes, año)
    return await _get(CHATBOT_VIEW, params)


async def consultar_manifiesto(numero: int, cedula: str = None, placa: str = None) -> dict | None:
    params = {
        "manifiesto": f"eq.{numero}",
        "select": (
            "manifiesto,fecha_despacho,origen,destino,cliente,"
            "conductor,cedula_conductor,celular,placa,tipo_vehiculo,propietario,"
            "flete_conductor,saldo,"
            "fecha_cumplido,compromiso_pago,fecha_estimada_pago,novedades,novedad_conductor,"
            "estado_interno,"
            "fecha_pago,valor_pagado,mes,año"
        ),
    }
    _apply_conductor(params, cedula)
    _apply_placa(params, placa)
    rows = await _get(CHATBOT_VIEW, params)
    if not rows:
        return None
    if rows[0].get("estado_interno") == "ANULADO":
        return None
    _add_dias_restantes(rows)
    return rows[0]


async def resumen_periodo(mes: str = None, año: int = None, cedula: str = None,
                          placa: str = None) -> dict:
    params = {
        "select": "manifiesto,flete_conductor,saldo,valor_pagado,estado_interno,fecha_pago",
        "or":     "(estado_interno.neq.ANULADO,estado_interno.is.null)",
    }
    _apply_periodo(params, mes, año)
    _apply_conductor(params, cedula)
    _apply_placa(params, placa)

    rows = await _get(CHATBOT_VIEW, params)

    total        = len(rows)
    total_flete  = sum(r.get("flete_conductor") or 0 for r in rows)
    pendiente    = sum(
        (r.get("saldo") or 0) - (r.get("valor_pagado") or 0)
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
        "total_flete":    total_flete,
        "pendiente_pago": pendiente,
    }


async def manifiestos_pendientes_pago(mes: str = None, año: int = None, cedula: str = None,
                                      placa: str = None) -> list[dict]:
    params = {
        "select": "manifiesto,fecha_despacho,conductor,saldo,flete_conductor,valor_pagado,fecha_cumplido,compromiso_pago,fecha_estimada_pago,estado_interno",
        "fecha_pago": "is.null",
        "or":         "(estado_interno.neq.ANULADO,estado_interno.is.null)",
    }
    _apply_periodo(params, mes, año)
    _apply_conductor(params, cedula)
    _apply_placa(params, placa)

    rows = await _get(CHATBOT_VIEW, params)
    _add_dias_restantes(rows)
    return rows[:50]


async def manifiestos_sin_factura(mes: str = None, año: int = None, cedula: str = None,
                                  placa: str = None) -> list[dict]:
    params = {
        "select": "manifiesto,fecha_despacho,cliente,conductor,responsable_estado_interno,estado_interno",
        "factura_no": "is.null",
        "or":         "(estado_interno.neq.ANULADO,estado_interno.is.null)",
    }
    _apply_periodo(params, mes, año)
    _apply_conductor(params, cedula)
    _apply_placa(params, placa)

    rows = await _get(TABLE, params)
    return rows[:50]


async def top_conductores(mes: str = None, año: int = None, limite: int = 10) -> list[dict]:
    params = {"select": "conductor,estado_interno"}
    _apply_periodo(params, mes, año)

    rows = await _get(TABLE, params)
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


async def top_clientes(mes: str = None, año: int = None, limite: int = 10) -> list[dict]:
    params = {"select": "cliente,valor_remesa,valor_factura"}
    _apply_periodo(params, mes, año)

    rows = await _get(TABLE, params)
    conteo: dict[str, int]   = {}
    remesas: dict[str, float] = {}
    facturado: dict[str, float] = {}
    for r in rows:
        nombre = r.get("cliente") or "Sin cliente"
        conteo[nombre]    = conteo.get(nombre, 0) + 1
        remesas[nombre]   = remesas.get(nombre, 0) + (r.get("valor_remesa") or 0)
        facturado[nombre] = facturado.get(nombre, 0) + (r.get("valor_factura") or 0)

    return [{"cliente": k, "manifiestos": conteo[k], "total_remesa": remesas[k], "total_facturado": facturado[k]}
            for k in sorted(conteo, key=lambda x: -conteo[x])[:limite]]


async def top_rutas(mes: str = None, año: int = None, limite: int = 10) -> list[dict]:
    params = {"select": "origen,destino"}
    _apply_periodo(params, mes, año)

    rows = await _get(TABLE, params)
    conteo: dict[str, int] = {}
    for r in rows:
        ruta = f"{r.get('origen', '?')} → {r.get('destino', '?')}"
        conteo[ruta] = conteo.get(ruta, 0) + 1

    return [{"ruta": k, "viajes": v}
            for k, v in sorted(conteo.items(), key=lambda x: -x[1])[:limite]]


_NOVEDAD_RUIDO = ("TIPO VEHICULO", "TIPO VEHÍCULO", "TURBO", "URBANO", "URBANOS")

async def manifiestos_con_novedad(mes: str = None, año: int = None, cedula: str = None,
                                  placa: str = None) -> list[dict]:
    params = {
        "select": "manifiesto,fecha_despacho,conductor,cliente,novedades,estado_interno",
        "novedades": "not.is.null",
        "or":        "(estado_interno.neq.ANULADO,estado_interno.is.null)",
        "limit":     "100",
    }
    _apply_periodo(params, mes, año)
    _apply_conductor(params, cedula)
    _apply_placa(params, placa)

    rows = await _get(CHATBOT_VIEW, params)
    result = []
    for r in rows:
        nov = (r.get("novedades") or "").strip()
        if not nov:
            continue
        nov_upper = nov.upper()
        if any(token in nov_upper for token in _NOVEDAD_RUIDO) and len(nov) < 60:
            continue
        result.append({
            "manifiesto":       r["manifiesto"],
            "fecha_despacho":   r.get("fecha_despacho"),
            "conductor":        r.get("conductor"),
            "cliente":          r.get("cliente"),
            "novedad_resumen":  nov[:120] + ("..." if len(nov) > 120 else ""),
        })
        if len(result) >= 25:
            break
    return result


async def conductor_info(nombre: str = None, cedula: str = None, cedula_auth: str = None) -> list[dict]:
    if cedula_auth:
        cedula = cedula_auth

    base_select = "conductor,cedula_conductor,celular,estado_interno"

    if cedula:
        params = {"cedula_conductor": f"eq.{cedula}", "select": base_select}
    elif nombre:
        params = {"conductor": f"ilike.*{nombre.upper()}*", "select": base_select}
    else:
        return []

    rows = await _get(CHATBOT_VIEW, params)
    if not rows:
        return []

    seen: dict[str, dict] = {}
    for r in rows:
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

async def get_conductor_by_cedula(cedula: str) -> dict | None:
    rows = await _get(TABLE, {
        "cedula_conductor": f"eq.{cedula}",
        "or":               "(estado_interno.neq.ANULADO,estado_interno.is.null)",
        "select":           "conductor,cedula_conductor",
        "limit":            "1",
    })
    if not rows:
        return None
    return {"nombre": rows[0]["conductor"], "cedula": rows[0]["cedula_conductor"]}


async def verificar_manifiesto_conductor(manifiesto: int, cedula: str) -> bool:
    rows = await _get(TABLE, {
        "manifiesto":       f"eq.{manifiesto}",
        "cedula_conductor": f"eq.{cedula}",
        "or":               "(estado_interno.neq.ANULADO,estado_interno.is.null)",
        "select":           "manifiesto",
    })
    return len(rows) > 0


async def get_propietario_by_placa(placa: str) -> dict | None:
    rows = await _get(TABLE, {
        "placa":  f"eq.{placa.upper()}",
        "or":     "(estado_interno.neq.ANULADO,estado_interno.is.null)",
        "select": "propietario,placa",
        "order":  "fecha_despacho.desc",
        "limit":  "1",
    })
    if not rows:
        return None
    return {
        "nombre": rows[0].get("propietario") or "Propietario",
        "placa":  rows[0]["placa"],
    }


async def verificar_manifiesto_propietario(manifiesto: int, placa: str) -> bool:
    rows = await _get(TABLE, {
        "manifiesto": f"eq.{manifiesto}",
        "placa":      f"eq.{placa.upper()}",
        "or":         "(estado_interno.neq.ANULADO,estado_interno.is.null)",
        "select":     "manifiesto",
    })
    return len(rows) > 0


# ── Admin Auth ────────────────────────────────────────────────────────────────

async def get_admin_by_wa_from(wa_from: str) -> dict | None:
    rows = await _get("admin_usuarios", {
        "wa_from": f"eq.{wa_from}",
        "select": "wa_from,nombre,password_hash,ultimo_acceso",
    })
    return rows[0] if rows else None


async def update_admin_ultimo_acceso(wa_from: str) -> None:
    from datetime import datetime, timezone
    await _request(
        "PATCH",
        "admin_usuarios",
        params={"wa_from": f"eq.{wa_from}"},
        json_body={"ultimo_acceso": datetime.now(timezone.utc).isoformat()},
        headers_extra={"Prefer": "return=minimal"},
    )


# ── Sesiones del chatbot ──────────────────────────────────────────────────────

async def _request(method: str, path: str, params: dict = None, json_body=None,
                   headers_extra: dict = None):
    qs = "&".join(f"{k}={v}" for k, v in (params or {}).items())
    url = f"/{path}?{qs}" if qs else f"/{path}"
    hdrs = dict(_HEADERS)
    if headers_extra:
        hdrs.update(headers_extra)
    r = await _CLIENT.request(method, url, headers=hdrs, json=json_body)
    r.raise_for_status()
    if r.status_code == 204 or not r.content:
        return None
    return r.json()


async def get_session(wa_from: str) -> dict | None:
    rows = await _get("chatbot_sesiones", {"wa_from": f"eq.{wa_from}", "select": "*"})
    return rows[0] if rows else None


async def upsert_session(session: dict) -> None:
    await _request(
        "POST",
        "chatbot_sesiones",
        json_body=session,
        headers_extra={"Prefer": "resolution=merge-duplicates,return=minimal"},
    )


async def delete_session(wa_from: str) -> None:
    await _request("DELETE", "chatbot_sesiones", params={"wa_from": f"eq.{wa_from}"})


async def mark_message_processed(message_id: str) -> bool:
    r = await _CLIENT.post(
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


async def log_jailbreak(wa_from: str | None, identificador: str | None,
                        mensaje: str, motivo: str) -> None:
    try:
        await _request(
            "POST",
            "jailbreak_log",
            json_body={
                "wa_from": wa_from,
                "cedula":  identificador,
                "mensaje": mensaje[:500],
                "motivo":  motivo,
            },
            headers_extra={"Prefer": "return=minimal"},
        )
    except Exception:
        pass


# ── System prompts from DB ─────────────────────────────────────────────────────

_prompt_cache: dict[str, tuple[str, float]] = {}
_PROMPT_CACHE_TTL = 300  # 5 min


async def get_prompt(clave: str) -> str | None:
    now = _time.time()
    cached = _prompt_cache.get(clave)
    if cached and (now - cached[1]) < _PROMPT_CACHE_TTL:
        return cached[0]
    try:
        rows = await _get("system_prompts", {
            "clave": f"eq.{clave}",
            "select": "contenido",
            "limit": "1",
        })
        if rows:
            contenido = rows[0]["contenido"]
            _prompt_cache[clave] = (contenido, now)
            return contenido
        _prompt_cache[clave] = (None, now)
        return None
    except (httpx.HTTPError, OSError, ValueError):
        return None
