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
# Vista enriquecida con campos calculados (fecha_estimada_pago, dias_cumplido).
# Usar para queries que necesitan esos campos; el resto puede seguir contra la tabla.
VIEW = "v_manifiestos"
# Vista restringida para el chatbot: solo columnas operativas visibles
# a conductores y propietarios. Sin datos financieros internos.
CHATBOT_VIEW = "v_chatbot_manifiestos"


def _add_dias_restantes(rows: list[dict]) -> list[dict]:
    """Calcula dias_restantes_pago = fecha_estimada_pago - hoy.
    NULL si fecha_estimada_pago no aplica (anulado, pagado, sin cumplido, modalidad sin plazo)."""
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

def _apply_placa(params: dict, placa: str = None):
    if placa:
        params["placa"] = f"eq.{placa.upper()}"

def _apply_identificador(params: dict, identificador: str = None, tipo_usuario: str = None):
    """Aplica el filtro correcto según el tipo de usuario autenticado."""
    if not identificador or not tipo_usuario:
        return
    if tipo_usuario == "propietario":
        _apply_placa(params, identificador)
    else:
        _apply_conductor(params, identificador)


# ── Queries ───────────────────────────────────────────────────────────────────

def listar_manifiestos(cedula: str = None, mes: str = None, año: int = None,
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
    return _get(CHATBOT_VIEW, params)


def consultar_manifiesto(numero: int, cedula: str = None, placa: str = None) -> dict | None:
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
    rows = _get(CHATBOT_VIEW, params)
    if not rows:
        return None
    # Si está anulado, ocultarlo: para el usuario el manifiesto no existe.
    if rows[0].get("estado_interno") == "ANULADO":
        return None
    _add_dias_restantes(rows)
    return rows[0]


def resumen_periodo(mes: str = None, año: int = None, cedula: str = None,
                    placa: str = None) -> dict:
    params = {
        "select": "manifiesto,flete_conductor,saldo,valor_pagado,estado_interno,fecha_pago",
        "or":     "(estado_interno.neq.ANULADO,estado_interno.is.null)",
    }
    _apply_periodo(params, mes, año)
    _apply_conductor(params, cedula)
    _apply_placa(params, placa)

    rows = _get(CHATBOT_VIEW, params)

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


def manifiestos_pendientes_pago(mes: str = None, año: int = None, cedula: str = None,
                                placa: str = None) -> list[dict]:
    params = {
        "select": "manifiesto,fecha_despacho,conductor,saldo,flete_conductor,valor_pagado,fecha_cumplido,compromiso_pago,fecha_estimada_pago,estado_interno",
        "fecha_pago": "is.null",
        "or":         "(estado_interno.neq.ANULADO,estado_interno.is.null)",
    }
    _apply_periodo(params, mes, año)
    _apply_conductor(params, cedula)
    _apply_placa(params, placa)

    rows = _get(CHATBOT_VIEW, params)
    _add_dias_restantes(rows)
    return rows[:50]


def manifiestos_sin_factura(mes: str = None, año: int = None, cedula: str = None,
                            placa: str = None) -> list[dict]:
    params = {
        "select": "manifiesto,fecha_despacho,cliente,conductor,responsable_estado_interno,estado_interno",
        "factura_no": "is.null",
        "or":         "(estado_interno.neq.ANULADO,estado_interno.is.null)",
    }
    _apply_periodo(params, mes, año)
    _apply_conductor(params, cedula)
    _apply_placa(params, placa)

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
    params = {"select": "cliente,valor_remesa,valor_factura"}
    _apply_periodo(params, mes, año)

    rows = _get(TABLE, params)
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


# Tokens que NO son novedades reales — son clasificación de vehículo o servicio.
# Filtrar server-side evita que el modelo procese ruido y agote MAX_TOOL_ITERS.
_NOVEDAD_RUIDO = ("TIPO VEHICULO", "TIPO VEHÍCULO", "TURBO", "URBANO", "URBANOS")

def manifiestos_con_novedad(mes: str = None, año: int = None, cedula: str = None,
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

    rows = _get(CHATBOT_VIEW, params)
    # Devolvemos solo novedades REALES (las que requieren atención).
    # Ruido como "TIPO VEHICULO: TURBO" se filtra aquí, no en el modelo,
    # para mantener la respuesta breve y permitirle resumir sin saturarse.
    result = []
    for r in rows:
        nov = (r.get("novedades") or "").strip()
        if not nov:
            continue
        nov_upper = nov.upper()
        if all(token in nov_upper for token in []):  # placeholder, no-op
            continue
        # Si TODA la novedad es ruido (sin texto adicional relevante), saltarla.
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

    rows = _get(CHATBOT_VIEW, params)
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


def get_propietario_by_placa(placa: str) -> dict | None:
    """Devuelve {nombre, placa} si la placa existe en algún manifiesto NO anulado.
    Usa el campo `propietario` del manifiesto más reciente con esa placa.
    """
    rows = _get(TABLE, {
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


def verificar_manifiesto_propietario(manifiesto: int, placa: str) -> bool:
    """Confirma que el manifiesto corresponde a esa placa y no está anulado."""
    rows = _get(TABLE, {
        "manifiesto": f"eq.{manifiesto}",
        "placa":      f"eq.{placa.upper()}",
        "or":         "(estado_interno.neq.ANULADO,estado_interno.is.null)",
        "select":     "manifiesto",
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


def log_jailbreak(wa_from: str | None, identificador: str | None, mensaje: str, motivo: str) -> None:
    """`identificador` es la cédula del conductor o la placa del propietario.
    Se persiste en la columna legacy `cedula` por compatibilidad."""
    try:
        _request(
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
        pass  # no bloquear el flujo principal por un fallo de auditoría
