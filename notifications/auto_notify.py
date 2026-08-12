"""
Envío automático de notificaciones WhatsApp a conductores.

Lee de la RPC get_pendientes_notificacion los 4 templates de saldo no pagado,
más mensajes pending de pago_realizado desde messages_sent, y envía la
plantilla correspondiente. Registra envíos en messages_sent para evitar reenvíos.

Agrupa los mensajes por plantilla y envía cada lote en paralelo (una plantilla
a la vez). El orden y el espaciado entre plantillas lo controla el scheduler
(5 minutos entre lotes) o run_auto_notify_cycle (disparo manual).

Modo de envío:
  - WA_SEND_MODE=template (default): usa send_whatsapp_template (plantillas
    aprobadas de Meta). Es el modo de producción.
  - WA_SEND_MODE=text: usa send_whatsapp con el contenido de la plantilla
    renderizado como texto libre (para pruebas antes de la aprobación de Meta).

Requiere: SUPABASE_URL, SUPABASE_SERVICE_KEY,
          WA_TOKEN, WA_PHONE_NUMBER_ID.
"""
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import httpx
from dotenv import load_dotenv

from message_texts import render_text
from whatsapp_notify import send_whatsapp, send_whatsapp_template

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

logger = logging.getLogger(__name__)

_MAX_CONSECUTIVE_ERRORS = 10
_MAX_CONCURRENT_SENDS = 5

_SEND_MODE = os.environ.get("WA_SEND_MODE", "template").strip().lower()

# Orden de envío entre plantillas: pago_realizado primero (urgente: confirmar
# el pago al conductor), luego los saldos pendientes.
TEMPLATE_ORDER = [
    "pago_realizado",
    "saldo_falta_factura",
    "saldo_falta_documentacion",
    "saldo_novedad_pendiente",
    "saldo_plazo_vigente",
]

_SPANISH_MONTHS = {
    1: "enero", 2: "febrero", 3: "marzo", 4: "abril",
    5: "mayo", 6: "junio", 7: "julio", 8: "agosto",
    9: "septiembre", 10: "octubre", 11: "noviembre", 12: "diciembre",
}

# Tokens que NO son novedades reales — son clasificación de vehículo o servicio.
# Coincide con el guardrail del RPC get_pendientes_notificacion.
_NOVEDAD_NOISE = ("TIPO VEHICULO", "TIPO VEHÍCULO", "TURBO", "URBANO", "URBANOS")


def _is_novedad_noise(novedades: str | None) -> bool:
    """True si novedades solo contiene ruido (clasificación, no problema real).

    Coincide con la lógica del RPC get_pendientes_notificacion (es_novedad_real).
    """
    if not novedades or not novedades.strip():
        return True
    nov = novedades.strip()
    if len(nov) <= 3:
        return True
    if len(nov) < 60 and any(t in nov.upper() for t in _NOVEDAD_NOISE):
        return True
    return False


# Nombre real de cada plantilla en la WABA (creadas en scripts/crear_plantillas_altrans.py)
_TEMPLATE_NAMES = {
    "saldo_falta_factura":       "altrans_saldo_falta_factura",
    "saldo_falta_documentacion": "altrans_saldo_falta_documentacion",
    "saldo_novedad_pendiente":   "altrans_saldo_novedad_pendiente",
    "saldo_plazo_vigente":       "altrans_saldo_plazo_vigente",
    "pago_realizado":            "altrans_pago_realizado",
}

# Parámetros posicionales {{1}}, {{2}}... que recibe cada plantilla.
# - saldo_* solo usan manifiesto.
# - saldo_plazo_vigente usa manifiesto + fecha estimada.
# - pago_realizado usa manifiesto + fecha de pago.
_TEMPLATE_PARAMS = {
    "saldo_falta_factura":       ["manifiesto"],
    "saldo_falta_documentacion": ["manifiesto"],
    "saldo_novedad_pendiente":   ["manifiesto"],
    "saldo_plazo_vigente":       ["manifiesto", "fecha_estimada"],
    "pago_realizado":            ["manifiesto", "fecha_pago"],
}


def _template_params(template_name: str, manifiesto: int,
                     fecha_estimada: str | None = None,
                     fecha_pago: str | None = None) -> list[str]:
    """Construye los valores posicionales de la plantilla en orden {{1}}, {{2}}..."""
    valores = {
        "manifiesto":     str(manifiesto),
        "fecha_estimada": fecha_estimada or "N/D",
        "fecha_pago":     fecha_pago or "N/D",
    }
    campos = _TEMPLATE_PARAMS.get(template_name, ["manifiesto"])
    return [valores[campo] for campo in campos]


def _log_sent(manifiesto: int, template_name: str, phone: str, status: str, error: str | None = None, ms_id: int | None = None) -> None:
    """Registra el envío en messages_sent vía REST.

    Si ms_id está presente, actualiza el registro pending existente
    en lugar de insertar uno nuevo (usado para pago_realizado).
    """
    supabase_url = os.environ['SUPABASE_URL']
    headers = {
        "apikey":        os.environ["SUPABASE_SERVICE_KEY"],
        "Authorization": f"Bearer {os.environ['SUPABASE_SERVICE_KEY']}",
        "Content-Type":  "application/json",
        "Prefer":        "return=minimal",
    }
    try:
        with httpx.Client(timeout=10) as client:
            if ms_id:
                client.patch(
                    f"{supabase_url}/rest/v1/messages_sent?id=eq.{ms_id}",
                    headers=headers,
                    json={"status": status, "error": error},
                )
            else:
                client.post(
                    f"{supabase_url}/rest/v1/messages_sent",
                    headers=headers,
                    json={
                        "manifiesto":    manifiesto,
                        "template_name": template_name,
                        "phone":         phone,
                        "status":        status,
                        "error":         error,
                    },
                )
    except Exception as exc:
        logger.warning("log_sent_failed", extra={"manifiesto": manifiesto, "error": str(exc)})


def _fetch_pending_pago_realizado(client: httpx.Client, headers: dict) -> list[dict]:
    """Obtiene TODOS los registros pending de pago_realizado con el valor pagado.

    Pagina automáticamente para evitar truncar con límites fijos.
    """
    _PAGE_SIZE = 500
    url_ms = f"{os.environ['SUPABASE_URL']}/rest/v1/messages_sent"
    url_mf = f"{os.environ['SUPABASE_URL']}/rest/v1/manifiestos_flat"

    all_rows: list[dict] = []
    start = 0
    while True:
        r = client.get(
            url_ms,
            headers={
                **headers,
                "Range": f"{start}-{start + _PAGE_SIZE - 1}",
            },
            params={
                "select": "id,manifiesto,phone",
                "template_name": "eq.pago_realizado",
                "status": "eq.pending",
                "order": "sent_at.asc",
            },
        )
        r.raise_for_status()
        chunk = r.json()
        if not chunk:
            break
        all_rows.extend(chunk)
        if len(chunk) < _PAGE_SIZE:
            break
        start += _PAGE_SIZE

    if not all_rows:
        return []

    manifiestos = [str(r["manifiesto"]) for r in all_rows]
    lookup: dict = {}

    # Fetch manifiestos_flat in chunks of 100 (Supabase REST in() filter limit)
    _MF_CHUNK = 100
    for i in range(0, len(manifiestos), _MF_CHUNK):
        chunk = manifiestos[i:i + _MF_CHUNK]
        r2 = client.get(
            url_mf,
            headers=headers,
            params={
                "select": "manifiesto,celular,valor_pagado,fecha_pago",
                "manifiesto": f"in.({','.join(chunk)})",
            },
        )
        r2.raise_for_status()
        for m in r2.json():
            lookup[m["manifiesto"]] = m

    result = []
    for row in all_rows:
        m = row["manifiesto"]
        info = lookup.get(m)
        if not info or not info.get("valor_pagado"):
            continue
        result.append({
            "ms_id": row["id"],
            "manifiesto": m,
            "phone": row["phone"],
            "template_name": "pago_realizado",
            "fecha_pago": info.get("fecha_pago"),
        })
    return result


def _fmt(d: str | None) -> str | None:
    if not d:
        return None
    try:
        dt = datetime.strptime(d, "%Y-%m-%d")
        mes = _SPANISH_MONTHS.get(dt.month, str(dt.month))
        return f"{dt.day} de {mes} de {dt.year}"
    except ValueError:
        return d


def _format_phone(raw: str) -> str:
    cleaned = re.sub(r"[\+\-\s\(\)]", "", raw)
    if len(cleaned) == 10 and cleaned.isdigit():
        return f"57{cleaned}"
    if len(cleaned) == 12 and cleaned.startswith("57") and cleaned.isdigit():
        return cleaned
    if len(cleaned) > 10 and cleaned.isdigit():
        return cleaned
    return raw


def _send_message(template: str, phone: str, template_name_real: str, params: list[str]) -> None:
    """Envía el mensaje por plantilla aprobada o texto libre según WA_SEND_MODE."""
    if _SEND_MODE == "text":
        send_whatsapp(phone, render_text(template, *params))
    else:
        send_whatsapp_template(phone, template_name_real, params)


def _send_one(item: dict) -> bool:
    """Envía un único mensaje y registra el resultado. Devuelve True si salió bien."""
    manifiesto = item["manifiesto"]
    phone = item["phone"]
    template = item["template"]
    ms_id = item.get("ms_id")
    try:
        _send_message(template, phone, item["template_name_real"], item["params"])
        _log_sent(manifiesto, template, phone, "sent", ms_id=ms_id)
        return True
    except Exception as exc:
        logger.warning("auto_notify_send_failed",
                       extra={"manifiesto": manifiesto, "phone": phone, "error": str(exc)})
        _log_sent(manifiesto, template, phone, "error", str(exc), ms_id=ms_id)
        return False


def run_auto_notify(manifestos: list[int] | None = None,
                    templates: list[str] | None = None) -> dict:
    """Ejecuta una ronda de notificaciones automáticas.

    Consulta get_pendientes_notificacion para los 4 templates de saldo,
    más messages_sent para pago_realizado, y envía WhatsApp a conductores.
    Los mensajes se agrupan por plantilla y cada lote se envía en paralelo.

    Args:
        manifestos: Si se pasa, limita el procesamiento solo a esos manifiestos
                    (útil para pruebas aisladas sin barrer datos reales).
        templates: Si se pasa, limita el procesamiento solo a esas plantillas
                   (el scheduler usa una plantilla por job para espaciarlas).
    """
    wa_token = os.environ.get("WA_TOKEN", "")
    wa_phone_id = os.environ.get("WA_PHONE_NUMBER_ID", "")
    if not wa_token or not wa_phone_id:
        logger.warning("wa_not_configured", extra={"detail": "WA_TOKEN o WA_PHONE_NUMBER_ID vacíos"})
        return {"status": "skipped", "reason": "WhatsApp no configurado"}

    url_rpc = f"{os.environ['SUPABASE_URL']}/rest/v1/rpc/get_pendientes_notificacion"
    headers = {
        "apikey":        os.environ["SUPABASE_SERVICE_KEY"],
        "Authorization": f"Bearer {os.environ['SUPABASE_SERVICE_KEY']}",
        "Content-Type":  "application/json",
    }

    try:
        with httpx.Client(timeout=30) as client:
            r = client.post(url_rpc, headers=headers)
            r.raise_for_status()
            pendientes: list = r.json() or []
            pagados = _fetch_pending_pago_realizado(client, headers)
    except Exception as exc:
        logger.error("get_pendientes_failed", extra={"error": str(exc)})
        return {"status": "error", "error": str(exc)}

    if manifestos is not None:
        allowed = set(manifestos)
        pendientes = [p for p in pendientes if p.get("manifiesto") in allowed]
        pagados = [p for p in pagados if p.get("manifiesto") in allowed]

    # Procesar primero los pago_realizado (urgentes) y luego los saldos.
    all_items: list[dict] = list(pagados) + list(pendientes)
    if templates is not None:
        allowed_t = set(templates)
        all_items = [i for i in all_items if i.get("template_name") in allowed_t]

    if not all_items:
        logger.info("auto_notify_no_pendientes")
        return {"status": "ok", "sent": 0, "errors": 0, "skipped": 0, "total": 0}

    # Normaliza cada item en un registro listo para enviar (o lo descarta).
    sendable: list[dict] = []
    skipped = 0
    for item in all_items:
        manifiesto = item.get("manifiesto")
        raw_phone = item.get("phone") or item.get("celular") or ""
        phone = _format_phone(raw_phone)
        template = item.get("template_name")

        if not phone or not template:
            skipped += 1
            logger.debug("auto_notify_skipped",
                         extra={"manifiesto": manifiesto, "reason": "missing phone or template"})
            continue

        # Defense in depth: saltar saldo_novedad_pendiente si novedades es ruido
        if template == 'saldo_novedad_pendiente' and _is_novedad_noise(item.get('novedades')):
            skipped += 1
            logger.debug("auto_notify_skipped",
                         extra={"manifiesto": manifiesto, "reason": "novedad noise"})
            continue

        template_name_real = _TEMPLATE_NAMES.get(template)
        if not template_name_real:
            skipped += 1
            logger.debug("auto_notify_skipped",
                         extra={"manifiesto": manifiesto, "reason": f"sin plantilla WABA para {template}"})
            continue

        fecha_str = _fmt(item.get("fecha_estimada"))
        fecha_pago_str = _fmt(item.get("fecha_pago"))
        params = _template_params(template, manifiesto, fecha_str, fecha_pago_str)
        sendable.append({
            "manifiesto": manifiesto,
            "phone": phone,
            "template": template,
            "template_name_real": template_name_real,
            "params": params,
            "ms_id": item.get("ms_id"),
        })

    total = len(all_items)

    # Agrupa por plantilla respetando TEMPLATE_ORDER.
    groups: dict[str, list[dict]] = {}
    for s in sendable:
        groups.setdefault(s["template"], []).append(s)
    ordered_templates = [t for t in TEMPLATE_ORDER if t in groups]

    sent = 0
    errors = 0
    consecutive_errors = 0

    for template in ordered_templates:
        batch = groups[template]
        if consecutive_errors >= _MAX_CONSECUTIVE_ERRORS:
            logger.error("auto_notify_too_many_consecutive_errors",
                         extra={"consecutive_errors": consecutive_errors,
                                "template": template, "remaining": len(batch)})
            break

        logger.info("auto_notify_batch_start",
                    extra={"template": template, "count": len(batch), "mode": _SEND_MODE})

        if len(batch) == 1:
            results = [_send_one(batch[0])]
        else:
            workers = min(_MAX_CONCURRENT_SENDS, len(batch))
            with ThreadPoolExecutor(max_workers=workers) as ex:
                futures = {ex.submit(_send_one, item): item for item in batch}
                results = [fut.result() for fut in as_completed(futures)]

        batch_sent = sum(1 for ok in results if ok)
        batch_errors = len(results) - batch_sent
        sent += batch_sent
        errors += batch_errors
        if batch_errors:
            consecutive_errors += batch_errors
        else:
            consecutive_errors = 0

        logger.info("auto_notify_batch_done",
                    extra={"template": template, "sent": batch_sent, "errors": batch_errors})

    logger.info("auto_notify_done",
                extra={"sent": sent, "errors": errors, "skipped": skipped, "total": total})
    return {"status": "ok", "sent": sent, "errors": errors, "skipped": skipped, "total": total}


def run_auto_notify_cycle(manifestos: list[int] | None = None,
                          templates: list[str] | None = None,
                          interval_minutes: int = 5) -> dict:
    """Ejecuta un ciclo completo: una plantilla a la vez, esperando el intervalo.

    Reutiliza run_auto_notify con un filtro de una sola plantilla por iteración
    y duerme `interval_minutes` entre lotes. Es el equivalente al espaciado del
    scheduler pero disparable de forma manual (POST /admin/auto-notify-cycle).
    """
    target_templates = list(templates or TEMPLATE_ORDER)
    results = []
    for i, template in enumerate(target_templates):
        result = run_auto_notify(manifestos=manifestos, templates=[template])
        results.append({"template": template, **result})
        if i < len(target_templates) - 1:
            logger.info("auto_notify_cycle_wait",
                        extra={"template": template, "minutes": interval_minutes})
            time.sleep(interval_minutes * 60)
    return {"status": "ok", "cycle": results}
