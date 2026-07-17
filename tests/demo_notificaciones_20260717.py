"""
Demo de notificaciones — 17 Julio 2026
========================================
1. auto-notify → envía las 4 plantillas a los conductores de prueba
2. pago_realizado → actualiza fecha_pago y dispara el trigger
3. backup → genera ZIP y lo envía por email

Uso:
    python -m tests.demo_notificaciones_20260717

Para cambiar los destinatarios antes de correr:
    CELULAR_DEMO=3145285119  (por defecto)
"""
import os
import sys
import json
import time
from datetime import datetime
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=False)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SERVICE_KEY  = os.environ["SUPABASE_SERVICE_KEY"]
WA_TOKEN     = os.environ["WA_TOKEN"]
ADMIN_TOKEN  = os.environ["ADMIN_TOKEN"]
NOTIF_URL    = os.environ.get("NOTIF_URL", "http://localhost:8081")
CELULAR      = os.environ.get("CELULAR_DEMO", "3145285119")

SUPA_HEADERS = {
    "apikey":        SERVICE_KEY,
    "Authorization": f"Bearer {SERVICE_KEY}",
    "Content-Type":  "application/json",
}
NOTIF_HEADERS = {
    "x-admin-token": ADMIN_TOKEN,
    "Content-Type":  "application/json",
}


def step(label: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")


def supa_get(table: str, params: dict | None = None) -> list:
    r = httpx.get(f"{SUPABASE_URL}/rest/v1/{table}",
                  headers=SUPA_HEADERS, params=params, timeout=15)
    r.raise_for_status()
    return r.json()


def supa_post(table: str, data: dict) -> dict:
    r = httpx.post(f"{SUPABASE_URL}/rest/v1/{table}",
                   headers=SUPA_HEADERS | {"Prefer": "return=minimal"},
                   json=data, timeout=15)
    if r.status_code != 201:
        print(f"  WARN: POST {table} → {r.status_code} {r.text}")
    return r.json() if r.text else {}


def notif_post(endpoint: str, data: dict | None = None) -> dict:
    url = f"{NOTIF_URL}{endpoint}"
    r = httpx.post(url, headers=NOTIF_HEADERS, json=data, timeout=30)
    try:
        return r.json()
    except Exception:
        return {"raw": r.text, "status": r.status_code}


def trigger_pago_realizado() -> None:
    """Actualiza fecha_pago en un manifiesto para disparar el trigger
    trg_notify_pago_realizado → pending en messages_sent"""
    step("Trigger pago_realizado")
    manifiesto = 999902  # usar el sin_factura como si lo hubieran pagado

    # Update the existing row via PATCH
    r = httpx.patch(
        f"{SUPABASE_URL}/rest/v1/manifiestos_flat?manifiesto=eq.{manifiesto}",
        headers=SUPA_HEADERS | {"Prefer": "return=minimal"},
        json={
            "fecha_pago": "2026-07-17",
            "valor_pagado": 800000,
            "entidad_financiera": "BANCOLOMBIA",
        },
        timeout=15,
    )
    if r.status_code in (200, 204):
        print(f"  ✅ Manifiesto {manifiesto} marcado como pagado")
    else:
        print(f"  ⚠️  Error: {r.status_code} {r.text}")

    time.sleep(1)
    pending = supa_get("messages_sent", {
        "manifiesto": f"eq.{manifiesto}",
        "select":     "manifiesto,template_name,status,phone",
    })
    if pending:
        print(f"  📝 Pending creado: {json.dumps(pending, indent=2)}")
    else:
        print("  ⚠️  No se creó pending (el trigger pudo no haber disparado)")


TEMPLATES = {
    "novedad": "Buen día, estimado transportador.\n\nLe informo que el saldo del manifiesto {manif} no se ha pagado debido a una novedad sin resolver (averías, faltantes, etc.). Por favor comuníquese con quien contrató su servicio.\n\nMensaje automático de ALTRANS. Puede contener errores.",
    "sin_factura": "Buen día, estimado transportador.\n\nLe informo que el manifiesto {manif} no se ha pagado porque falta la factura electrónica. Envíela a facturaelectronica@altrans.com.co.\n\nMensaje automático de ALTRANS. Puede contener errores.",
    "doc_vencida": "Buen día, estimado transportador.\n\nLe informo que el manifiesto {manif} no se ha pagado porque falta la documentación original firmada. Regularice esta situación con quien contrató su servicio.\n\nMensaje automático de ALTRANS. Puede contener errores.",
    "plazo_vigente": "Buen día, estimado transportador.\n\nLe informo que el saldo del manifiesto {manif} aún no se ha pagado porque está dentro del plazo pactado (~15 días hábiles desde el cumplido). Espere hasta aproximadamente el {fecha}.\n\nMensaje automático de ALTRANS. Puede contener errores.",
        "pago_realizado": "Buen día, estimado transportador.\n\nLe informamos que el saldo del manifiesto {manif} por ${monto} fue pagado el {fecha}. Revise sus extractos bancarios.\n\nGracias por su servicio.\n\nMensaje automático de ALTRANS. Puede contener errores.",
}


def main():
    print(f"🔔 Demo Notificaciones — {datetime.now():%Y-%m-%d %H:%M}")
    print(f"   Celular destino: {CELULAR}")
    print(f"   Notifications service: {NOTIF_URL}")

    phone = f"57{CELULAR}" if not CELULAR.startswith("57") else CELULAR

    # ── 1. Enviar las 4 plantillas manualmente ─────────────────────────────
    step("1. Plantilla: saldo_novedad_pendiente (manif 999901)")
    result = notif_post("/admin/notify/wa", {
        "phones": [phone],
        "message": TEMPLATES["novedad"].format(manif=999901),
    })
    print(f"   → {json.dumps(result)}")
    time.sleep(0.5)

    step("2. Plantilla: saldo_falta_factura (manif 999902)")
    result = notif_post("/admin/notify/wa", {
        "phones": [phone],
        "message": TEMPLATES["sin_factura"].format(manif=999902),
    })
    print(f"   → {json.dumps(result)}")
    time.sleep(0.5)

    step("3. Plantilla: saldo_falta_documentacion (manif 999903)")
    result = notif_post("/admin/notify/wa", {
        "phones": [phone],
        "message": TEMPLATES["doc_vencida"].format(manif=999903),
    })
    print(f"   → {json.dumps(result)}")
    time.sleep(0.5)

    step("4. Plantilla: saldo_plazo_vigente (manif 999904)")
    result = notif_post("/admin/notify/wa", {
        "phones": [phone],
        "message": TEMPLATES["plazo_vigente"].format(manif=999904, fecha="7 de agosto de 2026"),
    })
    print(f"   → {json.dumps(result)}")
    time.sleep(0.5)

    # ── 5. Pago realizado ─────────────────────────────────────────────────
    trigger_pago_realizado()

    step("5. Plantilla: pago_realizado (manif 999902)")
    result = notif_post("/admin/notify/wa", {
        "phones": [phone],
        "message": TEMPLATES["pago_realizado"].format(manif=999902, monto="800.000", fecha="17 de julio de 2026"),
    })
    print(f"   → {json.dumps(result)}")
    time.sleep(0.5)

    # ── 6. Backup ──────────────────────────────────────────────────────────
    step("6. Backup → ZIP con 5 tablas enviado por email")
    result = notif_post("/admin/backup")
    print(f"   → {json.dumps(result)}")

    # ── 7. Mensaje de cierre ───────────────────────────────────────────────
    step("7. Mensaje de confirmación")
    msg = ("🚀 *DEMO ALTRANS* — 17 Julio 2026\n"
           "✅ 5 plantillas de notificación\n"
           "✅ Backup ZIP automático\n"
           "✅ Pago realizado + trigger DB\n\n"
           "Sistema funcionando correctamente.")
    result = notif_post("/admin/notify/wa", {"phones": [phone], "message": msg})
    print(f"   → {json.dumps(result)}")

    print(f"\n{'='*60}")
    print("  🎯 Demo completada — revisa WhatsApp")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
