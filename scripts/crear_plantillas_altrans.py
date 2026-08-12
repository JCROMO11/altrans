"""
Crea las 5 plantillas de notificaciones Altrans en la WABA (categoría UTILITY).

Usa la API de plantillas de mensajes de Meta:
  POST https://graph.facebook.com/v23.0/{WABA_ID}/message_templates

Idempotente: si una plantilla ya existe en el idioma, no la duplica.

Uso:
  python -m scripts.crear_plantillas_altrans            # crear + listar estado
  python -m scripts.crear_plantillas_altrans --list     # solo listar estado
  python -m scripts.crear_plantillas_altrans --poller N # listar cada N segundos (espera aprobación)

Env: WA_TOKEN (token de Meta con permiso whatsapp_business_management).
"""
import argparse
import os
import sys
import time
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=False)

WABA_ID = os.environ.get("WABA_ID", "2434251620392649")
GRAPH = "https://graph.facebook.com/v23.0"

# Nombre de cada plantilla y texto con variables posicionales {{N}}.
# Las fechas se pasan ya formateadas (ej. "12 de agosto de 2026").
# El título va en el componente HEADER (formato TEXT) para que el
# transportador sepa de qué trata el mensaje. Datos clave en negrita (*texto*).
PLANTILLAS = {
    "altrans_saldo_falta_factura": {
        "header": "Pendiente de pago: factura electrónica",
        "body": (
            "Buen día, estimado transportador.\n\n"
            "Le informamos que el pago del manifiesto *{{1}}* no se ha efectuado "
            "porque no se ha legalizado la factura electrónica que debe enviar el "
            "propietario, quien está obligado a hacerlo según el RUT.\n\n"
            "Por favor envíela lo antes posible a facturaelectronica@altrans.com.co. "
            "Si ya la envió correctamente, reenvíela a la persona que contrató su servicio.\n\n"
            "Mensaje automático de ALTRANS."
        ),
        "ejemplos": ["22883"],
    },
    "altrans_saldo_falta_documentacion": {
        "header": "Pendiente de pago: documentación",
        "body": (
            "Buen día, estimado transportador.\n\n"
            "Le informamos que el pago del manifiesto *{{1}}* no se ha efectuado "
            "porque no se ha cumplido formalmente con la documentación original firmada "
            "que nos permite evidenciar que el transporte concluyó satisfactoriamente.\n\n"
            "Por favor regularice esta situación según las instrucciones de quien contrató "
            "su servicio. Si envió los documentos por mensajería, rastree y envíe la guía "
            "a la persona que contrató su servicio.\n\n"
            "Mensaje automático de ALTRANS."
        ),
        "ejemplos": ["22883"],
    },
    "altrans_saldo_novedad_pendiente": {
        "header": "Pendiente de pago: novedad sin resolver",
        "body": (
            "Buen día, estimado transportador.\n\n"
            "Le informamos que el pago del manifiesto *{{1}}* no se ha efectuado "
            "debido a una novedad sin resolver, que puede ser averías, faltantes "
            "o situaciones similares.\n\n"
            "Por favor comuníquese con la persona que contrató su servicio o "
            "adelante las instrucciones que ella le haya dado.\n\n"
            "Mensaje automático de ALTRANS."
        ),
        "ejemplos": ["22883"],
    },
    "altrans_saldo_plazo_vigente": {
        "header": "Pago dentro del plazo pactado",
        "body": (
            "Buen día, estimado transportador.\n\n"
            "Le informamos que el saldo del manifiesto *{{1}}* aún no se ha pagado "
            "porque no se ha completado el plazo pactado para realizarlo.\n\n"
            "Nuestro acuerdo fue pagarlo dentro de los 15 días hábiles siguientes "
            "al completado formal del transporte. Le pedimos una espera hasta "
            "aproximadamente el *{{2}}*.\n\n"
            "Mensaje automático de ALTRANS."
        ),
        "ejemplos": ["22883", "12 de agosto de 2026"],
    },
    "altrans_pago_realizado": {
        "header": "Pago realizado",
        "body": (
            "Buen día, estimado transportador.\n\n"
            "Le informamos que el saldo del manifiesto *{{1}}* ha sido pagado "
            "exitosamente el día *{{2}}* mediante transferencia bancaria.\n\n"
            "Por favor revise sus extractos bancarios. Gracias por su servicio.\n\n"
            "Mensaje automático de ALTRANS."
        ),
        "ejemplos": ["22883", "12 de agosto de 2026"],
    },
}


def _headers() -> dict:
    return {"Authorization": f"Bearer {os.environ['WA_TOKEN']}", "Content-Type": "application/json"}


def _components(name: str) -> list[dict]:
    cfg = PLANTILLAS[name]
    comps = [
        {"type": "HEADER", "format": "TEXT", "text": cfg["header"]},
        {"type": "BODY", "text": cfg["body"], "example": {"body_text": [cfg["ejemplos"]]}},
    ]
    return comps


def listar_templates() -> None:
    r = httpx.get(
        f"{GRAPH}/{WABA_ID}/message_templates",
        params={"fields": "name,status,category,language", "limit": 100},
        headers=_headers(), timeout=20,
    )
    r.raise_for_status()
    data = r.json().get("data", [])
    if not data:
        print("(sin plantillas)")
        return
    for t in data:
        print(f"{t.get('name'):<42} {t.get('category'):<12} {t.get('status'):<10} {t.get('language')}")


def crear_todas() -> None:
    for name in PLANTILLAS:
        r = httpx.post(
            f"{GRAPH}/{WABA_ID}/message_templates",
            headers=_headers(),
            json={
                "name": name,
                "language": "es",
                "category": "UTILITY",
                "components": _components(name),
            },
            timeout=30,
        )
        try:
            body = r.json()
        except Exception:
            body = {"raw": r.text[:300]}
        if r.status_code == 200:
            print(f"✅ {name:<42} creada (id {body.get('id', '?')})")
        else:
            err = body.get("error", {})
            msg = err.get("message", r.text[:200])
            code = err.get("code", "?")
            sub = err.get("error_subcode", "")
            # 100/2388024 = ya existe contenido en este idioma → ok, idempotente
            if code == 100 and "2388024" in str(sub):
                print(f"ℹ️  {name:<42} ya existe ({msg})")
            else:
                print(f"❌ {name:<42} code={code} sub={sub} {msg}")


def poller(intervalo: float) -> None:
    print(f"→ Estado de plantillas cada {intervalo}s (Ctrl+C para salir):")
    while True:
        print(f"\n[{time.strftime('%H:%M:%S')}]")
        try:
            listar_templates()
        except Exception as exc:
            print("  error:", exc)
        time.sleep(intervalo)


def main() -> None:
    parser = argparse.ArgumentParser(description="Crea plantillas Altrans en la WABA")
    parser.add_argument("--list", action="store_true", help="Solo listar plantillas")
    parser.add_argument("--poller", type=float, metavar="SEG", help="Listar cada N segundos")
    args = parser.parse_args()

    if args.list:
        listar_templates()
        return
    if args.poller:
        poller(args.poller)
        return

    crear_todas()
    print("\n— Estado actual —")
    listar_templates()


if __name__ == "__main__":
    main()
