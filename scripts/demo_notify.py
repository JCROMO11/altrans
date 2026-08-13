import os, sys, httpx
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

phone = os.environ.get('PHONE', sys.argv[1] if len(sys.argv) > 1 else '573145285119')
tok   = os.environ['WA_TOKEN']
pnid  = os.environ['WA_PHONE_NUMBER_ID']
url   = f"https://graph.facebook.com/v23.0/{pnid}/messages"
headers = {"Authorization": f"Bearer {tok}", "Content-Type": "application/json"}

def wa(msg):
    r = httpx.post(url, headers=headers, json={"messaging_product": "whatsapp", "to": phone, "type": "text", "text": {"body": msg}}, timeout=10)
    r.raise_for_status()

templates = [
    ("Novedad pendiente (manif 999901)",
     "Buen día, estimado transportador.\n\nLe informo que el saldo del manifiesto 999901 no se ha pagado debido a una novedad sin resolver (averías, faltantes, etc.). Por favor comuníquese con quien contrató su servicio.\n\nMensaje automático de ALTRANS. Puede contener errores."),
    ("Falta factura (manif 999902)",
     "Buen día, estimado transportador.\n\nLe informo que el manifiesto 999902 no se ha pagado porque falta la factura electrónica. Envíela a facturaelectronica@altrans.com.co.\n\nMensaje automático de ALTRANS. Puede contener errores."),
    ("Documentación vencida (manif 999903)",
     "Buen día, estimado transportador.\n\nLe informo que el manifiesto 999903 no se ha pagado porque falta la documentación original firmada. Regularice esta situación con quien contrató su servicio.\n\nMensaje automático de ALTRANS. Puede contener errores."),
    ("Plazo vigente (manif 999904)",
     "Buen día, estimado transportador.\n\nLe informo que el saldo del manifiesto 999904 aún no se ha pagado porque está dentro del plazo pactado (~15 días hábiles desde el cumplido). Espere hasta aproximadamente el 7 de agosto de 2026.\n\nMensaje automático de ALTRANS. Puede contener errores."),
    ("Pago realizado (manif 999902)",
     "Buen día, estimado transportador.\n\nLe informamos que el saldo del manifiesto 999902 por $800.000 fue pagado el 17 de julio de 2026. Revise sus extractos bancarios.\n\nGracias por su servicio.\n\nMensaje automático de ALTRANS. Puede contener errores."),
]

for label, msg in templates:
    print(f"📤 {label}... ", end="", flush=True)
    wa(msg)
    print("✅")

print(f"\n🎯 {len(templates)} mensajes enviados a {phone}")
