import os, sys, httpx
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

phone = os.environ.get('PHONE', sys.argv[1] if len(sys.argv) > 1 else '')
msg   = os.environ.get('MSG',   sys.argv[2] if len(sys.argv) > 2 else '')
if not phone or not msg:
    print("Uso: PHONE=57... MSG='texto' python3 scripts/send_wa.py")
    sys.exit(1)

r = httpx.post(
    f"https://graph.facebook.com/v20.0/{os.environ['WA_PHONE_NUMBER_ID']}/messages",
    headers={"Authorization": f"Bearer {os.environ['WA_TOKEN']}", "Content-Type": "application/json"},
    json={"messaging_product": "whatsapp", "to": phone, "type": "text", "text": {"body": msg}},
    timeout=10)
r.raise_for_status()
print(f"✅ Enviado a {phone}")
