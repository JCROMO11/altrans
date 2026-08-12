"""
Valida el token de Meta y lo actualiza en .env y en los servicios de Railway.

Los tokens de usuario de la WhatsApp Cloud API expiran (~24h). Este script
centraliza el cambio para no hacerlo a mano cada día:

  - Valida el token contra debug_token (muestra expiración y permisos).
  - Escribe el token nuevo en .env (raíz del proyecto).
  - Lo actualiza en los servicios de Railway indicados con --services.

Uso:
  python -m scripts.update_wa_token --token <TOKEN> \
      --services "<id_notif>:notifications,<id_chatbot>:chatbot"
  python -m scripts.update_wa_token --status

Requiere: .env con WA_TOKEN (para --status), CLI de railway autenticado.
"""
import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = ROOT / ".env"

GRAPH = "https://graph.facebook.com/v23.0"


def _load_env() -> dict:
    env: dict = {}
    if ENV_PATH.exists():
        for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                env[k.strip()] = v.strip()
    return env


def validar(token: str) -> dict:
    r = httpx.get(
        f"{GRAPH}/debug_token",
        params={"input_token": token},
        headers={"Authorization": f"Bearer {token}"},
        timeout=20,
    )
    r.raise_for_status()
    data = r.json().get("data", {})
    return data


def _fmt_expiracion(ts) -> str:
    if not ts:
        return "desconocida"
    dt = time.gmtime(ts)
    return time.strftime("%Y-%m-%d %H:%M UTC", dt)


def _write_env(token: str) -> None:
    env = _load_env()
    env["WA_TOKEN"] = token
    lines = [f"{k}={v}" for k, v in env.items()]
    ENV_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"✅ WA_TOKEN actualizado en {ENV_PATH.name}")


def _railway_set(service_id: str, token: str, nombre: str) -> None:
    cmd = ["railway", "variables", "--service", service_id, "--set", f"WA_TOKEN={token}"]
    print(f"→ Actualizando {nombre} (service {service_id})...")
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        print(f"  ❌ {nombre}: {res.stderr.strip() or res.stdout.strip()}")
        return
    print(f"  ✅ {nombre}: variable actualizada")


def main() -> None:
    parser = argparse.ArgumentParser(description="Actualiza WA_TOKEN en .env y Railway")
    parser.add_argument("--token", help="Token de Meta nuevo")
    parser.add_argument("--services", default="", help="csv 'id:nombre' de servicios Railway")
    parser.add_argument("--status", action="store_true", help="Mostrar vigencia del token actual")
    args = parser.parse_args()

    if args.status:
        env = _load_env()
        tok = env.get("WA_TOKEN", "")
        if not tok:
            print("❌ No hay WA_TOKEN en .env")
            sys.exit(1)
        data = validar(tok)
        if not data.get("is_valid"):
            print("❌ Token inválido o expirado")
            print("   Detalle:", data)
            sys.exit(1)
        print(f"✅ Token válido  | tipo: {data.get('type')}  | expira: {_fmt_expiracion(data.get('expires_at'))}")
        print("   Permisos:", ", ".join(data.get("scopes", [])))
        return

    if not args.token:
        print("❌ Usa --token <TOKEN> (o --status)")
        sys.exit(1)

    data = validar(args.token)
    if not data.get("is_valid"):
        print("❌ El token no es válido. Verifica que copiaste el token completo de Meta.")
        print("   Detalle:", data)
        sys.exit(1)

    print(f"✅ Token válido  | tipo: {data.get('type')}  | expira: {_fmt_expiracion(data.get('expires_at'))}")
    print("   Permisos:", ", ".join(data.get("scopes", [])))
    if data.get("type") != "SYSTEM_USER":
        print("⚠️  Es un token de USUARIO: expira. Para producción usa un token de System User.")

    _write_env(args.token)

    for entrada in [e for e in args.services.split(",") if e]:
        sid, _, nombre = entrada.partition(":")
        if not sid:
            continue
        _railway_set(sid, args.token, nombre or sid)

    print("\nListo. Recuerda que Railway redeploya el servicio al cambiar variables.")


if __name__ == "__main__":
    main()
