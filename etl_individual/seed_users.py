"""
Crea usuarios de prueba en Supabase Auth, uno por cada rol.

Roles: admin, financiero, operativo, tesoreria, digitador.
Pwd inicial común para todos: cambiar tras el primer login.

Idempotente: si el usuario ya existe, le actualiza el rol; si no, lo crea.

Uso:
    python -m etl_individual.seed_users                  # crea todos los roles
    python -m etl_individual.seed_users --solo admin     # solo uno
    python -m etl_individual.seed_users --listar         # lista los users actuales
"""
import argparse
import os
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_ROOT / ".env", override=False)

SUPA_URL    = os.environ.get("SUPABASE_URL")
SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")
if not (SUPA_URL and SERVICE_KEY):
    sys.exit("ERROR: SUPABASE_URL y SUPABASE_SERVICE_KEY deben estar en .env")

ADMIN_API = f"{SUPA_URL}/auth/v1/admin/users"
HEADERS = {
    "apikey":        SERVICE_KEY,
    "Authorization": f"Bearer {SERVICE_KEY}",
    "Content-Type":  "application/json",
}

# ── Configuración de usuarios de prueba ──────────────────────────────────────
# Cambiar emails/passwords según necesites. Estos son defaults seguros.
DEFAULT_PASSWORD = "altrans-test-2026"

USUARIOS = {
    "admin":      {"email": "admin@altrans.test",      "nombre": "Admin Prueba"},
    "financiero": {"email": "financiero@altrans.test", "nombre": "Financiero Prueba"},
    "operativo":  {"email": "operativo@altrans.test",  "nombre": "Operativo Prueba"},
    "tesoreria":  {"email": "tesoreria@altrans.test",  "nombre": "Tesorería Prueba"},
    "digitador":  {"email": "digitador@altrans.test",  "nombre": "Digitador Prueba"},
}


def _list_users() -> list[dict]:
    r = requests.get(ADMIN_API, headers=HEADERS, params={"per_page": 1000}, timeout=15)
    r.raise_for_status()
    return r.json().get("users", [])


def _find_by_email(email: str, users: list[dict]) -> dict | None:
    for u in users:
        if (u.get("email") or "").lower() == email.lower():
            return u
    return None


def _create(email: str, password: str, rol: str, nombre: str) -> dict:
    payload = {
        "email":             email,
        "password":          password,
        "email_confirm":     True,  # evita email de verificación
        "app_metadata":      {"role": rol},
        "user_metadata":     {"nombre": nombre},
    }
    r = requests.post(ADMIN_API, headers=HEADERS, json=payload, timeout=15)
    if not r.ok:
        raise RuntimeError(f"Crear {email}: {r.status_code} {r.text}")
    return r.json()


def _update_role(user_id: str, rol: str, nombre: str) -> dict:
    payload = {
        "app_metadata":  {"role": rol},
        "user_metadata": {"nombre": nombre},
    }
    r = requests.put(f"{ADMIN_API}/{user_id}", headers=HEADERS, json=payload, timeout=15)
    if not r.ok:
        raise RuntimeError(f"Actualizar {user_id}: {r.status_code} {r.text}")
    return r.json()


def seed(roles: list[str], password: str) -> None:
    existentes = _list_users()
    print(f"Usuarios actuales en Supabase: {len(existentes)}\n")

    for rol in roles:
        cfg = USUARIOS[rol]
        email  = cfg["email"]
        nombre = cfg["nombre"]
        existente = _find_by_email(email, existentes)

        if existente:
            actual = (existente.get("app_metadata") or {}).get("role")
            if actual == rol:
                print(f"  · {rol:10s} {email:35s} ya existe con rol correcto — skip")
            else:
                _update_role(existente["id"], rol, nombre)
                print(f"  ↻ {rol:10s} {email:35s} actualizado ({actual!r} → {rol!r})")
        else:
            _create(email, password, rol, nombre)
            print(f"  ✚ {rol:10s} {email:35s} creado")

    print(f"\nPassword inicial (todos): {password}")
    print("Cambia la contraseña tras el primer login desde el dashboard.")


def main() -> int:
    parser = argparse.ArgumentParser(description="Seed de usuarios test por rol en Supabase Auth")
    parser.add_argument("--solo", choices=list(USUARIOS),
                        help="Crear/actualizar solo un rol")
    parser.add_argument("--password", default=DEFAULT_PASSWORD,
                        help=f"Password inicial (default: {DEFAULT_PASSWORD})")
    parser.add_argument("--listar", action="store_true",
                        help="Solo listar usuarios actuales y su rol")
    args = parser.parse_args()

    if args.listar:
        users = _list_users()
        if not users:
            print("(ninguno)")
            return 0
        print(f"{'Email':<40s} {'Rol':<15s} {'ID'}")
        print("-" * 90)
        for u in users:
            rol = (u.get("app_metadata") or {}).get("role", "—")
            print(f"{(u.get('email') or '?'):<40s} {rol:<15s} {u['id']}")
        return 0

    roles = [args.solo] if args.solo else list(USUARIOS)
    seed(roles, args.password)
    return 0


if __name__ == "__main__":
    sys.exit(main())
