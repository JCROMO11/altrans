"""
Crea/actualiza los usuarios de producción en Supabase Auth.

Login: cédula + contraseña.  El email almacenado en Supabase es
{cedula}@altrans.internal (sintético, nunca se envía al usuario).

Roles activos: gerencia | logistico | tesoreria | digitador | financiero
Pendientes (comentados): administrativo, contadora

Idempotente: si el usuario ya existe lo actualiza; si no, lo crea.

Uso:
    python -m etl_individual.seed_users               # crea/actualiza todos
    python -m etl_individual.seed_users --listar      # lista usuarios actuales
    python -m etl_individual.seed_users --dry-run     # muestra qué haría sin ejecutar
    python -m etl_individual.seed_users --solo 1085336031  # solo una cédula
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

DEFAULT_PASSWORD = "altrans2026"

# ── Usuarios de producción ────────────────────────────────────────────────────
# Fuente: USUARIOS DRIVE PRODUCCION ALTRANS.xlsx
# email = {cedula}@altrans.internal (sintético, invisible al usuario)
# El usuario inicia sesión con su cédula como usuario y la contraseña.

USUARIOS = [
    # ── GERENCIA ─────────────────────────────────────────────────────────────
    {"cedula": "1193057039", "nombre": "JULIAN FUERTES ENRIQUEZ",
     "cargo": "SUBGERENTE",          "rol": "gerencia"},
    {"cedula": "13015050",   "nombre": "JULIO FUERTES VELA",
     "cargo": "GERENTE",             "rol": "gerencia"},

    # ── LOGÍSTICO ────────────────────────────────────────────────────────────
    {"cedula": "1233188009", "nombre": "ANGIE PAOLA OVIEDO OBANDO",
     "cargo": "DIRECTOR OPERATIVO Y LOGISTICO", "rol": "logistico"},
    {"cedula": "1085928088", "nombre": "CATHERIN MARCELA RODRIGUEZ PASTAS",
     "cargo": "DIRECTOR OPERATIVO Y LOGISTICO", "rol": "logistico"},
    {"cedula": "1085336031", "nombre": "HECTOR FLAMINIO MELO LEITON",
     "cargo": "DIRECTOR OPERATIVO Y LOGISTICO", "rol": "logistico"},
    {"cedula": "29974867",   "nombre": "LARIZA HERRERA VISCONDE",
     "cargo": "AUXILIAR OPERATIVO Y LOGISTICO CALI", "rol": "logistico"},
    {"cedula": "1118292701", "nombre": "ELIANA LUCIA GUEVARA RAMOS",
     "cargo": "AUXILIAR OPERATIVO Y LOGISTICO", "rol": "logistico"},
    {"cedula": "1085899873", "nombre": "INGRID VANESSA CALPA YANDUN",
     "cargo": "AUXILIAR OPERATIVA Y LOGISTICA", "rol": "logistico"},
    {"cedula": "1007156550", "nombre": "KATHERINE STHEFANIA VACA SOLANO",
     "cargo": "AUXILIAR OPERATIVA Y LOGISTICA", "rol": "logistico"},
    {"cedula": "1151441687", "nombre": "MIGUEL ANGEL PEÑALOZA GALLEGO",
     "cargo": "COORDINADOR AGENCIA BUENAVENTURA", "rol": "logistico"},

    # ── DIGITADOR (también tiene permisos logistico) ──────────────────────────
    {"cedula": "1004564051", "nombre": "MARCELA STEFANYA CUASQUEN ARCINIEGAS",
     "cargo": "AUXILIAR OPERATIVA Y LOGISTICA", "rol": "digitador"},

    # ── TESORERÍA (también tiene permisos logistico) ──────────────────────────
    {"cedula": "36861498",   "nombre": "JOHANA DEL SOCORRO UNIGARRO CEBALLOS",
     "cargo": "AUXILIAR FINANCIERA", "rol": "tesoreria"},

    # ── FINANCIERO ───────────────────────────────────────────────────────────
    {"cedula": "37008344",   "nombre": "MARIA ELENA LUCERO PAREDES",
     "cargo": "AUXILIAR CONTABLE",   "rol": "financiero"},

    # ── PENDIENTES (descomentar cuando Julian confirme permisos) ─────────────
    # {"cedula": "1086418433", "nombre": "OSCAR RODRIGUEZ MONTAÑEZ",
    #  "cargo": "ASISTENTE ADMINISTRATIVO", "rol": "administrativo"},
    # {"cedula": "37008966",   "nombre": "ANA LUCIA MEDEZ SUAREZ",
    #  "cargo": "CONTADORA", "rol": "contadora"},
]


def _email(cedula: str) -> str:
    return f"{cedula}@altrans.internal"


def _list_users() -> list[dict]:
    r = requests.get(ADMIN_API, headers=HEADERS, params={"per_page": 1000}, timeout=15)
    r.raise_for_status()
    return r.json().get("users", [])


def _find_by_email(email: str, users: list[dict]) -> dict | None:
    for u in users:
        if (u.get("email") or "").lower() == email.lower():
            return u
    return None


def _create(u: dict, password: str) -> None:
    payload = {
        "email":         _email(u["cedula"]),
        "password":      password,
        "email_confirm": True,
        "app_metadata":  {"role": u["rol"]},
        "user_metadata": {"nombre": u["nombre"], "cedula": u["cedula"], "cargo": u["cargo"]},
    }
    r = requests.post(ADMIN_API, headers=HEADERS, json=payload, timeout=15)
    if not r.ok:
        raise RuntimeError(f"Crear {u['cedula']}: {r.status_code} {r.text}")


def _update(user_id: str, u: dict) -> None:
    payload = {
        "app_metadata":  {"role": u["rol"]},
        "user_metadata": {"nombre": u["nombre"], "cedula": u["cedula"], "cargo": u["cargo"]},
    }
    r = requests.put(f"{ADMIN_API}/{user_id}", headers=HEADERS, json=payload, timeout=15)
    if not r.ok:
        raise RuntimeError(f"Actualizar {user_id}: {r.status_code} {r.text}")


def seed(usuarios: list[dict], password: str, dry_run: bool = False) -> None:
    existentes = _list_users()
    print(f"Usuarios actuales en Supabase: {len(existentes)}\n")

    for u in usuarios:
        email     = _email(u["cedula"])
        existente = _find_by_email(email, existentes)

        if existente:
            actual_rol = (existente.get("app_metadata") or {}).get("role")
            if actual_rol == u["rol"]:
                print(f"  · {u['rol']:14s} {u['cedula']:12s}  {u['nombre']} — sin cambios")
            else:
                if not dry_run:
                    _update(existente["id"], u)
                tag = "[DRY]" if dry_run else "↻"
                print(f"  {tag} {u['rol']:14s} {u['cedula']:12s}  {u['nombre']} (rol: {actual_rol!r} → {u['rol']!r})")
        else:
            if not dry_run:
                _create(u, password)
            tag = "[DRY]" if dry_run else "✚"
            print(f"  {tag} {u['rol']:14s} {u['cedula']:12s}  {u['nombre']} — creado")

    if not dry_run:
        print(f"\nPassword inicial (todos): {password}")
        print("Login: cédula como usuario, contraseña arriba.")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Seed de usuarios de producción en Supabase Auth (login por cédula)")
    parser.add_argument("--solo",     metavar="CEDULA",
                        help="Crear/actualizar solo el usuario con esta cédula")
    parser.add_argument("--password", default=DEFAULT_PASSWORD,
                        help=f"Password inicial (default: {DEFAULT_PASSWORD})")
    parser.add_argument("--listar",   action="store_true",
                        help="Solo listar usuarios actuales y su rol")
    parser.add_argument("--dry-run",  action="store_true",
                        help="Mostrar qué haría sin ejecutar nada")
    args = parser.parse_args()

    if args.listar:
        users = _list_users()
        if not users:
            print("(ninguno)")
            return 1
        print(f"{'Cédula':<15s} {'Rol':<15s} {'Nombre / Email'}")
        print("-" * 80)
        cedulas_presentes = set()
        for u in sorted(users, key=lambda x: (x.get("app_metadata") or {}).get("role", "")):
            rol    = (u.get("app_metadata") or {}).get("role", "—")
            nombre = (u.get("user_metadata") or {}).get("nombre") or u.get("email", "?")
            cedula = (u.get("user_metadata") or {}).get("cedula", "—")
            print(f"{cedula:<15s} {rol:<15s} {nombre}")
            cedulas_presentes.add(cedula)

        esperados = {u["cedula"] for u in USUARIOS}
        faltan = esperados - cedulas_presentes
        if faltan:
            print(f"\n❌ Faltan cédulas: {', '.join(sorted(faltan))}")
            print("   Ejecuta: make seed-users")
            return 1
        print(f"\n✅ Los {len(USUARIOS)} usuarios están en Supabase")
        return 0

    target = [u for u in USUARIOS if u["cedula"] == args.solo] if args.solo else USUARIOS
    if args.solo and not target:
        sys.exit(f"ERROR: cédula {args.solo!r} no encontrada en USUARIOS")

    seed(target, args.password, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
