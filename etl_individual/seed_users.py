"""
Crea/actualiza los usuarios de producción en Supabase Auth.

Login: cédula + contraseña.  El email almacenado en Supabase es
{cedula}@altrans.internal (sintético, nunca se envía al usuario).

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ROLES Y PERMISOS  (fuente: USUARIOS DRIVE PRODUCCION ALTRANS.xlsx)
Columnas referenciadas: Drive interno "PRODUCCIÓN ALTRANS S.A.S."
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  gerencia       ACCESO TOTAL (cols A–AE + eliminar + dashboard KPIs).
                 RPC: guardar_digitador, guardar_logistico,
                      guardar_estado_interno, guardar_tesoreria,
                      guardar_financiero, borrar_manifiesto.

  digitador      Cols A–Q (datos base: manifiesto, ruta, conductor,
                 vehículo, valores de despacho) + cols R–W (cumplimiento).
                 Carga masiva via Excel (A–Q).
                 RPC: guardar_digitador, guardar_logistico,
                      guardar_estado_interno.

  logistico      Cols R–W (cumplimiento operativo):
                   R=fecha_cumplido, T=condicion_pago, U=novedades,
                   V=estado_interno, W=responsable_estado_interno.
                  + campos adicionales de la app no presentes en el Drive:
                    ajustes al flete, ajustes_detalle,
                    consignacion_a_terceros.
                 NO escribe A–Q. NO puede cargar Excel.
                 RPC: guardar_logistico, guardar_estado_interno.

  tesoreria      Cols R–W (CUMPLE, igual que logístico) +
                 cols X–AA (pago conductor):
                   X=fecha_pago, Y=valor_pagado,
                   Z=entidad_financiera, AA=responsable.
                 RPC: guardar_logistico, guardar_tesoreria,
                      guardar_estado_interno.

  financiero     Col V (estado_interno) +
                 cols AB–AE (facturación):
                   AB=factura_no, AC=fecha_factura,
                   AD=mes_facturacion(auto), AE=factura_electronica.
                 Ve KPIs del dashboard.
                 RPC: guardar_estado_interno, guardar_financiero.

  contadora      Cols X–AA (pago conductor) + cols AB–AE (facturación).
                 (pendiente — descomentar en USUARIOS)
                 RPC: guardar_tesoreria, guardar_financiero.

  administrativo Col V (estado_interno) + supervisión dashboard KPIs.
                 (pendiente — descomentar en USUARIOS)
                 RPC: guardar_estado_interno.

Notas sobre metadata en Supabase:
  - `role` → app_metadata  (JWT claim, usado por RLS y RPCs en la DB).
  - `nombre`, `cedula`, `cargo` → user_metadata  (solo display, no seguridad).
  - El dashboard lee nombre de user_metadata con fallback a app_metadata.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Idempotente: si el usuario ya existe lo actualiza; si no, lo crea.

Uso:
    python -m etl_individual.seed_users                   # crea/actualiza todos
    python -m etl_individual.seed_users --generar-pw      # genera password único por usuario
    python -m etl_individual.seed_users --listar          # lista usuarios actuales
    python -m etl_individual.seed_users --dry-run         # muestra qué haría sin ejecutar
    python -m etl_individual.seed_users --solo 1085336031 # solo una cédula

Password por usuario (--generar-pw):
    Genera una contraseña única y legible por cada usuario: prefijo "Alt26"
    seguido de 4 pares letra-dígito (p. ej. Alt26D7T3H8V9, ~30 bits de entropía).
    Las aplica por la Admin API y guarda el mapeo en docs/credenciales_piloto.txt
    (gitignored) para reenviar a gerencia. Es idempotente: si una cédula ya tiene
    password no se sobrescribe a menos que se use --forzar-pw.
"""
import argparse
import os
import random
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


# ── Generación de contraseñas (por usuario) ───────────────────────────────────
# Prefijo común "Alt26" + 4 pares letra-dígito. El prefijo es igual para todos
# (no aporta entropía), los 8 caracteres aleatorios dan ~30 bits.
# Alfabeto sin caracteres ambiguos (sin I/O/L, sin 0/1) para que sea fácil de
# teclear y dictar por WhatsApp.

_PW_PREFIX = "Alt26"
_PW_LETTERS = "ABCDEFGHJKMNPQRSTUVWXYZ"
_PW_DIGITS = "23456789"
_CRED_FILE = Path(__file__).resolve().parent.parent / "docs" / "credenciales_piloto.txt"


def _generar_password(rng: random.Random) -> str:
    code = "".join(rng.choice(_PW_LETTERS) + rng.choice(_PW_DIGITS) for _ in range(4))
    return _PW_PREFIX + code


def _generar_passwords_unicas(usuarios: list[dict], rng: random.Random) -> dict[str, str]:
    """Cédula → password, todos únicos."""
    usados: set[str] = set()
    mapa: dict[str, str] = {}
    for u in usuarios:
        pw = _generar_password(rng)
        while pw in usados:
            pw = _generar_password(rng)
        usados.add(pw)
        mapa[u["cedula"]] = pw
    return mapa


def _escribir_credenciales(mapa: dict[str, str], usuarios: list[dict]) -> Path:
    """Escribe el mapeo Empleado → PW en docs/credenciales_piloto.txt (gitignored)."""
    _CRED_FILE.parent.mkdir(parents=True, exist_ok=True)
    lineas = [
        "CREDENCIALES PRUEBA PILOTO — ALTRANS",
        "=====================================",
        "Login: cédula | Contraseña: indicada abajo",
        "Contraseña inicial obligatoria a cambiar en producción.",
        "",
        f"  {'EMPLEADO':<30s} | {'ROL':<11s} | {'USUARIO (cédula)':<17s} | CONTRASEÑA",
        "  " + "-" * 94,
    ]
    por_cedula = {u["cedula"]: u for u in usuarios}
    for cedula, pw in mapa.items():
        u = por_cedula[cedula]
        lineas.append(
            f"  {u['nombre'][:30]:<30s} | {u['rol']:<11s} | {cedula:<17s} | {pw}"
        )
    _CRED_FILE.write_text("\n".join(lineas) + "\n", encoding="utf-8")
    return _CRED_FILE



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


def _update_password(user_id: str, password: str) -> None:
    r = requests.put(f"{ADMIN_API}/{user_id}", headers=HEADERS, json={"password": password}, timeout=15)
    if not r.ok:
        raise RuntimeError(f"Password {user_id}: {r.status_code} {r.text}")


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


def seed_passwords(usuarios: list[dict], dry_run: bool = False, forzar: bool = False) -> None:
    """Genera password único por usuario.

    Sin --forzar-pw: solo crea los usuarios que no existen (con su pw generado);
    los existentes quedan intactos. Con --forzar-pw: sobrescribe el password
    de todos y reescribe el archivo de credenciales.
    """
    existentes = _list_users()
    por_email = {_email(u["cedula"]): u for u in usuarios}
    rng = random.Random()

    mapa = _generar_passwords_unicas(usuarios, rng)
    print(f"Usuarios actuales en Supabase: {len(existentes)}\n")
    aplicados: list[dict] = []
    for email, u in por_email.items():
        existente = _find_by_email(email, existentes)
        pw = mapa[u["cedula"]]
        if existente and not forzar:
            print(f"  · {u['rol']:12s} {u['cedula']:12s}  {u['nombre']} — ya existe, sin cambios")
            continue
        if existente:
            if not dry_run:
                _update_password(existente["id"], pw)
            tag = "[DRY]" if dry_run else "✓"
            print(f"  {tag} {u['rol']:12s} {u['cedula']:12s}  {pw}  (sobrescrito)")
        else:
            if not dry_run:
                _create(u, pw)
            tag = "[DRY]" if dry_run else "✚"
            print(f"  {tag} {u['rol']:12s} {u['cedula']:12s}  {pw}  (creado)")
        aplicados.append(u)

    if not dry_run and aplicados:
        archivo = _escribir_credenciales(mapa, usuarios)
        print(f"\n✅ Passwords aplicados y guardados en {archivo} (gitignored)")
    elif dry_run:
        print("\n[DRY] No se aplicó nada.")
    else:
        print("\nNada que aplicar.")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Seed de usuarios de producción en Supabase Auth (login por cédula)")
    parser.add_argument("--solo",     metavar="CEDULA",
                        help="Crear/actualizar solo el usuario con esta cédula")
    parser.add_argument("--password", default=DEFAULT_PASSWORD,
                        help=f"Password inicial (default: {DEFAULT_PASSWORD})")
    parser.add_argument("--listar",   action="store_true",
                        help="Solo listar usuarios actuales y su rol")
    parser.add_argument("--generar-pw", action="store_true",
                        help="Genera password único por usuario y lo aplica (guarda en docs/)")
    parser.add_argument("--forzar-pw", action="store_true",
                        help="Con --generar-pw: sobrescribe passwords existentes (default: no)")
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

    if args.generar_pw:
        seed_passwords(target, dry_run=args.dry_run, forzar=args.forzar_pw)
        return 0

    seed(target, args.password, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
