"""
Suite de seguridad e integridad para la DB de Altrans.

Cubre:
  1. Enmascaramiento de valor_factura por rol
  2. RLS sobre tablas privadas (chatbot_sesiones, processed_messages, jailbreak_log)
  3. Permisos de RPCs por rol (guardar_digitador / logistico / tesoreria / financiero / borrar)
  4. Upsert idempotente de guardar_digitador
  5. CASCADE en audit_log
  6. Fórmula flete_neto_conductor (NO incluye consignacion_a_terceros)
  7. ANULADO oculto a conductores en RPCs del chatbot

Manifiestos de prueba reservados: 999000-999999.
Ejecutar: python tests/test_seguridad_integridad.py
"""
import os, sys, json, psycopg2, requests
import datetime as _dt
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)

# ── Credenciales ──────────────────────────────────────────────────────────────
DB_URL      = os.environ["DATABASE_URL"]
SUPA_URL    = os.environ["SUPABASE_URL"]
ANON_KEY    = os.environ["SUPABASE_ANON_KEY"]
SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

PASS = "\033[92m✓\033[0m"
FAIL = "\033[91m✗\033[0m"
SKIP = "\033[93m·\033[0m"
results = []  # (categoria, nombre, ok|None)

def ok(cat, name):
    print(f"  {PASS} {name}")
    results.append((cat, name, True))

def fail(cat, name, detail=""):
    print(f"  {FAIL} {name}" + (f" — {detail}" if detail else ""))
    results.append((cat, name, False))

def skip(cat, name, detail=""):
    print(f"  {SKIP} {name}" + (f" — {detail}" if detail else ""))
    results.append((cat, name, None))

# ── Helpers SQL ───────────────────────────────────────────────────────────────
_TEST_CLAIMS = json.dumps({
    "sub": "test-user",
    "role": "authenticated",
    "email": "test@altrans.local",
    "app_metadata": {"role": "gerencia"},
})

def _role_claims(role):
    return json.dumps({
        "sub": "test-user",
        "role": "authenticated",
        "email": "test@altrans.local",
        "app_metadata": {"role": role},
    })

def sql_as_role(query, role, params=None, commit=False):
    """Ejecuta SQL simulando app_metadata.role. commit=True para asserts sobre estado persistido."""
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    try:
        cur = conn.cursor()
        cur.execute("SET LOCAL request.jwt.claims = %s", (_role_claims(role),))
        cur.execute("SET LOCAL ROLE authenticated")
        cur.execute(query, params)
        try:
            rows = cur.fetchall()
        except psycopg2.ProgrammingError:
            rows = []
        if commit:
            conn.commit()
        else:
            conn.rollback()
        return rows, None
    except Exception as e:
        conn.rollback()
        return None, str(e)
    finally:
        conn.close()

def sql_direct(query, params=None, fetch=True):
    """SQL como service_role/superuser. Setea claims dummy para que el trigger de audit no rompa."""
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    try:
        cur = conn.cursor()
        cur.execute("SET LOCAL request.jwt.claims = %s", (_TEST_CLAIMS,))
        cur.execute(query, params)
        if fetch:
            try:
                rows = cur.fetchall()
            except psycopg2.ProgrammingError:
                rows = []
        else:
            rows = []
        conn.commit()
        return rows
    finally:
        conn.close()

def svc_headers():
    return {"apikey": SERVICE_KEY, "Authorization": f"Bearer {SERVICE_KEY}",
            "Content-Type": "application/json"}
def anon_headers():
    return {"apikey": ANON_KEY, "Authorization": f"Bearer {ANON_KEY}",
            "Content-Type": "application/json"}

# Manifiestos reservados para tests
TEMP_NUM_BASE = 999100   # base — usamos 999100..999199
TEMP_NUM_CASCADE = 999900

def cleanup_temp():
    """Borra todo lo del rango de prueba."""
    sql_direct("DELETE FROM audit_log WHERE manifiesto BETWEEN 999000 AND 999999", fetch=False)
    sql_direct("DELETE FROM manifiestos_flat WHERE manifiesto BETWEEN 999000 AND 999999", fetch=False)


# ═══════════════════════════════════════════════════════════════════════════════
print("\n── 1. Enmascaramiento de valor_factura por rol ───────────────────────")

rows = sql_direct(
    "SELECT manifiesto, valor_factura FROM manifiestos_flat WHERE valor_factura IS NOT NULL LIMIT 1"
)
if not rows:
    skip("mask", "valor_factura masking", "no hay manifiestos con valor_factura — insertar uno")
else:
    manifiesto_test, valor_real = rows[0]

    rows_op, err = sql_as_role(
        f"SELECT valor_factura FROM v_manifiestos WHERE manifiesto = {manifiesto_test}",
        "logistico",
    )
    if rows_op and rows_op[0][0] is None:
        ok("mask", "logistico ve valor_factura = NULL")
    else:
        fail("mask", "logistico ve valor_factura = NULL", f"obtuvo: {rows_op} err={err}")

    rows_fin, err = sql_as_role(
        f"SELECT valor_factura FROM v_manifiestos WHERE manifiesto = {manifiesto_test}",
        "financiero",
    )
    if rows_fin and rows_fin[0][0] is not None:
        ok("mask", "financiero ve valor_factura real")
    else:
        fail("mask", "financiero ve valor_factura real", f"obtuvo: {rows_fin} err={err}")

    rows_adm, err = sql_as_role(
        f"SELECT valor_factura FROM v_manifiestos WHERE manifiesto = {manifiesto_test}",
        "gerencia",
    )
    if rows_adm and rows_adm[0][0] is not None:
        ok("mask", "gerencia ve valor_factura real")
    else:
        fail("mask", "gerencia ve valor_factura real", f"obtuvo: {rows_adm} err={err}")


# ═══════════════════════════════════════════════════════════════════════════════
print("\n── 2. RLS: tablas privadas bloqueadas para anon ──────────────────────")

for tabla in ["chatbot_sesiones", "processed_messages", "jailbreak_log"]:
    r = requests.get(f"{SUPA_URL}/rest/v1/{tabla}?limit=1", headers=anon_headers(), timeout=10)
    try: body = r.json()
    except Exception: body = r.text
    tiene_datos = isinstance(body, list) and len(body) > 0
    if not tiene_datos:
        ok("rls", f"{tabla} sin datos para anon (status {r.status_code})")
    else:
        fail("rls", f"{tabla} bloqueada para anon",
             f"devolvió {len(body)} filas: {str(body)[:120]}")


# ═══════════════════════════════════════════════════════════════════════════════
print("\n── 3. Permisos de RPCs por rol ───────────────────────────────────────")

# Casos: (rol, rpc, args, debe_pasar)
# Limpiamos antes para no contaminar
cleanup_temp()

# Prerequisito: insertar un manifiesto base para los updates
sql_direct("""
    INSERT INTO manifiestos_flat (
        manifiesto, archivo_origen, mes, año, periodo, semana,
        consecutivo_semanal, fecha_despacho, origen, departamento_origen,
        destino, departamento_destino, cliente, remesas,
        placa, tipo_vehiculo, conductor, cedula_conductor
    ) VALUES (
        999100,'TEST.xlsx','MAYO',2026,'2026-05-01','S20',1,
        '2026-05-01','BOGOTA','CUNDINAMARCA','CALI','VALLE DEL CAUCA',
        'CLIENTE TEST','REM-TEST','ABC123','SENCILLO','CONDUCTOR TEST','12345678'
    )
""", fetch=False)

CASOS_RPC = [
    # guardar_digitador: solo digitador/gerencia
    ("digitador", "guardar_digitador",
        "SELECT public.guardar_digitador(p_manifiesto := 999100, p_celular := '3001111111')",
        True),
    ("logistico", "guardar_digitador",
        "SELECT public.guardar_digitador(p_manifiesto := 999100)",
        False),
    ("tesoreria", "guardar_digitador",
        "SELECT public.guardar_digitador(p_manifiesto := 999100)",
        False),
    ("financiero", "guardar_digitador",
        "SELECT public.guardar_digitador(p_manifiesto := 999100)",
        False),

    # guardar_logistico: logistico/digitador/tesoreria/financiero/gerencia (todos tienen CUMPLE)
    ("logistico", "guardar_logistico",
        "SELECT public.guardar_logistico(p_manifiesto := 999100, p_estado_interno := 'CUMPLIDO')",
        True),
    ("digitador", "guardar_logistico",
        "SELECT public.guardar_logistico(p_manifiesto := 999100, p_estado_interno := 'CUMPLIDO')",
        True),
    ("tesoreria", "guardar_logistico",
        "SELECT public.guardar_logistico(p_manifiesto := 999100, p_estado_interno := 'CUMPLIDO')",
        True),
    ("financiero", "guardar_logistico",
        "SELECT public.guardar_logistico(p_manifiesto := 999100, p_estado_interno := 'CUMPLIDO')",
        True),

    # guardar_tesoreria: solo tesoreria/gerencia
    ("tesoreria", "guardar_tesoreria",
        "SELECT public.guardar_tesoreria(p_manifiesto := 999100, p_valor_pagado := 100000)",
        True),
    ("logistico", "guardar_tesoreria",
        "SELECT public.guardar_tesoreria(p_manifiesto := 999100)",
        False),
    ("financiero", "guardar_tesoreria",
        "SELECT public.guardar_tesoreria(p_manifiesto := 999100)",
        False),

    # guardar_financiero: solo financiero/gerencia
    ("financiero", "guardar_financiero",
        "SELECT public.guardar_financiero(p_manifiesto := 999100, p_factura_no := 'F-001')",
        True),
    ("logistico", "guardar_financiero",
        "SELECT public.guardar_financiero(p_manifiesto := 999100)",
        False),
    ("digitador", "guardar_financiero",
        "SELECT public.guardar_financiero(p_manifiesto := 999100)",
        False),

    # borrar_manifiesto: solo gerencia (sobre un manifiesto inexistente, no importa el efecto)
    ("gerencia", "borrar_manifiesto",
        "SELECT public.borrar_manifiesto(999199)",
        True),
    ("digitador", "borrar_manifiesto",
        "SELECT public.borrar_manifiesto(999199)",
        False),
    ("financiero", "borrar_manifiesto",
        "SELECT public.borrar_manifiesto(999199)",
        False),
    ("tesoreria", "borrar_manifiesto",
        "SELECT public.borrar_manifiesto(999199)",
        False),
]

for rol, rpc, query, debe_pasar in CASOS_RPC:
    _, err = sql_as_role(query, rol)  # rollback — solo testeamos el permiso, no el efecto
    paso = err is None
    if paso == debe_pasar:
        verbo = "permite" if debe_pasar else "rechaza"
        ok("rpc-perm", f"{rol} → {rpc}: {verbo} ({'OK' if debe_pasar else 'sin permiso'})")
    else:
        fail("rpc-perm", f"{rol} → {rpc}: esperado {'pasa' if debe_pasar else 'falla'}",
             f"err={err}")


# ═══════════════════════════════════════════════════════════════════════════════
print("\n── 4. Upsert idempotente de guardar_digitador ─────────────────────────")

count_antes = sql_direct("SELECT COUNT(*) FROM manifiestos_flat")[0][0]

# Llamar dos veces con el mismo manifiesto — no debe duplicar (commit=True para verificar persistencia)
_, err1 = sql_as_role(
    "SELECT public.guardar_digitador(p_manifiesto := 999100, p_celular := '3001234567')",
    "gerencia", commit=True,
)
_, err2 = sql_as_role(
    "SELECT public.guardar_digitador(p_manifiesto := 999100, p_celular := '3009999999')",
    "gerencia", commit=True,
)
count_despues = sql_direct("SELECT COUNT(*) FROM manifiestos_flat")[0][0]
celular_final = sql_direct("SELECT celular FROM manifiestos_flat WHERE manifiesto = 999100")[0][0]

if err1 is None and err2 is None and count_antes == count_despues:
    ok("upsert", f"guardar_digitador no duplica (total filas estable: {count_despues})")
else:
    fail("upsert", "guardar_digitador no duplica",
         f"antes={count_antes} después={count_despues} err1={err1} err2={err2}")

if celular_final == '3009999999':
    ok("upsert", "guardar_digitador actualiza con el último valor")
else:
    fail("upsert", "guardar_digitador actualiza", f"celular final={celular_final}")


# ═══════════════════════════════════════════════════════════════════════════════
print("\n── 5. CASCADE en audit_log al borrar manifiesto ──────────────────────")

# Insertar manifiesto temporal + audit_log
sql_direct("""
    INSERT INTO manifiestos_flat (
        manifiesto, archivo_origen, mes, año, periodo, semana,
        consecutivo_semanal, fecha_despacho, origen, departamento_origen,
        destino, departamento_destino, cliente, remesas,
        placa, tipo_vehiculo, conductor, cedula_conductor
    ) VALUES (
        999900,'TEST_CASCADE.xlsx','MAYO',2026,'2026-05-01','S20',1,
        '2026-05-01','BOGOTA','CUNDINAMARCA','CALI','VALLE DEL CAUCA',
        'CLIENTE CASCADE','REM-CASCADE','XYZ999','SENCILLO','CONDUCTOR CASCADE','99999999'
    ) ON CONFLICT (manifiesto) DO NOTHING
""", fetch=False)
sql_direct("""
    INSERT INTO audit_log (manifiesto, campo, valor_anterior, valor_nuevo, usuario)
    VALUES (999900, 'novedades', NULL, 'test cascade', 'test@test.com')
""", fetch=False)

audit_antes = sql_direct("SELECT COUNT(*) FROM audit_log WHERE manifiesto = 999900")[0][0]
sql_direct("DELETE FROM manifiestos_flat WHERE manifiesto = 999900", fetch=False)
audit_despues = sql_direct("SELECT COUNT(*) FROM audit_log WHERE manifiesto = 999900")[0][0]

if audit_antes > 0 and audit_despues == 0:
    ok("cascade", f"audit_log eliminado al borrar manifiesto ({audit_antes} fila)")
else:
    fail("cascade", "audit_log eliminado en CASCADE",
         f"antes={audit_antes} después={audit_despues}")


# ═══════════════════════════════════════════════════════════════════════════════
print("\n── 6. Fórmula flete_neto_conductor (sin consignacion) ─────────────────")

# Insertar manifiesto con todos los componentes y verificar el GENERATED
sql_direct("""
    UPDATE manifiestos_flat SET
        flete_conductor       = 500000,
        ajuste_positivo_flete = 50000,
        ajuste_negativo_flete = 20000,
        consignacion_a_terceros = 100000
    WHERE manifiesto = 999100
""", fetch=False)

neto = sql_direct("SELECT flete_neto_conductor FROM manifiestos_flat WHERE manifiesto = 999100")[0][0]
esperado = 500000 + 50000 - 20000  # SIN restar consignacion
if neto is not None and float(neto) == float(esperado):
    ok("formula", f"flete_neto = 500k + 50k - 20k = {neto} (consignacion NO se resta)")
else:
    fail("formula", "flete_neto sin consignacion",
         f"obtuvo {neto}, esperado {esperado}. ¿Migración aplicada?")

# Caso flete_conductor NULL → neto NULL
sql_direct("""
    UPDATE manifiestos_flat SET flete_conductor = NULL
    WHERE manifiesto = 999100
""", fetch=False)
neto_null = sql_direct("SELECT flete_neto_conductor FROM manifiestos_flat WHERE manifiesto = 999100")[0][0]
if neto_null is None:
    ok("formula", "flete_conductor NULL → flete_neto NULL")
else:
    fail("formula", "flete_conductor NULL → flete_neto NULL", f"obtuvo {neto_null}")


# ═══════════════════════════════════════════════════════════════════════════════
print("\n── 7. ANULADO oculto en consultas (vía PostgREST) ─────────────────────")

# Crear un manifiesto anulado con cedula reservada
sql_direct("""
    INSERT INTO manifiestos_flat (
        manifiesto, archivo_origen, mes, año, periodo, semana,
        consecutivo_semanal, fecha_despacho, origen, departamento_origen,
        destino, departamento_destino, cliente, remesas,
        placa, tipo_vehiculo, conductor, cedula_conductor, estado_interno
    ) VALUES (
        999500,'TEST_ANULADO.xlsx','MAYO',2026,'2026-05-01','S20',1,
        '2026-05-01','BOGOTA','CUNDINAMARCA','CALI','VALLE DEL CAUCA',
        'CLIENTE','REM','PLA999','SENCILLO','CONDUCTOR ANULADO','99999998','ANULADO'
    ) ON CONFLICT (manifiesto) DO UPDATE SET estado_interno = 'ANULADO'
""", fetch=False)

# La cláusula que usa el chatbot
filtro = "(estado_interno.neq.ANULADO,estado_interno.is.null)"
r = requests.get(
    f"{SUPA_URL}/rest/v1/manifiestos_flat?manifiesto=eq.999500&or={filtro}&select=manifiesto",
    headers=svc_headers(), timeout=10,
)
if r.status_code == 200 and r.json() == []:
    ok("anulado", "filtro 'or=(estado_interno.neq.ANULADO,...)' oculta el anulado")
else:
    fail("anulado", "filtro de ANULADO oculta", f"status={r.status_code} body={r.text[:200]}")

# Sin filtro sí lo trae (sanity)
r2 = requests.get(
    f"{SUPA_URL}/rest/v1/manifiestos_flat?manifiesto=eq.999500&select=manifiesto,estado_interno",
    headers=svc_headers(), timeout=10,
)
if r2.status_code == 200 and len(r2.json()) == 1 and r2.json()[0]["estado_interno"] == "ANULADO":
    ok("anulado", "sin filtro sí aparece (sanity check)")
else:
    fail("anulado", "sanity sin filtro", f"status={r2.status_code} body={r2.text[:200]}")


# ═══════════════════════════════════════════════════════════════════════════════
print("\n── 8. Facturación: dias_para_facturar y guardar_financiero ───────────")

# Insertar manifiesto base con fecha_despacho conocida
MANIF_FACT = 999110
sql_direct("DELETE FROM manifiestos_flat WHERE manifiesto = %s", (MANIF_FACT,), fetch=False)
sql_direct("""
    INSERT INTO manifiestos_flat (
        manifiesto, archivo_origen, mes, año, periodo, semana,
        consecutivo_semanal, fecha_despacho, origen, departamento_origen,
        destino, departamento_destino, cliente, remesas,
        placa, tipo_vehiculo, conductor, cedula_conductor
    ) VALUES (
        %s,'TEST.xlsx','MAYO',2026,'2026-05-01','S20',1,
        '2026-05-01','BOGOTA','CUNDINAMARCA','CALI','VALLE DEL CAUCA',
        'CLIENTE FACT','REM-FACT','ABC123','SENCILLO','CONDUCTOR FACT','12345678'
    )
""", (MANIF_FACT,), fetch=False)

# Guardar facturación completa
_, err = sql_as_role(
    "SELECT public.guardar_financiero(p_manifiesto := 999110, "
    "p_factura_no := 'F-TEST-001', p_fecha_factura := '2026-05-15'::date, "
    "p_valor_factura := 1500000)",
    "gerencia", commit=True,
)
if err is None:
    ok("facturacion", "guardar_financiero acepta los 3 campos completos")
else:
    fail("facturacion", "guardar_financiero con 3 campos", f"err={err}")

# Verificar dias_para_facturar = fecha_factura - fecha_despacho = 14
row = sql_direct(
    "SELECT dias_para_facturar FROM manifiestos_flat WHERE manifiesto = %s",
    (MANIF_FACT,),
)
dias = row[0][0] if row else None
if dias == 14:
    ok("facturacion", f"dias_para_facturar se calcula correctamente (esperado=14, actual={dias})")
else:
    fail("facturacion", "dias_para_facturar", f"esperado=14 actual={dias}")

# fecha_factura NULL → dias_para_facturar debe ser NULL
sql_direct(
    "UPDATE manifiestos_flat SET fecha_factura = NULL WHERE manifiesto = %s",
    (MANIF_FACT,), fetch=False,
)
row = sql_direct(
    "SELECT dias_para_facturar FROM manifiestos_flat WHERE manifiesto = %s",
    (MANIF_FACT,),
)
if row and row[0][0] is None:
    ok("facturacion", "dias_para_facturar es NULL cuando fecha_factura es NULL")
else:
    fail("facturacion", "dias_para_facturar con fecha_factura NULL",
         f"esperado=None actual={row[0][0] if row else None}")

sql_direct("DELETE FROM manifiestos_flat WHERE manifiesto = %s", (MANIF_FACT,), fetch=False)


# ═══════════════════════════════════════════════════════════════════════════════
# Limpieza final
cleanup_temp()


# ═══════════════════════════════════════════════════════════════════════════════
# Resumen
total    = len(results)
passed   = sum(1 for *_ , v in results if v is True)
failed   = sum(1 for *_ , v in results if v is False)
skipped  = sum(1 for *_ , v in results if v is None)

print(f"\n{'═'*60}")
print(f"  Resultado: {passed}/{total - skipped} pruebas pasaron · {skipped} saltadas")
print(f"  Por categoría:")
cats = {}
for cat, _, v in results:
    cats.setdefault(cat, [0, 0])
    if v is True: cats[cat][0] += 1
    if v is False: cats[cat][1] += 1
for cat, (p, f_) in cats.items():
    print(f"    {cat:12} {p} pass · {f_} fail")
if failed == 0:
    print("  \033[92mTodo OK\033[0m")
else:
    print("  \033[91mHay fallos — revisar arriba\033[0m")
print(f"{'═'*60}\n")

# Reporte markdown
out_dir = os.path.join(os.path.dirname(__file__), "reportes")
os.makedirs(out_dir, exist_ok=True)
ts = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
out_path = os.path.join(out_dir, f"reporte_db_{ts}.md")
with open(out_path, "w") as f:
    f.write(f"# Reporte de seguridad/integridad DB\n\n")
    f.write(f"**Fecha:** {_dt.datetime.now().isoformat(timespec='seconds')}\n\n")
    f.write(f"**Resultado:** {passed}/{total - skipped} ({skipped} saltadas)\n\n")
    f.write("| Categoría | Prueba | Resultado |\n|---|---|---|\n")
    for cat, name, v in results:
        mark = "✅" if v is True else ("❌" if v is False else "·")
        f.write(f"| {cat} | {name} | {mark} |\n")
print(f"Reporte: {out_path}")

sys.exit(0 if failed == 0 else 1)
