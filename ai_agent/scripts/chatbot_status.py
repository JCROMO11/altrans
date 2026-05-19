"""Salud del chatbot — queries de monitoreo rápido.

Uso: python3 ai_agent/scripts/chatbot_status.py
"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
import psycopg2

_ROOT = Path(__file__).resolve().parent.parent.parent  # development/
load_dotenv(_ROOT / ".env", override=False)

DB_URL = os.environ.get("DATABASE_URL")
if not DB_URL:
    sys.exit("ERROR: DATABASE_URL no definido en .env")

conn = psycopg2.connect(DB_URL)
cur = conn.cursor()

print("=== Sesiones ===")
cur.execute("""
    SELECT
        COUNT(*) FILTER (WHERE estado = 'activa') AS activas,
        COUNT(*) FILTER (WHERE estado = 'activa' AND last_activity > now() - interval '1h') AS ultima_hora,
        COUNT(*) FILTER (WHERE locked_until > now()) AS bloqueadas,
        COUNT(*) FILTER (WHERE msg_count >= 4) AS limite_alcanzado,
        COUNT(*) AS total
    FROM chatbot_sesiones
""")
r = cur.fetchone()
print(f"  Activas:               {r[0]:>5}")
print(f"  Activas última hora:   {r[1]:>5}")
print(f"  Bloqueadas (strikes):  {r[2]:>5}")
print(f"  Límite alcanzado:      {r[3]:>5}")
print(f"  Total sesiones:        {r[4]:>5}")
print()

print("=== Jailbreaks (últimas 24h) ===")
cur.execute("""
    SELECT motivo, COUNT(*) AS n FROM jailbreak_log
    WHERE detectado_en > now() - interval '24h'
    GROUP BY 1 ORDER BY n DESC
""")
jbs = cur.fetchall()
if jbs:
    for motivo, n in jbs:
        print(f"  {motivo:7s} {n:>4}")
else:
    print("  (ninguno)")
print()

print("=== Conductores activos hoy ===")
cur.execute("""
    SELECT conductor_nombre, msg_count,
           EXTRACT(epoch FROM now() - last_activity)::int / 60 AS mins_ultimo
    FROM chatbot_sesiones
    WHERE estado = 'activa' AND last_activity > now() - interval '24h'
    ORDER BY last_activity DESC LIMIT 10
""")
activos = cur.fetchall()
if activos:
    for nombre, msgs, mins in activos:
        print(f"  {(nombre or '?')[:28]:<30s} {msgs:>2} msgs   hace {mins:>4} min")
else:
    print("  (ninguno)")
print()

print("=== Procesados (últimas 24h) ===")
cur.execute(
    "SELECT COUNT(*) FROM processed_messages "
    "WHERE processed_at > now() - interval '24h'"
)
print(f"  Mensajes: {cur.fetchone()[0]:,}")

conn.close()
