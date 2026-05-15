.PHONY: help setup status db-reset etl load backup seed-users list-users \
        test-db test-agent test-agent-multi test-concurrency \
        serve ngrok probar-todo clean

PY   := python3
PORT ?= 8080

help:
	@echo "Comandos disponibles:"
	@echo ""
	@echo "  Setup"
	@echo "    setup              - Instala deps y valida .env"
	@echo "    status             - Conteos rápidos por tabla"
	@echo ""
	@echo "  Pipeline de datos  (ojo: destructivo)"
	@echo "    db-reset           - DROPea schema y reaplica schema_consolidated.sql"
	@echo "    etl                - Excel -> sheets -> cleaning -> informe_etl"
	@echo "    load               - Sube cleaned_data a Supabase"
	@echo "    backup             - Backup CSV de todas las tablas"
	@echo ""
	@echo "  Usuarios"
	@echo "    seed-users         - Crea/actualiza usuarios test (1 por rol)"
	@echo "    list-users         - Lista usuarios actuales + rol"
	@echo ""
	@echo "  Tests"
	@echo "    test-db            - pytest tests/ (audit, RPCs, seguridad)"
	@echo "    test-agent         - Suite del chatbot (DeepSeek)"
	@echo "    test-agent-multi   - A/B contra deepseek,groq,gemini,claude"
	@echo "    test-concurrency   - Solo el test de 10 conductores paralelos"
	@echo ""
	@echo "  Dev server"
	@echo "    serve              - uvicorn FastAPI puerto $$PORT (2 workers)"
	@echo "    ngrok              - Túnel ngrok al puerto $$PORT"
	@echo ""
	@echo "  Atajo del día"
	@echo "    probar-todo        - db-reset + etl  (revisar informe antes de load)"

# ── Setup ──────────────────────────────────────────────────────────────────────

setup:
	@test -f .env || (echo "ERROR: falta .env"; exit 1)
	@grep -q DATABASE_URL .env       || (echo "ERROR: DATABASE_URL no en .env"; exit 1)
	@grep -q SUPABASE_URL .env       || (echo "ERROR: SUPABASE_URL no en .env"; exit 1)
	@grep -q SUPABASE_SERVICE_KEY .env || (echo "ERROR: SUPABASE_SERVICE_KEY no en .env"; exit 1)
	pip install -r ai_agent/requirements.txt
	pip install pandas openpyxl sqlalchemy psycopg2-binary python-dotenv pytest requests
	@echo "✅ Setup OK"

status:
	@$(PY) -c "import os; from dotenv import load_dotenv; load_dotenv(); \
import psycopg2; c = psycopg2.connect(os.environ['DATABASE_URL']); cur = c.cursor(); \
print('Tabla'.ljust(25), 'Filas'.rjust(10)); print('-'*40); \
[(cur.execute(f'SELECT COUNT(*) FROM public.{t}'), print(t.ljust(25), str(cur.fetchone()[0]).rjust(10))) for t in ['manifiestos_flat','audit_log','chatbot_sesiones','processed_messages','jailbreak_log']]"

# ── Pipeline de datos ─────────────────────────────────────────────────────────

db-reset:
	@echo "⚠️  ESTO BORRA TODOS LOS DATOS DE LA DB. Ctrl+C en 3s para cancelar..."
	@sleep 3
	@psql "$$(grep '^DATABASE_URL=' .env | cut -d= -f2-)" -f supabase/schema_consolidated.sql
	@echo "✅ Schema re-aplicado"

etl:
	$(PY) -m etl_individual.exports
	$(PY) -m etl_individual.cleaning_individual
	$(PY) -m etl_individual.informes
	@echo ""
	@echo "✅ ETL listo."
	@echo "   👉 RÉVISALO: cleaned_data/informe_calidad/informe_etl.xlsx"
	@echo "   Si OK ejecuta: make load"

load:
	$(PY) -m etl_individual.load_flat
	@echo "✅ Datos cargados a Supabase"

backup:
	$(PY) -m etl_individual.backup_db

# ── Usuarios ─────────────────────────────────────────────────────────────────

seed-users:
	$(PY) -m etl_individual.seed_users

list-users:
	$(PY) -m etl_individual.seed_users --listar

# ── Tests ─────────────────────────────────────────────────────────────────────

test-db:
	$(PY) -m pytest tests/test_audit_log.py tests/test_read_rpcs.py -v
	$(PY) tests/test_seguridad_integridad.py

test-agent:
	cd ai_agent && $(PY) scripts/test_agent.py

test-agent-multi:
	cd ai_agent && $(PY) scripts/test_agent.py --modelos deepseek,groq,gemini,claude

test-concurrency:
	cd ai_agent && $(PY) scripts/test_agent.py --concurrencia --categoria consultas

# ── Dev server ────────────────────────────────────────────────────────────────

serve:
	cd ai_agent && uvicorn main:app --host 0.0.0.0 --port $(PORT) --workers 2 --reload

ngrok:
	ngrok http $(PORT)

# ── Atajo del día ─────────────────────────────────────────────────────────────

probar-todo: db-reset etl
	@echo ""
	@echo "👉 Revisa cleaned_data/informe_calidad/informe_etl.xlsx"
	@echo "    Si OK:   make load && make seed-users && make test-db && make test-agent"

clean:
	@find . -type d \( -name __pycache__ -o -name .pytest_cache \) -exec rm -rf {} + 2>/dev/null || true
	@echo "✅ Caches limpios"
