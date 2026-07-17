.PHONY: help setup status count dedup-check verify-load verify-schema \
        db-reset etl load backup verify-backup verify-backup-email seed-users seed-users-dry list-users \
        chatbot-status \
        test-db test-etl test-webhook test-dashboard test-agent \
        test-agent-propietario test-agent-multi test-concurrency test-moderacion test-all \
        test-excel-pre test-excel-post test-excel-generar \
        serve ngrok probar-todo clean install-dashboard-deps \
        deploy-chatbot deploy-notifications deploy-all \
        demo demo-stop

SHELL   := /bin/bash
PY      := python3
PORT    ?= 8080

help:
	@echo "Comandos disponibles:"
	@echo ""
	@echo "  Setup"
	@echo "    setup              - Instala deps y valida .env"
	@echo "    status             - Conteos rápidos por tabla"
	@echo "    install-dashboard-deps - npm install en dashboard/"
	@echo ""
	@echo "  Pipeline de datos  (ojo: destructivo)"
	@echo "    db-reset           - DROPea schema y reaplica schema_consolidated.sql"
	@echo "    etl                - Excel -> sheets -> cleaning -> informe_etl"
	@echo "    load               - Sube cleaned_data a Supabase"
	@echo "    count              - SELECT COUNT(*) de manifiestos_flat"
	@echo "    dedup-check        - Detecta manifiestos duplicados en DB"
	@echo "    verify-load        - DB vs CSV cleaned + dedup-check (validar tras load)"
	@echo "    verify-schema      - Verifica tablas + RPCs (usar tras db-reset)"
	@echo "    backup             - Backup CSV de todas las tablas"
	@echo "    verify-backup      - Verifica integridad del último backup local vs DB (conteo + PKs + sumas)"
	@echo "    verify-backup-email - Genera el backup de producción (Supabase REST) y verifica conteos vs DB (no envía email)"
	@echo ""
	@echo "  Usuarios"
	@echo "    seed-users         - Crea/actualiza usuarios de producción (login por cédula)"
	@echo "    seed-users-dry     - Previsualiza seed sin ejecutar"
	@echo "    list-users         - Lista usuarios actuales + rol"
	@echo ""
	@echo "  Tests"
	@echo "    test-db            - Seguridad, integridad, audit_log, RPCs"
	@echo "    test-etl           - Funciones de limpieza del ETL"
	@echo "    test-webhook       - Webhook WhatsApp (auth, jailbreak, límites, HMAC)"
	@echo "    test-dashboard     - vitest (componentes y hooks del dashboard)"
	@echo "    test-agent         - Suite completa del chatbot (conductor+admin+propietario+concurrencia)"
	@echo "    test-agent-propietario - Solo casos de propietario (placa)"
	@echo "    test-agent-multi   - A/B contra deepseek,gemini,claude"
	@echo "    test-concurrency   - Suite completa + 30 conductores paralelos (--n-concurrencia N para cambiar)"
	@echo "    test-moderacion    - Test de la capa de seguridad (prompt-guard-2): recall/precision"
	@echo "    test-all           - TODOS los tests (db + etl + webhook + agent)"
	@echo ""
	@echo "  Excel upload (2.6)"
	@echo "    test-excel-generar - Genera prueba_invalidas.xlsx y prueba_vacio.xlsx en tests/reportes/"
	@echo "    test-excel-pre     - Compara CSV cleaned vs Lista_Manifiestos (qué va a cambiar)"
	@echo "    test-excel-post    - Compara DB actual vs Lista_Manifiestos (verifica que se aplicaron)"
	@echo ""
	@echo "  Monitoreo"
	@echo "    chatbot-status     - Salud del chatbot (sesiones, jailbreaks, activos)"
	@echo ""
	@echo "  Dev server"
	@echo "    serve              - uvicorn FastAPI puerto $$PORT (2 workers)"
	@echo "    ngrok              - Túnel ngrok al puerto $$PORT"
	@echo ""
	@echo "  Demo"
	@echo "    demo WA_TOKEN=<tok>  - Actualiza token, levanta uvicorn + ngrok"
	@echo "    demo-stop            - Mata uvicorn + ngrok"
	@echo ""
	@echo "  Deploy Railway"
	@echo "    deploy-chatbot       - Deploy manual del chatbot a Railway"
	@echo "    deploy-notifications - Deploy manual del servicio de notificaciones"
	@echo "    deploy-all           - Deploy manual de ambos servicios"
	@echo ""
	@echo "  Atajo del día"
	@echo "    probar-todo        - db-reset + etl (revisar informe antes de make load)"

# ── Setup ──────────────────────────────────────────────────────────────────────

setup:
	@test -f .env || (echo "ERROR: falta .env"; exit 1)
	@grep -q DATABASE_URL .env       || (echo "ERROR: DATABASE_URL no en .env"; exit 1)
	@grep -q SUPABASE_URL .env       || (echo "ERROR: SUPABASE_URL no en .env"; exit 1)
	@grep -q SUPABASE_SERVICE_KEY .env || (echo "ERROR: SUPABASE_SERVICE_KEY no en .env"; exit 1)
	pip install -r ai_agent/requirements.txt
	pip install pandas openpyxl sqlalchemy psycopg2-binary python-dotenv pytest requests
	@echo "✅ Setup OK"

install-dashboard-deps:
	cd dashboard && npm install
	@echo "✅ Dashboard deps instaladas"

status:
	@$(PY) -c "import os; from dotenv import load_dotenv; load_dotenv(); \
import psycopg2; c = psycopg2.connect(os.environ['DATABASE_URL']); cur = c.cursor(); \
print('Tabla'.ljust(25), 'Filas'.rjust(10)); print('-'*40); \
[(cur.execute(f'SELECT COUNT(*) FROM public.{t}'), print(t.ljust(25), str(cur.fetchone()[0]).rjust(10))) for t in ['manifiestos_flat','audit_log','chatbot_sesiones','processed_messages','jailbreak_log']]"

# ── Pipeline de datos ─────────────────────────────────────────────────────────

db-reset:
	@test -f .env || (echo "ERROR: falta .env en la raíz del proyecto"; exit 1)
	@grep -q '^DATABASE_URL=' .env || (echo "ERROR: DATABASE_URL no está definido en .env"; exit 1)
	@echo "⚠️  ESTO BORRA TODOS LOS DATOS Y RECREA EL SCHEMA. Ctrl+C en 3s para cancelar..."
	@sleep 3
	@psql "$$(grep '^DATABASE_URL=' .env | cut -d= -f2-)" \
	    -c "TRUNCATE public.manifiestos_flat, public.audit_log, public.chatbot_sesiones, public.processed_messages, public.jailbreak_log RESTART IDENTITY CASCADE;" \
	    -f supabase/schema_consolidated.sql
	@echo "✅ Datos borrados y schema re-aplicado"

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

count:
	@$(PY) -c "import os; from dotenv import load_dotenv; load_dotenv(); \
import psycopg2; c = psycopg2.connect(os.environ['DATABASE_URL']); cur = c.cursor(); \
cur.execute('SELECT COUNT(*) FROM public.manifiestos_flat'); \
print(f'manifiestos_flat: {cur.fetchone()[0]:,} filas')"

dedup-check:
	@$(PY) -c "import os; from dotenv import load_dotenv; load_dotenv(); \
import psycopg2; c = psycopg2.connect(os.environ['DATABASE_URL']); cur = c.cursor(); \
cur.execute('SELECT manifiesto, COUNT(*) AS n FROM public.manifiestos_flat GROUP BY 1 HAVING COUNT(*) > 1 ORDER BY n DESC'); \
dups = cur.fetchall(); \
[print(f'  ⚠️  {m}: {n} ocurrencias') for m, n in dups] if dups else print('✅ Sin duplicados')"

verify-load:
	$(PY) -m etl_individual.verify_load

verify-schema:
	$(PY) -m etl_individual.verify_schema

backup:
	$(PY) -m etl_individual.backup_db

verify-backup:
	$(PY) -m etl_individual.verify_backup

verify-backup-email:
	cd notifications && $(PY) test_backup_consistency.py

# ── Usuarios ─────────────────────────────────────────────────────────────────

seed-users:
	$(PY) -m etl_individual.seed_users

seed-users-dry:
	$(PY) -m etl_individual.seed_users --dry-run

list-users:
	$(PY) -m etl_individual.seed_users --listar

# ── Monitoreo ─────────────────────────────────────────────────────────────────

chatbot-status:
	$(PY) ai_agent/scripts/chatbot_status.py

# ── Tests ─────────────────────────────────────────────────────────────────────

test-db:
	$(PY) -m pytest tests/test_audit_log.py tests/test_read_rpcs.py -v
	$(PY) tests/test_seguridad_integridad.py

test-etl:
	$(PY) -m pytest tests/test_cleaning_etl.py -v

test-webhook:
	$(PY) -m pytest tests/test_webhook.py -v

test-dashboard:
	@test -d dashboard/node_modules || (echo "⚠️  node_modules no encontrado. Ejecuta: make install-dashboard-deps" && exit 1)
	cd dashboard && npx vitest run

test-agent:
	cd ai_agent && $(PY) scripts/test_agent.py --tipo todos --concurrencia

test-agent-propietario:
	cd ai_agent && $(PY) scripts/test_agent.py --tipo propietario

test-agent-multi:
	cd ai_agent && $(PY) scripts/test_agent.py --modelos deepseek,gemini,claude

test-concurrency:
	cd ai_agent && $(PY) scripts/test_agent.py --tipo todos --concurrencia

test-moderacion:
	cd ai_agent && $(PY) scripts/test_agent.py --moderacion

test-all: test-db test-etl test-webhook test-agent
	@echo ""
	@echo "✅ Suite completa (db + etl + webhook + agent)."
	@echo "   Dashboard tests: make test-dashboard  (requiere Node)"

# ── Excel upload (numeral 2.6) ────────────────────────────────────────────

test-excel-generar:
	$(PY) tests/generar_excel_prueba.py --invalidas --vacio
	@echo ""
	@echo "✅ Excels de prueba generados en data/data_test/"
	@echo "   prueba_invalidas.xlsx  — 6 válidas + 4 inválidas  → subir al dashboard"
	@echo "   prueba_vacio.xlsx      — sin datos                 → debe mostrar error"

test-excel-pre:
	$(PY) tests/generar_excel_comparacion.py \
	    --csv cleaned_data/individual_cleaned.csv \
	    --excel data/Lista_Manifiestos_08_05_2026.xlsx \
	    --output tests/reportes/comparacion_pre_upload.xlsx
	@echo ""
	@echo "✅ Reporte PRE-upload: tests/reportes/comparacion_pre_upload.xlsx"
	@echo "   → Muestra qué campos CAMBIARÁN al importar el Excel"

test-excel-post:
	$(PY) tests/generar_excel_comparacion.py \
	    --desde-db \
	    --excel data/Lista_Manifiestos_08_05_2026.xlsx \
	    --output tests/reportes/comparacion_post_upload.xlsx
	@echo ""
	@echo "✅ Reporte POST-upload: tests/reportes/comparacion_post_upload.xlsx"
	@echo "   → 'Sin Cambios' = cambios aplicados correctamente"
	@echo "   → 'Con Cambios' = manifiestos que quedaron distintos (deseleccionados o errores)"

# ── Dev server ────────────────────────────────────────────────────────────────

serve:
	cd ai_agent && uvicorn main:app --host 0.0.0.0 --port $(PORT) --workers 2 --reload

ngrok:
	ngrok http $(PORT)

# ── Atajo del día ─────────────────────────────────────────────────────────────

probar-todo: db-reset etl
	@echo ""
	@echo "👉 Revisa cleaned_data/informe_calidad/informe_etl.xlsx"
	@echo "    Si OK:   make load && make seed-users && make verify-load && make test-all"

# ── Demo: levantar chatbot + ngrok en un solo comando ───────────────────────

demo-stop:
	@pkill -f "uvicorn main:app" 2>/dev/null || true
	@pkill -f "ngrok http" 2>/dev/null || true
	@echo "✅ Procesos detenidos"

demo: demo-stop
	@test -n "$(WA_TOKEN)" || (echo "ERROR: Usa make demo WA_TOKEN=<token_de_meta>"; exit 1)
	@sed -i "s|^WA_TOKEN=.*|WA_TOKEN=$(WA_TOKEN)|" .env
	@echo "✅ WA_TOKEN actualizado en .env"
	@cd ai_agent && setsid python3 -m uvicorn main:app --host 0.0.0.0 --port $(PORT) > /tmp/chatbot.log 2>&1 &
	@sleep 3
	@curl -s http://localhost:$(PORT)/health > /dev/null && echo "✅ Uvicorn OK en puerto $(PORT)" || (echo "❌ Uvicorn falló"; exit 1)
	@setsid ngrok http $(PORT) > /tmp/ngrok.log 2>&1 &
	@sleep 4
	@NGROK_URL=$$(curl -s http://localhost:4040/api/tunnels 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['tunnels'][0]['public_url'])" 2>/dev/null); \
	echo "✅ Ngrok activo: $$NGROK_URL"; \
	echo ""; \
	echo "═══ Configurar en Meta Developers ═══"; \
	echo "  Callback URL: $$NGROK_URL/webhook"; \
	echo "  Verify token: 7a275268ea05768a7a5de0f8990fbd1"; \
	echo ""; \
	curl -s -m 3 "$$NGROK_URL/health" > /dev/null && echo "✅ Internet -> ngrok -> uvicorn: OK" || echo "❌ No se puede alcanzar desde internet"

# ── Deploy Railway ────────────────────────────────────────────────────────────

deploy-chatbot:
	cd ai_agent && railway up --service "Altrans Chatbot" --detach

deploy-notifications:
	cd notifications && railway up --service "Altrans Notifications" --detach

deploy-all: deploy-chatbot deploy-notifications

clean:
	@find . -type d \( -name __pycache__ -o -name .pytest_cache \) -exec rm -rf {} + 2>/dev/null || true
	@echo "✅ Caches limpios"
