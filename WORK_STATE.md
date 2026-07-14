# Work State — Chatbot Altrans

## Objetivo
Implementar rate limiting (concurrencia + ventana deslizante), migrar logging a loguru, refactorizar todo el código a async, y añadir tips para conductores sobre cómo usar el chatbot.

## Cambios realizados (commit actual)

### Nuevos archivos
- `ai_agent/core/rate_limiter.py` — Lock por concurrencia (`asyncio.Lock`) + ventana deslizante (5 msg/min) + cola de mensajes pendientes por usuario
- `ai_agent/core/middleware.py` — Rate limiting REST: `/login` (5/min por IP), `/chat` (10/min por token JWT)
- `ai_agent/core/__init__.py` — Paquete vacío

### Archivos modificados
- `ai_agent/logging_config.py` — Migrado de `logging` estándar a `loguru` con formato JSON a stdout
- `ai_agent/config.py` — Nuevas constantes: `MAX_MSG_PER_MINUTE=5`, `LOGIN_MAX_PER_MIN=5`, `CHAT_MAX_PER_MIN=10`
- `ai_agent/requirements.txt` — Añadido `loguru`
- `ai_agent/db/queries.py` — `httpx.Client` → `httpx.AsyncClient`, todas las funciones ahora `async`
- `ai_agent/whatsapp/client.py` — Migrado a async + loguru, `send_text()` y `mark_as_read()` ahora son `async`
- `ai_agent/agent/graph.py` — `AsyncOpenAI` + `AsyncGroq`, `run()` y `moderate()` ahora son `async`, loguru, corregido bug de sintaxis en `llm_both_failed`
- `ai_agent/agent/tools.py` — Todas las tool executions ahora son `async` (`await fn(args)`)
- `ai_agent/whatsapp/webhook.py` — `handle_message()` async, integrado `rate_limiter.try_acquire/release`, tips para conductores (`_TIPS`), loguru
- `ai_agent/main.py` — Todos los endpoints async, `asyncio.create_task` para webhook, `RateLimitMiddleware`, loguru
- `tests/test_webhook.py` — Actualizado para async con `_run_async()` wrapper, mocks de rate limiter, nuevo grupo `TestContadorConsultas` (3 tests), añadido mock de `get_admin`

### Detalles técnicos
- **Rate limiting por mensaje**: 3 capas en `core/rate_limiter.py` — lock por concurrencia, ventana deslizante (5 msg/min por usuario), cola si el usuario está ocupado
- **Rate limiting REST**: `core/middleware.py` — 5/min por IP en `/login`, 10/min por token en `/chat`
- **Todo en memoria** (sin Redis) — escala a 1 worker
- **Tips**: Constante `_TIPS` en `webhook.py` — "Escribe cada consulta completa en un solo mensaje. Tienes 4 consultas por sesión — úsalas para preguntas concretas". Se muestra al iniciar sesión y al activar la sesión.
- **Conteo de consultas**: Solo descuenta cuando `tools_called=True` (línea 463-464 de `webhook.py`). Aclaraciones, jailbreaks, errores no descuentan.
- **Modelo LLM**: `deepseek/deepseek-v4-flash` via OpenRouter (primario), `llama-3.3-70b-versatile` via Groq (fallback gratuito), `openai/gpt-oss-safeguard-20b` (moderación)
- **Tests**: 62/62 pasan

## Pendiente / Backlog
- [ ] **Persistencia de logs** — Hoy los logs de loguru solo van a stdout, se pierden al reiniciar. Opción: archivo rotativo o servicio externo.
- [ ] **Prompts en BD** — Hoy están hardcodeados en `agent/graph.py` y `agent/prompts.py`. Mover a tabla `prompts` en Supabase.
- [ ] **Control de costos LLM (aplazado)** — Limitar tokens por sesión o por día. Se evaluará cuando haya uso real para decidir umbrales. DeepSeek v4 Flash es muy barato (~$0.15/M input), por lo que el costo mensual estimado es bajo.
- [ ] **Tests de integración** — Con Supabase real o testcontainers.
- [ ] **Dashboard de uso** — Consultas por conductor, tasa de error, costos.