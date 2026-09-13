# Workflow — Sesión 11 Sep 2026

## Estado Actual

### Hosting — Render (actual, free) → Railway (pago, pendiente)
| Servicio | URL | Estado |
|---|---|---|
| **Chatbot** | `https://altrans-chatbot.onrender.com` | ✅ Online |
| **Notifications** | `https://altrans-notifications.onrender.com` | ✅ Online |
| **Dashboard** | `https://dashboard-2zk.pages.dev` | ✅ Cloudflare Pages |

- ⚠️ **Render free se duerme** tras ~15 min sin tráfico → el scheduler de
  notificaciones no dispara de forma fiable (cold start). Es **temporal**:
  al pagar Railway se migra (blueprint de referencia en `render.yaml`).
- **Railway sin pagar** → sin deploy. Al pagar: setear `WA_PHONE_NUMBER_ID`,
  `WA_TOKEN`, `WA_SEND_MODE=template`, `WA_APP_SECRET` y URLs
  (`CHATBOT_URL`, `NOTIFICATIONS_URL`, `DASHBOARD_URL`).

### WhatsApp Cloud API — Configuración
- WABA **producción**: `2373194893454397` ("Altrans SAS", COP, tz America/Bogota)
- Phone **producción**: `1218689764671502` (+57 312 3228874, verified_name "Altrans SAS", CONNECTED)
- WABA/Phone de prueba (antiguos): WABA `2434251620392649` / Phone `1135782176294036` (+1 555-183-1621)
- App: `1355075269345648` ("Altrans Chatbot")
- Webhook (Meta): `https://altrans-chatbot.onrender.com/webhook`
- Verify token: `7a275268ea05768a7a5de0f8990fbd1`
- WA_TOKEN (SYSTEM_USER permanente) en `.env` local y en Render (ambos servicios)
- HMAC del webhook ✅ **resuelto**: `WA_APP_SECRET` configurado en `.env` y Render.
  Verificado contra producción: firma inválida/ausente → `403`, firma válida → `200`
  (`_validate_signature` en `ai_agent/main.py`). Ya no aparece `hmac_skipped_no_secret`.
- WABA `account_review_status=APPROVED`; phone `CONNECTED` / `VERIFIED`, quality `GREEN`.
  Plantillas `altrans_*` APPROVED y entregando en el número de producción.
- ⚠️ Las plantillas `altrans_*` deben existir **en el WABA del número** (producción `2373194893454397`).
  Si faltan → Meta error `132001 Template name does not exist in the translation`.
  Crear/verificar con `WABA_ID=2373194893454397 python -m scripts.crear_plantillas_altrans --list`
- ⚠️ `GET /<PHONE_NUMBER_ID>?fields=health_status` revela el WABA que posee el número
  (entidad `WABA`), útil si no se tiene `business_management` para enumerar WABAs.

### Chatbot — Flujo funcionando
1. Usuario escribe "Hola" → webhook recibe → chatbot responde pidiendo cédula
2. Usuario escribe cédula → valida vs `manifiestos_flat` → pide manifiesto
3. Usuario escribe manifiesto → verifica pertenencia → sesión activa
4. Usuario consulta saldos, manifiestos, etc.
- Límite: 4 consultas cada 8h por número (tabla `chatbot_cuota`, ventana persistente).
  Sobrevive al cierre de sesión ("gracias"/inactividad) y al re-login; solo se
  reinicia si pasan 8h desde `ventana_inicio`. `chatbot_sesiones.msg_count` queda
  como espejo para logs/estado.
- Tasa: 5 msg/min, rate limiter con cola

### LLM — Proveedores y fallback
- Cadena: **DeepSeek directo** (`deepseek-chat`, primario) → **Groq** (`openai/gpt-oss-20b`, última línea, free tier)
- **OpenRouter RETIRADO** (sep-2026): no se usa. `OPENROUTER_API_KEY` comentada en `.env`
  y eliminada de `config.py`/`render.yaml`. `DEEPSEEK_API_KEY` ahora es requerida por `get_settings()`.
- **PREGUNTAR EN LA EMPRESA**: ¿recargan créditos en DeepSeek? (es la primaria; ~$5).
  Groq se mantiene gratis como respaldo.

### Preguntas para la reunión con Altrans
1. **Créditos LLM**: ¿recargan DeepSeek? (primario). Groq gratis como respaldo. OpenRouter ya no se usa.
2. **Contacto humano**: número real para `WA_CONTACTO_HUMANO` (hoy `600 00 00`).
3. **Inconsistencia de montos**: criterio correcto entre "me deben" (suma `saldo`,
   p. ej. `$7.175.400`) y resumen anual (resta `valor_pagado`, `$6.647.775`).
4. **Archivados en KPIs y corte de notificaciones** (ver
   `docs/informe_columnas_roles_y_datos_historicos.md`, Parte 2):
   ¿se excluyen los 10.401 archivados de los KPIs/totales/tendencia/catálogos?
   ¿el corte de notificaciones en 2026-01-01 es el correcto (evita 838 de 2023-2025)?
5. **Columnas por rol** — ¿aprueban los campos a los que accede cada rol?
   - **Conductor**: `manifiesto, fecha_despacho, origen, destino, cliente,
     flete_conductor, saldo, fecha_cumplido, compromiso_pago, fecha_estimada_pago,
     fecha_pago, valor_pagado, estado_interno, novedades, mes, año`
     (+ su cédula/celular).
   - **Propietario**: lo anterior para los viajes de su placa + `conductor, placa,
     propietario`; puede ver cédula y celular de los conductores que manejaron su vehículo.
   - **Bloqueado a ambos**: datos de otros conductores/propietarios y consolidados
     de la empresa (facturación, NIT, listas de conductores, totales).
6. **Modificaciones de gerencia**: implementar las "respuestas de gerencia" pendientes.

### Notificaciones — Envío manual funciona ✅
- 4 plantillas: `saldo_falta_factura`, `saldo_falta_documentacion`, `saldo_novedad_pendiente`, `saldo_plazo_vigente`
- 1 plantilla de pago: `pago_realizado`
- **AUTO-NOTIFY DESHABILITADO** (13-sep-2026) con `AUTO_NOTIFY_ENABLED=false` hasta
  aprobación de la empresa. Los envíos manuales (`/admin/auto-notify`, `/preview`) siguen.
  `GET /health` expone `auto_notify_enabled`; con false el scheduler solo deja
  `backup_entre_semana` y `morning_report`.
- **E2E contra Render OK**: `POST /admin/notify/preview` → 913 msgs / 435 celulares;
  15 plantillas `sent`; dedup y `messages_sent` OK. El usuario confirmó que **le llegaron** los WhatsApp.
- Con los datos del 13-sep: 976 saldos + 144 `pago_realizado` pendientes (no se envían).
- Backup vía email: funciona. **Fix 13-sep**: `manifiestos_flat` daba 504 y el ZIP
  parcial se enviaba como exitoso; ahora hay reintento por tabla, un backup incompleto
  se marca `[INCOMPLETO]`, no se sube al bucket y no se reporta como válido.
- Demo script: `tests/demo_notificaciones_20260717.py`

### Monitoreo — Chequeo matutino (implementado en sesión del 19-ago-2026)
- `notifications/health_report.py`: `run_morning_check()` revisa en un solo lugar:
  - Servicios: Chatbot `/health`, Notifications `/health`, Dashboard `/`
  - Infraestructura: vigencia WA_TOKEN (debug_token), filas+frescura de `manifiestos_flat`,
    último backup (bucket `altrans-backups`), auto-notify de hoy (`messages_sent`),
    sesiones activas/bloqueadas, jailbreaks y errores ERROR (24h)
- Se ejecuta automáticamente todos los días a las **7:00 AM Colombia** (job `morning_report`
  en `scheduler.py`), y manualmente con `POST /admin/morning-check` (header `x-admin-token`)
  o `make morning-check`
- Envía el resumen por email (Brevo) y/o WhatsApp según `MORNING_REPORT_EMAIL` / `MORNING_REPORT_TO`
- Heartbeats opcionales a Healthchecks.io tras cada job (`HC_BACKUP_URL`, `HC_NOTIFY_URL`, `HC_MORNING_URL`)
- Vars nuevas en `.env`: `CHATBOT_URL`, `NOTIFICATIONS_URL`, `DASHBOARD_URL`, `MORNING_REPORT_*`, `HC_*_URL`
- Dashboard URL: `https://dashboard-2zk.pages.dev` (Cloudflare Pages) → `DASHBOARD_URL`
- El chequeo matutino del 19-ago detectó 401 del WA_TOKEN y 1000 errores de auto-notify;
  **causa confirmada: falta el WA_TOKEN definitivo** (se actualiza con `make update-wa-token WA_TOKEN=<tok>`)

### Sesión 13-sep-2026 (pre-piloto)
- **Datos actualizados** con el Excel de hoy: 14.154 → 14.486 manifiestos.
  - Enum `responsable_enum` +`JOHANA` (consolidado en `schema_consolidated.sql`).
  - `load_flat.py` normaliza `N/A` → `NULL` en `semana` (el esquema exige `Semana N`).
  - Archivado: 10.401 filas (`make archive-historico`); se corrigió que el UPDATE
    re-tocaba las ya archivadas (faltaba el filtro `archivado IS DISTINCT FROM`).
- **OpenRouter retirado** de la cadena LLM (DeepSeek directo → Groq).
- **Backup blindado** y **sink de logs desactivado en tests** (`LOG_SINK_ENABLED=false`).
- **Informe para la empresa**: `docs/informe_columnas_roles_y_datos_historicos.md`.
- Gerentes confirmados en `admin_usuarios`: Julián `573004724887`, Julio `573184871084`.
- **Tests del chatbot** (`make test-agent`): **130/130** (138 casos, DeepSeek). Se corrigieron
  falsos negativos del LLM-judge: `tools_called` ahora es **lista de nombres** en
  `agent/graph.py` y se le pasa al judge; ante un FAIL el judge pide una **2ª opinión**
  (`_JUDGE_PROMPT_CONFIRM` en `scripts/test_agent.py`). Prompts mejorados (inferencia de
  fechas y de saldo; `propietario_block` v3, `system_prompt_base` v3) en `agent/prompts.py`,
  `schema_consolidated.sql` y la tabla `system_prompts`. `make test-dashboard` 101/101.
- **DB/Supabase**: `make test-db` en verde (se actualizaron los tests tras el rename
  `tipo_vehiculo`→`placa_remolque`, el constraint `chk_semana_formato` y el test de
  CASCADE en `audit_log`, ahora append-only). **Advisors de performance en 0**
  (InitPlan `(select auth.role())`, policies acotadas por rol, 2 índices sin uso
  dropeados) — todo consolidado en `supabase/schema_consolidated.sql`. HIBP no
  aplica (requiere plan Pro).
  CLI re-linkeado a `cymxvxrdfkmydnbuvbeq` (apuntaba a `kpepoagujbeiyyqpvxpo`, inexistente).
- ⚠️ **504 transitorios de PostgREST** en Supabase free (visto en backup
  `manifiestos_flat` y en `tendencia_anual`): reintentar; ya hay retry en backup y en
  `ai_agent/db/queries._get` (429/5xx + timeouts, 3 intentos con backoff).

### Pendientes para próxima sesión

#### 1. Chatbot en producción (Render) — ✅ probado
- Flujo verificado: "Hola" → cédula → manifiesto → consultas, enviando desde el
  número de producción (`+57 312 3228874`) al celular de prueba.
- Cupo persistente de 8h validado (sobrevive a "gracias" y al re-login).
- Datos de prueba: DAVID ARNALDO SERNA `98702858` (manifiestos `35740`/`34241`).

#### 2. Notificaciones automáticas — ✅ manual OK
- `POST /admin/notify/preview` y envío manual confirmados contra Render.
- Falta confirmar el disparo automático del scheduler (ver nota de Render free
  arriba); manual: `POST /admin/auto-notify` con header `x-admin-token`.

#### 3. Modificaciones de gerencia (pendientes)
- User tiene "respuestas de gerencia" con cambios a implementar
- Revisar requerimientos y modificar código del chatbot/notificaciones

#### 4. Mejoras al chatbot implementadas (Sep 09 2026)
- [x] **Contacto humano alternativo**: intercepción por regex en `webhook.py`
      (`_PEDIR_HUMANO_RE`) → responde con el número de `WA_CONTACTO_HUMANO`
      (placeholder `600 00 00` — ⚠️ **reemplazar por el real en producción**).
      Línea corta "📞 Contacto Altrans" también en el saludo inicial y post-login.
      No consume consulta ni entra al historial; no aplica a admins.
- [x] **Auto-logout por inactividad**: columna `inactividad_avisado_at` en
      `chatbot_sesiones` (ALTER TABLE aplicado) + worker `whatsapp/inactivity.py`
      lanzado en startup de `main.py` (1 instancia). Env: `WA_INACT_AVISO_MIN=5`,
      `WA_INACT_CIERRE_MIN=10`. Solo sesiones `estado=activa`.
- [x] Fix loguru: `format` callable devolvía JSON → loguru lo leía como plantilla
      `{...}` (KeyError '"ts"'). Solución: escapar llaves en `logging_config.py`.

#### 5. WA_APP_SECRET (HMAC del webhook) — ✅ resuelto
- `WA_APP_SECRET` configurado en `.env` local y en Render (Chatbot). Al migrar a
  Railway, copiar la misma variable.
- Verificado contra producción: firma inválida/ausente → `403`, firma válida → `200`.
- ⚠️ Si el secreto se cambia y queda mal, Meta recibe 403 en **todos** los webhooks
  y el bot queda mudo; probar con un WhatsApp real justo después de configurarlo.

#### 6. Contacto humano — pendiente del número real
- `WA_CONTACTO_HUMANO=600 00 00` sigue siendo **placeholder**. Reemplazar por el
  número real cuando Altrans lo entregue (reunión). Aplica a `_CONTACTO_HUMANO` en
  `ai_agent/whatsapp/webhook.py` y a la línea "📞 Contacto Altrans" del saludo/post-login.
- Nota menor: el valor sin comillas rompe `set -a; . .env` en scripts de shell
  (python-dotenv lo tolera). Al poner el real, usar comillas si tiene espacios.

#### 7. Inconsistencia de montos — decisión de negocio
- "¿Cuánto me deben?" (`manifiestos_pendientes_pago`, suma `saldo`) vs resumen anual
  (`resumen_periodo`, resta `valor_pagado`): difieren por los abonos de pago parcial.
  Ejemplo DAVID ARNALDO SERNA (`98702858`): `$7.175.400` vs `$6.647.775`
  (diferencia `$527.625` = abono del manifiesto 34287).
- Pendiente definir el criterio correcto con la empresa y unificar.

#### 8. Migrar a Railway (pago)
- Al pagar: setear `WA_PHONE_NUMBER_ID`, `WA_TOKEN`, `WA_SEND_MODE=template`,
  `WA_APP_SECRET` y URLs; redeploy; reconfigurar el webhook de Meta a la URL de Railway.
- Mientras tanto, Render free: el scheduler de notificaciones no es fiable
  (cold start). Si se necesita ya, cron externo que pegue a `/admin/auto-notify`.
