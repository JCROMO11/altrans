# Workflow — Sesión 19 Agosto 2026

## Estado Actual

### Railway — 2 servicios desplegados
| Servicio | URL | Estado |
|---|---|---|
| **Chatbot Altrans** | `https://altrans-production.up.railway.app` | ✅ Online |
| **Notifications Altrans** | `https://notifications-altrans-production-5b04.up.railway.app` | ✅ Online |

### WhatsApp Cloud API — Configuración
- WABA: `2434251620392649` (Test WhatsApp Business Account)
- Phone: `1135782176294036` (+1 555-183-1621)
- App: `1355075269345648` ("Altrans Chatbot")
- Webhook: `https://altrans-production.up.railway.app/webhook`
- Verify token: `7a275268ea05768a7a5de0f8990fbd1`
- WA_TOKEN (token actual) almacenado en `.env` local y en Railway (ambos servicios)

### Chatbot — Flujo funcionando
1. Usuario escribe "Hola" → webhook recibe → chatbot responde pidiendo cédula
2. Usuario escribe cédula → valida vs `manifiestos_flat` → pide manifiesto
3. Usuario escribe manifiesto → verifica pertenencia → sesión activa
4. Usuario consulta saldos, manifiestos, etc.
- Límite: 4 consultas por sesión, sesión expira en 8h
- Tasa: 5 msg/min, rate limiter con cola

### LLM — Proveedores y fallback
- Cadena: **DeepSeek directo** (`deepseek-chat`, primario) → **OpenRouter** (`deepseek/deepseek-v4-flash`, alt) → **Groq** (`openai/gpt-oss-20b`, última línea, free tier)
- DeepSeek es la fuente principal; Groq sirvió de respaldo mientras faltaba `DEEPSEEK_API_KEY` en Railway (ya se agregó)
- **PREGUNTAR EN LA EMPRESA**: ¿la empresa recargará créditos en los 3 servicios? Recomendación: recargar DeepSeek ($5, es la primaria), **no** recargar OpenRouter (agrega markup sobre el mismo modelo DeepSeek → doble gasto; dejar solo como respaldo ante caída de DeepSeek con crédito mínimo), mantener Groq gratis como última línea
- ⚠️ OpenRouter actualmente con 402 (créditos agotados); Groq es el respaldo efectivo hoy

### Notificaciones — Envío manual funciona ✅
- 4 plantillas: `saldo_falta_factura`, `saldo_falta_documentacion`, `saldo_novedad_pendiente`, `saldo_plazo_vigente`
- 1 plantilla de pago: `pago_realizado`
- Backup vía email: funciona
- Las notificaciones automáticas fallaron porque el WA_TOKEN del servicio Notifications estaba desactualizado. **Ya se actualizó**.
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

### Pendientes para próxima sesión

#### 1. Probar chatbot en Railway
- Enviar "Hola" a +1 555-183-1621
- Probar login con cédula + manifiesto (conductores de prueba: 1023871762/manif 22883)
- Verificar respuestas del agente en sesión activa

#### 2. Probar notificaciones automáticas
- Esperar a las 6AM o 12PM Colombia (hora programada)
- O llamar manualmente: `POST /admin/auto-notify` con header `x-admin-token`
- Verificar que se envíen los WhatsApp a conductores con saldos pendientes

#### 3. Modificaciones de gerencia (pendientes)
- User tiene "respuestas de gerencia" con cambios a implementar
- Revisar requerimientos y modificar código del chatbot/notificaciones

#### 4. WA_APP_SECRET (HMAC del webhook) — pendiente de producción real
- `WA_APP_SECRET` (App Secret de la app de Meta "Altrans Chatbot") está **vacío/ausente**: la validación `X-Hub-Signature-256` del webhook se omite (`hmac_skipped_no_secret` en main.py)
- Configurarlo **después** de la verificación del negocio de Meta, cuando esté listo para producción real
- Se obtiene en Meta Developer Portal → Configuración → Básico → "Clave secreta de la aplicación" (es distinto de WA_TOKEN y WA_VERIFY_TOKEN)
- Configurar en Railway (Chatbot Altrans) y en `.env` local; luego probar webhook completo
