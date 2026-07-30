# Workflow — Sesión 24 Julio 2026

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

### Notificaciones — Envío manual funciona ✅
- 4 plantillas: `saldo_falta_factura`, `saldo_falta_documentacion`, `saldo_novedad_pendiente`, `saldo_plazo_vigente`
- 1 plantilla de pago: `pago_realizado`
- Backup vía email: funciona
- Las notificaciones automáticas fallaron porque el WA_TOKEN del servicio Notifications estaba desactualizado. **Ya se actualizó**.
- Demo script: `tests/demo_notificaciones_20260717.py`

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
