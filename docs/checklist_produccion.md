# Checklist de Producción — Altrans

> Generado: 13-sep-2026. Cambiar cuentas personales por cuentas de la empresa
> antes/durante la migración a producción. No commitear valores; solo nombres.

## A. Cuentas personales → de la empresa

| Variable | Servicio | Hoy | Acción |
|---|---|---|---|
| `DEEPSEEK_API_KEY` | chatbot + notifications | cuenta personal | clave de la empresa + recargar créditos |
| `GROQ_API_KEY` | chatbot + notifications | cuenta personal | clave de la empresa (fallback) |
| `BREVO_API_KEY` | notifications | cuenta personal | cuenta Brevo de la empresa |
| `BREVO_SMTP_LOGIN` | notifications | `jromoguijarro@gmail.com` | login corporativo |
| `BREVO_SMTP_PASSWORD` | notifications | clave personal | clave corporativa |
| `BACKUP_EMAIL_FROM` | notifications | `jromoguijarro@gmail.com` | remitente corporativo |
| `BACKUP_EMAIL_TO` | notifications | `jromoguijarro@gmail.com` | destinatarios de la empresa |
| `MORNING_REPORT_EMAIL` | notifications | `jromoguijarro@gmail.com` | correo(s) de la empresa |
| `MORNING_REPORT_TO` | notifications | vacío | WhatsApp de la empresa (opcional) |
| `HC_BACKUP_URL` / `HC_NOTIFY_URL` / `HC_MORNING_URL` | notifications | Healthchecks.io personal | cuenta de la empresa o quitar |
| `ADMIN_TOKEN` | chatbot + notifications | valor actual | **rotar** a uno fuerte de la empresa |
| `JWT_SECRET` | chatbot | valor actual | **rotar** |

Nota: el envío de correo usa **Brevo**. No hay Resend en el código.

## B. Infraestructura a migrar

| Área | Hoy | Producción |
|---|---|---|
| Base de datos | Supabase pruebas `cymxvxrdfkmydnbuvbeq` | proyecto definitivo de la empresa (candidato `kghiaaeilszubxudzdji` "Altrans") |
| Variables DB | — | `SUPABASE_URL`, `SUPABASE_SERVICE_KEY`, `SUPABASE_ANON_KEY`, `DATABASE_URL`, `VITE_SUPABASE_URL`, `VITE_SUPABASE_ANON_KEY` (dashboard) |
| Hosting | Render free (se duerme) | Railway pagado de la empresa |
| URLs | — | `CHATBOT_URL`, `NOTIFICATIONS_URL`, `DASHBOARD_URL` a las URLs finales |
| WhatsApp/Meta | App `Altrans Chatbot` bajo cuenta personal | transferir App + WABA al Business Manager de la empresa: `WA_TOKEN`, `WA_PHONE_NUMBER_ID`, `WABA_ID`, `WA_APP_SECRET`, `WA_VERIFY_TOKEN`, `APP_ID` |

## C. Meta Business Suite

- **Método de pago**: hoy tarjeta personal → cambiar a la de la empresa
  (Configuración del negocio → Facturación y pagos).
- El WABA de producción ya está a nombre de "Altrans SAS" (`2373194893454397`) y el
  número `+57 312 3228874`. Falta que **la App y el Business Manager** queden a
  nombre de la empresa para no depender de la cuenta personal.

## D. Pendientes de valor

- `WA_CONTACTO_HUMANO` = `600 00 00` (placeholder) → número real.
- `AUTO_NOTIFY_ENABLED` = `false` → poner `true` **solo** cuando la empresa apruebe
  el envío automático.
- `WA_SEND_MODE` ya está en `template` (correcto).
- Decisión de negocio: archivados en KPIs y corte de notificaciones (`2026-01-01`).
- Decisión de negocio: criterio de montos ("me deben" vs resumen anual).

## E. Limpieza del repo (pendiente)

Dejar el repo con lo necesario para operar; borrar lo generado, lo de demo y lo
que no se use. Candidatos (verificar referencias antes de borrar):

- **Assets sin uso** (0 referencias en el código):
  - `dashboard/src/assets/react.svg`, `dashboard/src/assets/vite.svg` (defaults de Vite)
  - `dashboard/src/assets/hero.png` (44K)
  - `dashboard/public/icons.svg`
- **Scripts de demo / datos de prueba** (mover a `tests/` o borrar si ya no se usan):
  - `scripts/demo_notify.py`
  - `tests/demo_notificaciones_20260717.py`, `tests/demo_notificaciones_e2e.py`
  - `tests/test_chatbot_demo_20260717.py`, `tests/test_chatbot_live_20260717_1151.json`
  - `tests/generar_excel_comparacion.py`, `tests/generar_excel_prueba.py`
  - `tests/orquestar_prueba_escalonada.py`, `tests/setup_datos_prueba.py`
- **Fixtures / verificadores puntuales** (evaluar): `ai_agent/scripts/descubrir_fixtures.py`,
  `ai_agent/scripts/fixtures_auto.py`, `etl_individual/verify_*.py`.
- **Docs obsoletos**: `docs/informe_estado.txt`; `dashboard/README.md` (genérico de Vite).
- **Temporal**: `render.yaml` (se retira al consolidar Railway).
- **Ignorados fuera del repo** (no limpiar a mano, ya no se versionan): `data/`,
  `data_sheets/`, `cleaned_data/`, `backups/`, `dashboard/dist/`, `.pytest_cache/`,
  `ai_agent/scripts/reportes/`, `.env`/`dashboard/.env`.
- **Nota `.gitignore`**: `docs/` está ignorado, por eso este checklist se agrega con
  `git add -f docs/checklist_produccion.md`. Decidir si se saca `docs/` del ignore
  para versionar la documentación del proyecto.
- Revisar que no queden binarios/caches/artefactos trackeados antes del cierre.
