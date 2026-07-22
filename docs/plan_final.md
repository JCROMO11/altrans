# Plan Final — Dashboard Altrans

## Estado actual

- **Bloques 1-6**: ✅ Completos (FE multi-manifiesto, Restablecer, Exportar CSV/Excel, Iconos FE)
- **Bloque 7 (Roles/permisos)**: 🟡 Diagnóstico completo, pendiente reunión con gerencia
- **Toggle "Ver solo mis manifiestos"**: ✅ Implementado (match nombre_usuario → responsable vía normalize)
- **Validaciones Excel**: ✅ toNum() limpia $, celular validado, RESPONSABLE_FIXES sincronizado, ciudades faltantes agregadas
- **Tests**: 101/101 pasando

---

## Prioridad 1 — Verificar limpieza del Excel ⚠️ PENDIENTE

Revisar que `data/PRODUCCIÓN ALTRANS S.A.S.xlsx` se importe correctamente.

### Hallazgos — corregidos (✅) y pendientes (⚠️)

| #   | Hallazgo | Estado | Dónde |
| --- | -------- | ------ | ------ |
| 1.1 | `toNum()` no limpia `$` — Python sí | ✅ Corregido: `s.replace(/[^0-9.,;\-]/g, '')` | `excel-upload.js` |
| 1.2 | `celular` sin validación — DB exige `^\d{10}$` | ✅ Corregido: `cleanCelular()` nullifica no-válidos | `excel-upload.js` |
| 1.3 | `guardar_digitador_batch` usa `COALESCE` en conductor/propietario | ⚠️ Requiere modificar RPC en Supabase | `schema_consolidated.sql` |
| 1.4 | `parseCiudad()` faltan `"SANTA FE DE BOGOTA"`, `"SANTA MARTA"` | ✅ Corregido: agregados a `CITY_DEPT_FALLBACK` | `geography.js` |
| 1.5 | `RESPONSABLE_FIXES` JS incompleto | ✅ Corregido: agregado `',': null` (match Python) | `excel-upload.js` |
| 1.6 | **Toggle "Ver solo mis manifiestos"** | ✅ Nuevo: match nombre_usuario → responsable + toggle | `ConsultaPage.jsx` |

### Acciones pendientes

1. Revisar 1.3 (COALESCE en RPC) — requiere cambio en Supabase, pendiente de reunión
2. Comparar el Excel contra la DB para cuantificar filas problemáticas restantes

---

## Prioridad 2 — Reforzar validaciones ✅ COMPLETADO

Cerrar los gaps de validación identificados en P1.

### Implementado

| Capa                        | Qué se agregó                                                               | Archivo                                         |
| --------------------------- | --------------------------------------------------------------------------- | ----------------------------------------------- |
| **Excel upload (JS)** | Stripear `$` en `toNum()`                                               | `excel-upload.js`                             |
| **Excel upload (JS)** | `cleanCelular()` — valida 10 dígitos, nullifica inválidos                   | `excel-upload.js`                             |
| **Excel upload (JS)** | `',': null` en RESPONSABLE_FIXES (match Python)                             | `excel-upload.js`                             |
| **Excel upload (JS)** | `"SANTA FE DE BOGOTA"`, `"SANTA MARTA"` a CITY_DEPT_FALLBACK               | `geography.js`                                |
| **Dashboard**         | Toggle "Ver solo mis manifiestos" + match nombre_usuario → responsable      | `ConsultaPage.jsx`                            |

### Pendiente (pide reunión)

| Capa                        | Qué falta                                                                   |
| --------------------------- | --------------------------------------------------------------------------- |
| **API layer**         | `Field(gt=0, pattern=...)` en modelos Pydantic existentes                |
| **Supabase RPC**      | Validar placa, celular, valores no negativos en `guardar_digitador_batch` |

---

## Prioridad 3 — Probar chatbot (3 roles)

### Tests existentes

| Archivo                                   | Tests | Cobertura                                     |
| ----------------------------------------- | ----- | --------------------------------------------- |
| `tests/test_chatbot_rest_api.py`        | 131   | API REST auth + chat                          |
| `tests/test_chatbot_profile_filters.py` | 290   | Filtros por rol (conductor/propietario/admin) |
| `tests/test_webhook.py`                 | 600   | Auth WhatsApp, jailbreak, rate limits         |
| `tests/test_prompts.py`                 | 204   | System prompts                                |
| `tests/test_chatbot_demo_20260717.py`   | 178   | Demo E2E: 10 preguntas                        |

### Pruebas manuales por rol

| Rol                   | Preguntas a probar                                                                                                               |
| --------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| **Conductor**   | "¿Cuánto me deben?", "¿Qué manifiestos tengo?", "¿Cuándo me pagan?", "¿Qué facturas me faltan?"                          |
| **Propietario** | "¿Qué viajes tiene mi placa X?", "¿Cuánto debe la placa Y?" (debe negarse), "¿Cuánto debe toda la empresa?" (debe negarse) |
| **Admin**       | "Top 5 conductores por flete", "¿Cuánto debe la empresa?", "Manifiestos sin factura", consultar datos de cualquier conductor   |

### Criterio de éxito

- 1203 tests pasan
- Cada rol solo ve sus datos (conductor solo lo suyo, propietario solo su placa, admin todo)
- Jailbreak bloquea intentos de inyección
- Rate limits funcionan (4 queries para no-admin)

---

## Prioridad 4 — Probar notificaciones

### Tests existentes

| Archivo                                   | Tests | Cobertura                                       |
| ----------------------------------------- | ----- | ----------------------------------------------- |
| `tests/test_notificaciones_api.py`      | 102   | API auth + body validation                      |
| `tests/test_notificaciones_db.py`       | 404   | Triggers, RPC, dedup, noise filtering           |
| `tests/demo_notificaciones_20260717.py` | 191   | Demo E2E: 5 templates + pago_realizado + backup |

### Casos a probar con datos artificiales

| #   | Template                      | Cómo provocarlo                                                       |
| --- | ----------------------------- | ---------------------------------------------------------------------- |
| 4.1 | `saldo_plazo_vigente`       | Setear`fecha_cumplido` en una fila → trigger inserta `pending`    |
| 4.2 | `saldo_pago_realizado`      | Setear`fecha_pago` + `valor_pagado` → trigger inserta `pending` |
| 4.3 | `saldo_falta_factura`       | Manifiesto cumplido sin`factura_electronica`                         |
| 4.4 | `saldo_falta_documentacion` | Cumplido hace >21 días sin documentación                             |
| 4.5 | `saldo_novedad_pendiente`   | Novedad real (filtrar ruido TURBO/URBANO)                              |

### Criterio de éxito

- 697 tests pasan
- Los 5 templates se envían correctamente
- Dedup funciona (no re-envía dentro de 7 días)
- Ruido (TURBO, URBANO, `.`, `ok`) se filtra
- Backup por email funciona

---

## Prioridad 5 — Implementar dashboard + merge

**Gated a**: reunión con gerencia responda preguntas P10-P15 (Bloque 7).

### Pasos

1. Implementar lo que falte del dashboard según respuestas de gerencia
2. Prueba integral del flujo completo: Excel → DB → dashboard → chatbot → notificaciones
3. Merge a `main`

### Criterio de éxito

- Dashboard completo funcional
- Todos los tests pasan (1203 + nuevos)
- Gerencia confirma que el sistema responde a sus necesidades
- Merge sin conflictos a `main`

---

## Diagrama de dependencias

```
Semana actual:
  [P1 Excel check] ──→ [P2 Validaciones] ──→ [P3 Chatbot test]
         │                                        │
         └──────────────────┬─────────────────────┘
                            │
                      [P4 Notif. test]
                            │
               (reunión gerencia)
                            │
                   ┌────────┴────────┐
                   │                 │
              [P5 Dashboard]    [Preguntas
                   │            pendientes]
                   │                 │
                   └──────┬──────────┘
                          │
                    [Full test + merge]
```
