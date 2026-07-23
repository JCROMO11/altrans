# Estructura de Base de Datos — ALTRANS S.A.S.

## Tabla principal: `manifiestos_flat`

### Identificación

| Columna | Tipo | Restricciones | Notas |
|---------|------|--------------|-------|
| `manifiesto` | `BIGINT` | `PRIMARY KEY` | Número único del manifiesto |

### Contexto del origen

| Columna | Tipo | Restricciones | Notas |
|---------|------|--------------|-------|
| `archivo_origen` | `TEXT` | — | Nombre del archivo Excel de origen |
| `mes` | `TEXT` | `CHECK (mes::mes_enum IS NOT NULL)` | `ENERO`–`DICIEMBRE` |
| `año` | `SMALLINT` | `CHECK (año IS NULL OR (año >= 2023 AND año <= 2026))` | Rango esperado |
| `periodo` | `DATE` | — | `YYYY-MM-01` (primer día del mes) |
| `semana` | `TEXT` | `CHECK (semana IS NULL OR semana ~ '^Semana \d+$')` | Formato: `Semana 1` |
| `consecutivo_semanal` | `INTEGER` | — | Consecutivo dentro de la semana |

### Operación

| Columna | Tipo | Restricciones | Notas |
|---------|------|--------------|-------|
| `fecha_despacho` | `DATE` | — | Fecha de emisión/despacho |
| `origen` | `TEXT` | — | Ciudad de origen |
| `departamento_origen` | `TEXT` | — | Departamento de origen |
| `destino` | `TEXT` | — | Ciudad de destino |
| `departamento_destino` | `TEXT` | — | Departamento de destino |
| `cliente` | `TEXT` | — | Nombre del cliente/generador |
| `remesas` | `TEXT` | — | Códigos de remesa separados por coma |

### Financiero base

| Columna | Tipo | Restricciones | Notas |
|---------|------|--------------|-------|
| `valor_remesa` | `NUMERIC(14,2)` | `CHECK (>= 0)` | Valor de la(s) remesa(s) |
| `flete_conductor` | `NUMERIC(14,2)` | `CHECK (>= 0)` | Flete del conductor |
| `anticipo` | `NUMERIC(14,2)` | `CHECK (>= 0)` | Anticipo entregado antes de ruta |

### Vehículo y conductor

| Columna | Tipo | Restricciones | Notas |
|---------|------|--------------|-------|
| `placa` | `TEXT` | `CHECK (placa ~ '^[A-Z0-9]{4,7}$' OR placa IN ('ANULADO','CONS ANULADO'))` | Placa del vehículo tractor |
| `placa_remolque` | `TEXT` | — | Placa del remolque (antes llamado `tipo_vehiculo`) |
| `conductor` | `TEXT` | — | Nombre del conductor |
| `celular` | `TEXT` | `CHECK (celular ~ '^\d{10}$')` | Teléfono de 10 dígitos |
| `cedula_conductor` | `TEXT` | `CHECK (cedula_conductor ~ '^\d{6,10}$')` | Cédula de 6-10 dígitos |
| `propietario` | `TEXT` | — | Propietario/poseedor del vehículo |

### Despacho

| Columna | Tipo | Restricciones | Notas |
|---------|------|--------------|-------|
| `agencia_despachadora` | `TEXT` | `CHECK (agencia::agencia_enum IS NOT NULL)` | `CALI`, `BOGOTA`, `IPIALES`, `BUENAVENTURA` |
| `nombre_responsable` | `TEXT` | `CHECK (IN whitelist de 27 valores)` | Digitador responsable |

### Cumplimiento operativo

| Columna | Tipo | Restricciones | Notas |
|---------|------|--------------|-------|
| `fecha_cumplido` | `DATE` | — | Fecha de cumplido operativo |
| `compromiso_pago` | `TEXT` | `CHECK (compromiso_pago::compromiso_pago_enum IS NOT NULL)` | `DEFAULT 'PAGO A 15 DIAS'` |
| `novedades` | `TEXT` | — | Notas y novedades generales |
| `novedad_conductor` | `TEXT` | — | Novedades del conductor |
| `novedad_empresa` | `TEXT` | — | Novedades de la empresa |
| `estado_interno` | `TEXT` | `CHECK (estado_interno::estado_interno_enum IS NOT NULL)` | Ej: `CUMPLIDO`, `ANULADO` |
| `responsable_estado_interno` | `TEXT` | — | Quién marcó el estado |

### Ajustes al flete

| Columna | Tipo | Restricciones | Notas |
|---------|------|--------------|-------|
| `ajuste_positivo_flete` | `NUMERIC(14,2)` | `CHECK (>= 0)` | Ajuste que aumenta el flete |
| `ajuste_negativo_flete` | `NUMERIC(14,2)` | `CHECK (>= 0)` | Ajuste que disminuye el flete |
| `consignacion_a_terceros` | `NUMERIC(14,2)` | — | Consignación a terceros |
| `reteica` | `NUMERIC(14,2)` | `CHECK (>= 0)` | ReteICA (0.33%–1.0% según municipio) |
| `r_fopat` | `NUMERIC(14,2)` | `CHECK (>= 0)` | FOPAT (0.1% fijo) |
| `retencion_conductor` | `NUMERIC(14,2)` | `GENERATED ALWAYS AS (flete * 0.01)` | 1% de retención en la fuente |
| `saldo` | `NUMERIC(14,2)` | `GENERATED ALWAYS AS (flete + aj.positivo - aj.negativo - retencion - reteica - r_fopat - anticipo)` | Saldo neto a pagar |

### Pago al conductor

| Columna | Tipo | Restricciones | Notas |
|---------|------|--------------|-------|
| `fecha_pago` | `DATE` | — | Fecha de pago |
| `valor_pagado` | `NUMERIC(14,2)` | `CHECK (>= 0)` | Monto pagado |
| `entidad_financiera` | `TEXT` | `CHECK (entidad_financiera::entidad_financiera_enum IS NOT NULL)` | `TRANSF BANCOLOMBIA`, `CHEQUE DAVIVIENDA`, etc. |
| `responsable` | `TEXT` | `CHECK (responsable::responsable_enum IS NOT NULL)` | Tesorero responsable |

### Facturación

| Columna | Tipo | Restricciones | Notas |
|---------|------|--------------|-------|
| `factura_no` | `TEXT` | — | Número de factura |
| `fecha_factura` | `DATE` | — | Fecha de factura |
| `factura_electronica` | `TEXT` | — | Código FE (formato variable) |
| `mes_facturacion` | `SMALLINT` | — | 1–12 |
| `valor_factura` | `NUMERIC(14,2)` | `CHECK (>= 0)` | Valor facturado |
| `dias_para_facturar` | `INTEGER` | `GENERATED ALWAYS AS (fecha_factura - fecha_despacho)` | Días entre despacho y factura |

### Auditoría

| Columna | Tipo | Restricciones | Notas |
|---------|------|--------------|-------|
| `cargado_en` | `TIMESTAMPTZ` | `NOT NULL DEFAULT now()` | Fecha/hora de creación |
| `actualizado_en` | `TIMESTAMPTZ` | `NOT NULL DEFAULT now()` | Fecha/hora de última actualización |

### Reglas de negocio

| Nombre | Condición | Propósito |
|--------|-----------|-----------|
| `chk_cumplido_requiere_fe` | `fecha_cumplido NOT NULL ⇒ factura_electronica NOT NULL` | No se puede cumplir sin FE |

---

## ENUMs definidos

### `compromiso_pago_enum`
```
PAGO A 15 DIAS, PAGO A 20 DIAS, PAGO A 30 DIAS, PAGO A 5-8 DIAS,
CONTRAENTREGA, PRONTO PAGO, PAGO NORMAL, URBANO, ANULADO,
PAGADO, PAGO INMEDIATO, PRIORITARIO, RNDC, OTROS,
CONTINGENCIA 20-25 DH
```

### `estado_interno_enum`
```
CUMPLIDO, NO SE HA CUMPLIDO, ANULADO,
PENDIENTE FACTURA ELECTRONICA, NOVEDAD PENDIENTE, FACTURA RECIBIDA
```

### `agencia_enum`
```
CALI, BOGOTA, IPIALES, BUENAVENTURA, ANULADO
```

### `entidad_financiera_enum`
```
TRANSF BANCOLOMBIA, TRANSF DAVIVIENDA, TRANSF BANCO DE BOGOTA,
CHEQUE BANCOLOMBIA, CHEQUE DAVIVIENDA, CHEQUE BANCO DE BOGOTA,
CHEQUE, TRANSF/CHEQUE, ANULADO, OTRO
```

### `responsable_enum`
```
KAROL ARCINIEGAS, JOHANA UNIGARRO, ELIZABETH SUAREZ,
MILENA GUTIERREZ, MARIAE, FLOTA PROPIA, FP, ANULADO
```

### `mes_enum`
```
ENERO, FEBRERO, MARZO, ABRIL, MAYO, JUNIO,
JULIO, AGOSTO, SEPTIEMBRE, OCTUBRE, NOVIEMBRE, DICIEMBRE
```

---

## Índices

| Nombre | Columnas | Propósito |
|--------|----------|-----------|
| `idx_mflat_fecha_despacho` | `fecha_despacho` | Filtros por fecha |
| `idx_mflat_periodo` | `periodo` | Agrupación mensual |
| `idx_mflat_año_mes` | `año, mes` | Consultas por año+mes |
| `idx_mflat_cliente` | `cliente` | Búsquedas por cliente |
| `idx_mflat_conductor` | `conductor` | Búsquedas por conductor |
| `idx_mflat_placa` | `placa` | Búsquedas por placa |
| `idx_mflat_placa_remolque` | `placa_remolque` | Búsquedas por remolque |
| `idx_mflat_agencia` | `agencia_despachadora` | Filtros por agencia |
| `idx_mflat_archivo_origen` | `archivo_origen` | Trazabilidad de carga |
| `idx_mflat_cedula` | `cedula_conductor` | Búsquedas por cédula |
| `idx_mflat_estado_interno` | `estado_interno` | Filtros de estado |

---

## Tabla de auditoría: `audit_log`

| Columna | Tipo | Notas |
|---------|------|-------|
| `id` | `BIGSERIAL` | `PRIMARY KEY` |
| `manifiesto` | `BIGINT` | `NOT NULL` (sin FK: el DELETE debe sobrevivir) |
| `campo` | `TEXT` | `NOT NULL` — nombre de la columna modificada |
| `valor_anterior` | `TEXT` | Valor antes del cambio |
| `valor_nuevo` | `TEXT` | Valor después del cambio |
| `usuario` | `TEXT` | Email o nombre del usuario |
| `ejecutado_en` | `TIMESTAMPTZ` | `NOT NULL DEFAULT now()` |

**Triggers:**
- `trg_audit_manifiestos`: `AFTER UPDATE` — registra cada columna cambiada
- `trg_audit_manifiestos_delete`: `AFTER DELETE` — guarda snapshot JSON completo

---

## Vistas

### `v_manifiestos` (security_invoker)
Vista principal de consulta. Hereda RLS de la tabla base.
- Enriquece con `dias_cumplido` (CURRENT_DATE - fecha_cumplido)
- Enriquece con `fecha_estimada_pago` (días hábiles × 1.4)
- **Enmascara** `valor_factura` para roles no financieros

### `v_chatbot_manifiestos` (sin security_invoker)
Vista para el chatbot de WhatsApp. Expone solo datos operativos.
- Sin datos financieros internos (solo `flete_conductor`, `saldo`, `valor_pagado`)
- Acceso vía `service_role`

---

## RPCs de escritura

| Función | Rol | Columnas que modifica |
|---------|-----|----------------------|
| `guardar_digitador` | digitador, gerencia | A–Q (operación, vehículo, valores) |
| `guardar_digitador_batch` | digitador, gerencia | Como guardar_digitador pero sin sobreescribir conductor/propietario |
| `guardar_logistico` | logistico, digitador, tesoreria, gerencia | Cumplimiento, novedades, ajustes |
| `guardar_estado_interno` | financiero, administrativo, gerencia | Solo `estado_interno` + `responsable` |
| `guardar_tesoreria` | tesoreria, gerencia | Pago, valor_pagado, entidad, responsable |
| `guardar_financiero` | financiero, contadora, gerencia | Facturación |
| `borrar_manifiesto` | gerencia | Elimina el registro |

---

## Migraciones

| Archivo | Cambios |
|---------|---------|
| `migrations/002_restricciones_y_enums.sql` | ENUMs, CHECKs, rename `tipo_vehiculo` → `placa_remolque` |

---

## Convenciones de nombres

- **Columnas**: `snake_case` (ej: `fecha_despacho`, `valor_remesa`)
- **Parámetros RPC**: `p_snake_case` (ej: `p_fecha_despacho`)
- **Índices**: `idx_mflat_<columna>` (prefijo `mflat` = manifiestos_flat)
- **CHECKs**: `chk_<descripcion>` (ej: `chk_placa_formato`)
- **Triggers**: `trg_<descripcion>` (ej: `trg_audit_manifiestos`)
- **Funciones**: `fn_<descripcion>` (ej: `fn_audit_manifiestos`)
- **Vistas**: `v_<nombre>` (ej: `v_manifiestos`)