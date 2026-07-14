-- =============================================================================
-- ALTRANS S.A.S — SCHEMA CONSOLIDADO (v1.0 — Mayo 2026)
--
-- Este archivo contiene TODO el schema de producción, listo para ejecutar
-- en un proyecto Supabase vacío. Reemplaza:
--   - supabase/schema.sql
--   - supabase/migrations/002_nuevos_campos_y_audit.sql
--   - supabase/migrations/003_consolidacion_y_seguridad.sql
--   - supabase/migrations/004_security_warnings.sql
--   - supabase/migrations/005_get_usuarios.sql
--   - supabase/migrations/20260505_get_catalogos.sql
--   - supabase/migrations/20260505_novedad_flete_ajustes.sql
--   - supabase/migrations/20260505_security_hardening.sql
--
-- Estructura:
--   1. Limpieza
--   2. Tabla principal manifiestos_flat (con todas las columnas)
--   3. Índices
--   4. Tabla audit_log + trigger + RLS
--   5. Tablas chatbot (sesiones, idempotencia, jailbreak)
--   6. Vista v_manifiestos (security_invoker, enmascara valor_factura)
--   7. Función user_role
--   8. RPCs de lectura: consulta_manifiestos, consulta_totales, tendencia_anual
--   9. RPCs de escritura por rol (digitador, logistico, tesoreria, financiero)
--  10. RPCs gerencia: borrar_manifiesto, get_usuarios, get_catalogos
--  11. RLS sobre manifiestos_flat
--  12. Permisos finales (REVOKE PUBLIC + GRANT authenticated)
-- =============================================================================


-- ╔══════════════════════════════════════════════════════════════════════════╗
-- ║ 1. LIMPIEZA — eliminar objetos previos                                  ║
-- ╚══════════════════════════════════════════════════════════════════════════╝

DROP TRIGGER IF EXISTS trg_audit_manifiestos        ON public.manifiestos_flat;
DROP TRIGGER IF EXISTS trg_audit_manifiestos_delete ON public.manifiestos_flat;
DROP TRIGGER IF EXISTS trg_notify_plazo_vigente     ON public.manifiestos_flat;
DROP FUNCTION IF EXISTS public.fn_audit_manifiestos                CASCADE;
DROP FUNCTION IF EXISTS public.fn_audit_manifiestos_delete         CASCADE;
DROP FUNCTION IF EXISTS public.consulta_manifiestos                CASCADE;
DROP FUNCTION IF EXISTS public.consulta_totales                    CASCADE;
DROP FUNCTION IF EXISTS public.dashboard_kpis                      CASCADE;
DROP FUNCTION IF EXISTS public.tendencia_anual                     CASCADE;
DROP FUNCTION IF EXISTS public.get_pendientes_notificacion         CASCADE;
DROP FUNCTION IF EXISTS public.guardar_digitador                   CASCADE;
DROP FUNCTION IF EXISTS public.guardar_logistico                   CASCADE;
DROP FUNCTION IF EXISTS public.guardar_estado_interno              CASCADE;
DROP FUNCTION IF EXISTS public.guardar_tesoreria                   CASCADE;
DROP FUNCTION IF EXISTS public.guardar_financiero                  CASCADE;
DROP FUNCTION IF EXISTS public.borrar_manifiesto                   CASCADE;
DROP FUNCTION IF EXISTS public.get_usuarios                        CASCADE;
DROP FUNCTION IF EXISTS public.get_catalogos                       CASCADE;
DROP FUNCTION IF EXISTS public.user_role                           CASCADE;
DROP VIEW     IF EXISTS public.v_manifiestos                       CASCADE;
-- Tabla principal: DROP explícito para que el CREATE TABLE siempre recree
-- la estructura completa (columnas generadas, constraints). Sin esto,
-- IF NOT EXISTS la preserva con el esquema viejo y las columnas nuevas no se crean.
DROP TABLE    IF EXISTS public.messages_sent                       CASCADE;
DROP TABLE    IF EXISTS public.manifiestos_flat                    CASCADE;


-- ╔══════════════════════════════════════════════════════════════════════════╗
-- ║ 2. TABLA PRINCIPAL                                                       ║
-- ╚══════════════════════════════════════════════════════════════════════════╝

CREATE TABLE IF NOT EXISTS public.manifiestos_flat (

    -- ── Identificación ──────────────────────────────────────────────────────
    manifiesto                  BIGINT          PRIMARY KEY,

    -- ── Contexto del sheet de origen ────────────────────────────────────────
    archivo_origen              TEXT,
    mes                         TEXT,
    año                         SMALLINT,
    periodo                     DATE,
    semana                      TEXT,
    consecutivo_semanal         INTEGER,

    -- ── Operación ───────────────────────────────────────────────────────────
    fecha_despacho              DATE,
    origen                      TEXT,
    departamento_origen         TEXT,
    destino                     TEXT,
    departamento_destino        TEXT,
    cliente                     TEXT,
    remesas                     TEXT,           -- códigos separados por coma

    -- ── Financiero base ─────────────────────────────────────────────────────
    valor_remesa                NUMERIC(14, 2),
    flete_conductor             NUMERIC(14, 2),
    anticipo                    NUMERIC(14, 2),

    -- ── Vehículo y conductor ────────────────────────────────────────────────
    placa                       TEXT,
    tipo_vehiculo               TEXT,
    conductor                   TEXT,
    celular                     TEXT
        CHECK (celular IS NULL OR celular ~ '^\d{10}$'),
    cedula_conductor            TEXT,
    propietario                 TEXT,

    -- ── Despacho ────────────────────────────────────────────────────────────
    agencia_despachadora        TEXT,
    nombre_responsable          TEXT,

    -- ── Cumplimiento operativo ──────────────────────────────────────────────
    fecha_cumplido              DATE,
    compromiso_pago             TEXT            DEFAULT 'PAGO A 15 DIAS',
    novedades                   TEXT,
    novedad_conductor           TEXT,
    novedad_empresa             TEXT,
    estado_interno              TEXT,
    responsable_estado_interno  TEXT,

    -- ── Ajustes al flete ────────────────────────────────────────────────────
    ajuste_positivo_flete       NUMERIC(14, 2)  CHECK (ajuste_positivo_flete >= 0),
    ajuste_negativo_flete       NUMERIC(14, 2)  CHECK (ajuste_negativo_flete >= 0),
    consignacion_a_terceros     NUMERIC(14, 2),
    -- Retención en la fuente: siempre 1% del flete total (regla de gerencia,
    -- sin excepciones a hoy). Columna generada para que el saldo quede auditado
    -- en cada fila sin que el chatbot tenga que inferir la regla.
    retencion_conductor         NUMERIC(14, 2)
        GENERATED ALWAYS AS (
            CASE WHEN flete_conductor IS NOT NULL
                 THEN ROUND(flete_conductor * 0.01, 2)
            END
        ) STORED,
    -- Saldo del conductor = flete + ajuste_positivo - ajuste_negativo
    --                       - retención (1%) - anticipo.
    -- El anticipo se entrega ANTES de salir a ruta (obligatorio), por eso ya
    -- no forma parte del saldo: el saldo es lo que QUEDA por pagar al cumplido,
    -- a ~15 días hábiles. La conciliación del pago (saldo - valor_pagado) se
    -- calcula aparte contra valor_pagado.
    -- (Antes se llamaba flete_neto_conductor; renombrada a saldo por claridad.)
    saldo                       NUMERIC(14, 2)
        GENERATED ALWAYS AS (
            CASE WHEN flete_conductor IS NOT NULL
                 THEN flete_conductor
                      + COALESCE(ajuste_positivo_flete, 0)
                      - COALESCE(ajuste_negativo_flete, 0)
                      - ROUND(flete_conductor * 0.01, 2)
                      - COALESCE(anticipo, 0)
            END
        ) STORED,

    -- ── Pago al conductor ───────────────────────────────────────────────────
    fecha_pago                  DATE,
    valor_pagado                NUMERIC(14, 2),
    entidad_financiera          TEXT,
    responsable                 TEXT,

    -- ── Facturación ─────────────────────────────────────────────────────────
    factura_no                  TEXT,
    fecha_factura               DATE,
    factura_electronica         TEXT,
    mes_facturacion             SMALLINT,
    valor_factura               NUMERIC(14, 2),
    dias_para_facturar          INTEGER
        GENERATED ALWAYS AS (
            CASE WHEN fecha_factura IS NOT NULL AND fecha_despacho IS NOT NULL
                 THEN fecha_factura - fecha_despacho
            END
        ) STORED,

    -- ── Auditoría ───────────────────────────────────────────────────────────
    cargado_en                  TIMESTAMPTZ     NOT NULL DEFAULT now(),
    actualizado_en              TIMESTAMPTZ     NOT NULL DEFAULT now()
);


-- ╔══════════════════════════════════════════════════════════════════════════╗
-- ║ 3. ÍNDICES                                                               ║
-- ╚══════════════════════════════════════════════════════════════════════════╝

CREATE INDEX IF NOT EXISTS idx_mflat_fecha_despacho   ON public.manifiestos_flat (fecha_despacho);
CREATE INDEX IF NOT EXISTS idx_mflat_periodo          ON public.manifiestos_flat (periodo);
CREATE INDEX IF NOT EXISTS idx_mflat_año_mes          ON public.manifiestos_flat (año, mes);
CREATE INDEX IF NOT EXISTS idx_mflat_cliente          ON public.manifiestos_flat (cliente);
CREATE INDEX IF NOT EXISTS idx_mflat_conductor        ON public.manifiestos_flat (conductor);
CREATE INDEX IF NOT EXISTS idx_mflat_placa            ON public.manifiestos_flat (placa);
CREATE INDEX IF NOT EXISTS idx_mflat_agencia          ON public.manifiestos_flat (agencia_despachadora);
CREATE INDEX IF NOT EXISTS idx_mflat_archivo_origen   ON public.manifiestos_flat (archivo_origen);
CREATE INDEX IF NOT EXISTS idx_mflat_cedula           ON public.manifiestos_flat (cedula_conductor);
CREATE INDEX IF NOT EXISTS idx_mflat_estado_interno   ON public.manifiestos_flat (estado_interno);


-- ╔══════════════════════════════════════════════════════════════════════════╗
-- ║ 4. AUDIT LOG (cambios sobre manifiestos_flat)                            ║
-- ╚══════════════════════════════════════════════════════════════════════════╝

-- audit_log es append-only e independiente de manifiestos_flat:
-- registros de DELETE sobreviven al borrado del manifiesto correspondiente.
-- Por eso no hay FK (un FK con CASCADE borraría la auditoría del DELETE;
-- sin CASCADE bloquearía el borrado).
CREATE TABLE IF NOT EXISTS public.audit_log (
    id              BIGSERIAL   PRIMARY KEY,
    manifiesto      BIGINT      NOT NULL,
    campo           TEXT        NOT NULL,
    valor_anterior  TEXT,
    valor_nuevo     TEXT,
    usuario         TEXT,
    ejecutado_en    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Migración: si el FK existe (schemas previos), eliminarlo.
ALTER TABLE public.audit_log DROP CONSTRAINT IF EXISTS audit_log_manifiesto_fk;

CREATE INDEX IF NOT EXISTS audit_log_manifiesto_idx   ON public.audit_log (manifiesto);
CREATE INDEX IF NOT EXISTS audit_log_ejecutado_en_idx ON public.audit_log (ejecutado_en DESC);


CREATE OR REPLACE FUNCTION public.fn_audit_manifiestos()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
    col      text;
    v_old    text;
    v_new    text;
    v_claims json;
    v_usuario text;
BEGIN
    v_claims  := NULLIF(current_setting('request.jwt.claims', true), '')::json;
    v_usuario := COALESCE(
        v_claims->'user_metadata'->>'nombre',
        v_claims->>'email'
    );
    FOREACH col IN ARRAY ARRAY[
        'fecha_despacho','origen','destino','cliente','conductor','cedula_conductor',
        'celular','placa','tipo_vehiculo','propietario','agencia_despachadora',
        'nombre_responsable','valor_remesa','flete_conductor','anticipo','remesas',
        'fecha_cumplido','compromiso_pago','novedades','estado_interno',
        'responsable_estado_interno','novedad_conductor','novedad_empresa',
        'ajuste_positivo_flete','ajuste_negativo_flete','consignacion_a_terceros',
        'saldo','fecha_pago','valor_pagado','entidad_financiera',
        'responsable','factura_no','fecha_factura','factura_electronica',
        'mes_facturacion','valor_factura'
    ]
    LOOP
        EXECUTE format('SELECT ($1).%I::text', col) INTO v_old USING OLD;
        EXECUTE format('SELECT ($1).%I::text', col) INTO v_new USING NEW;
        IF v_old IS DISTINCT FROM v_new THEN
            INSERT INTO public.audit_log(manifiesto, campo, valor_anterior, valor_nuevo, usuario)
            VALUES (NEW.manifiesto, col, v_old, v_new, v_usuario);
        END IF;
    END LOOP;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_audit_manifiestos
    AFTER UPDATE ON public.manifiestos_flat
    FOR EACH ROW EXECUTE FUNCTION public.fn_audit_manifiestos();


-- ── Audit DELETE: registra un único renglón por borrado, con snapshot completo
-- de la fila eliminada en valor_anterior (JSON). campo = 'ELIMINADO'.
CREATE OR REPLACE FUNCTION public.fn_audit_manifiestos_delete()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
    v_claims  json;
    v_usuario text;
BEGIN
    v_claims  := NULLIF(current_setting('request.jwt.claims', true), '')::json;
    v_usuario := COALESCE(
        v_claims->'user_metadata'->>'nombre',
        v_claims->>'email'
    );
    INSERT INTO public.audit_log(manifiesto, campo, valor_anterior, valor_nuevo, usuario)
    VALUES (OLD.manifiesto, 'ELIMINADO', row_to_json(OLD)::text, NULL, v_usuario);
    RETURN OLD;
END;
$$;

CREATE TRIGGER trg_audit_manifiestos_delete
    AFTER DELETE ON public.manifiestos_flat
    FOR EACH ROW EXECUTE FUNCTION public.fn_audit_manifiestos_delete();


-- ── Trigger: auto-notificar saldo_plazo_vigente al marcar fecha_cumplido ─────
CREATE OR REPLACE FUNCTION public.fn_notify_plazo_vigente()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
BEGIN
    IF NEW.fecha_cumplido IS NOT NULL
       AND (OLD.fecha_cumplido IS NULL OR OLD.fecha_cumplido IS DISTINCT FROM NEW.fecha_cumplido)
       AND NEW.fecha_pago IS NULL
       AND NEW.estado_interno IS DISTINCT FROM 'ANULADO'
       AND NEW.celular ~ '^\d{10}$'
    THEN
        INSERT INTO public.messages_sent (manifiesto, template_name, phone, status)
        VALUES (NEW.manifiesto, 'saldo_plazo_vigente', NEW.celular, 'pending')
        ON CONFLICT DO NOTHING;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_notify_plazo_vigente
    AFTER UPDATE OF fecha_cumplido ON public.manifiestos_flat
    FOR EACH ROW EXECUTE FUNCTION public.fn_notify_plazo_vigente();


-- ── Trigger: notificar pago_realizado al marcar fecha_pago ─────────────────
CREATE OR REPLACE FUNCTION public.fn_notify_pago_realizado()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
BEGIN
    IF NEW.fecha_pago IS NOT NULL
       AND (OLD.fecha_pago IS NULL OR OLD.fecha_pago IS DISTINCT FROM NEW.fecha_pago)
       AND NEW.valor_pagado IS NOT NULL
       AND NEW.estado_interno IS DISTINCT FROM 'ANULADO'
       AND NEW.celular ~ '^\d{10}$'
    THEN
        INSERT INTO public.messages_sent (manifiesto, template_name, phone, status)
        VALUES (NEW.manifiesto, 'pago_realizado', NEW.celular, 'pending')
        ON CONFLICT DO NOTHING;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_notify_pago_realizado
    AFTER UPDATE OF fecha_pago ON public.manifiestos_flat
    FOR EACH ROW EXECUTE FUNCTION public.fn_notify_pago_realizado();


-- ╔══════════════════════════════════════════════════════════════════════════╗
-- ║ 5. TABLAS DEL CHATBOT                                                    ║
-- ╚══════════════════════════════════════════════════════════════════════════╝

-- Sesiones persistentes de WhatsApp
-- tipo_usuario distingue conductor (autentica con cédula) de propietario (con placa).
-- identificador_temp guarda la cédula o placa durante el flujo de autenticación;
-- identificador_auth es el valor verificado tras pasar manifiesto.
CREATE TABLE IF NOT EXISTS public.chatbot_sesiones (
    wa_from                TEXT PRIMARY KEY,
    estado                 TEXT        NOT NULL DEFAULT 'esperando_identificador',
    tipo_usuario           TEXT,                                 -- 'conductor' | 'propietario'
    identificador_temp     TEXT,                                 -- cédula o placa en flujo de auth
    identificador_auth     TEXT,                                 -- cédula o placa ya verificada
    nombre_temp            TEXT,
    nombre                 TEXT,
    -- Campos legacy: se mantienen para compatibilidad con sesiones previas.
    cedula_temp            TEXT,
    conductor_nombre_temp  TEXT,
    conductor_cedula       TEXT,
    conductor_nombre       TEXT,
    historial              JSONB       NOT NULL DEFAULT '[]'::jsonb,
    msg_count              INTEGER     NOT NULL DEFAULT 0,
    last_activity          TIMESTAMPTZ NOT NULL DEFAULT now(),
    auth_fails             INTEGER     NOT NULL DEFAULT 0,
    locked_until           TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_chatbot_last_activity ON public.chatbot_sesiones (last_activity);

-- Migración idempotente: agrega columnas nuevas si la tabla ya existía
-- desde una versión anterior del schema.
ALTER TABLE public.chatbot_sesiones ADD COLUMN IF NOT EXISTS tipo_usuario       TEXT;
ALTER TABLE public.chatbot_sesiones ADD COLUMN IF NOT EXISTS identificador_temp TEXT;
ALTER TABLE public.chatbot_sesiones ADD COLUMN IF NOT EXISTS identificador_auth TEXT;
ALTER TABLE public.chatbot_sesiones ADD COLUMN IF NOT EXISTS nombre_temp        TEXT;
ALTER TABLE public.chatbot_sesiones ADD COLUMN IF NOT EXISTS nombre             TEXT;

-- Idempotencia del webhook de Meta
CREATE TABLE IF NOT EXISTS public.processed_messages (
    message_id   TEXT        PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_processed_at ON public.processed_messages (processed_at);

-- Auditoría de intentos de jailbreak
CREATE TABLE IF NOT EXISTS public.jailbreak_log (
    id          BIGSERIAL   PRIMARY KEY,
    wa_from     TEXT,
    cedula      TEXT,
    mensaje     TEXT        NOT NULL,
    motivo      TEXT        NOT NULL,
    detectado_en TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_jb_detectado_en ON public.jailbreak_log (detectado_en DESC);


-- Control de reenvíos: registra cada mensaje automático enviado por manifiesto,
-- para no enviar la misma plantilla al mismo conductor repetidamente.
CREATE TABLE IF NOT EXISTS public.messages_sent (
    id              BIGSERIAL   PRIMARY KEY,
    manifiesto      BIGINT      NOT NULL,
    template_name   TEXT        NOT NULL,
    phone           TEXT,
    status          TEXT        NOT NULL DEFAULT 'sent',
    error           TEXT,
    sent_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_ms_manifiesto  ON public.messages_sent (manifiesto);
CREATE INDEX IF NOT EXISTS idx_ms_sent_at     ON public.messages_sent (sent_at DESC);
CREATE INDEX IF NOT EXISTS idx_ms_lookup      ON public.messages_sent (manifiesto, template_name, sent_at);
CREATE UNIQUE INDEX IF NOT EXISTS idx_ms_pending_dedup ON public.messages_sent (manifiesto, template_name) WHERE status = 'pending';


-- ╔══════════════════════════════════════════════════════════════════════════╗
-- ║ 6. FUNCIÓN user_role                                                     ║
-- ╚══════════════════════════════════════════════════════════════════════════╝

CREATE OR REPLACE FUNCTION public.user_role()
RETURNS TEXT
LANGUAGE sql STABLE
SET search_path = ''
AS $$
    SELECT COALESCE((auth.jwt() -> 'app_metadata' ->> 'role'), '')
$$;


-- ╔══════════════════════════════════════════════════════════════════════════╗
-- ║ 7. VISTA v_manifiestos                                                   ║
-- ║    - security_invoker: respeta el RLS del usuario que consulta.          ║
-- ║    - Enmascara valor_factura para no-financiero.                          ║
-- ║    - Expone dias_cumplido (calculado con CURRENT_DATE).                   ║
-- ╚══════════════════════════════════════════════════════════════════════════╝

CREATE VIEW public.v_manifiestos
WITH (security_invoker = true)
AS
SELECT
    m.manifiesto, m.archivo_origen, m.mes, m.año, m.periodo, m.semana,
    m.consecutivo_semanal, m.fecha_despacho, m.origen, m.departamento_origen,
    m.destino, m.departamento_destino, m.cliente, m.remesas, m.valor_remesa,
    m.flete_conductor, m.anticipo, m.placa, m.tipo_vehiculo, m.conductor,
    m.celular, m.cedula_conductor, m.propietario, m.agencia_despachadora,
    m.nombre_responsable, m.fecha_cumplido, m.compromiso_pago, m.novedades,
    m.novedad_conductor, m.novedad_empresa,
    m.ajuste_positivo_flete, m.ajuste_negativo_flete, m.consignacion_a_terceros,
    m.retencion_conductor, m.saldo,
    m.fecha_pago, m.valor_pagado, m.entidad_financiera, m.responsable,
    m.factura_no, m.fecha_factura, m.factura_electronica, m.mes_facturacion,
    CASE WHEN public.user_role() IN ('financiero', 'contadora', 'administrativo', 'gerencia')
         THEN m.valor_factura
         ELSE NULL
    END AS valor_factura,
    m.estado_interno, m.responsable_estado_interno,
    m.dias_para_facturar,
    m.cargado_en, m.actualizado_en,
    CASE WHEN m.fecha_cumplido IS NOT NULL
         THEN CURRENT_DATE - m.fecha_cumplido
    END AS dias_cumplido,
    -- ── Fecha estimada de pago ────────────────────────────────────────────────
    -- Aproximación días hábiles → calendario con factor ×1.4 (Julián 2026-05-23).
    -- NULL = no aplica (anulado, ya pagado, sin fecha_cumplido, o modalidad sin plazo).
    -- Casos sin valor numérico documentado (PRONTO PAGO, PRIORITARIO, OTROS, NULL):
    -- se usa 15 dh ≈ 21 cal como tentativo; el chatbot debe avisar al conductor.
    CASE
        WHEN m.fecha_cumplido IS NULL                THEN NULL
        WHEN m.fecha_pago     IS NOT NULL            THEN NULL
        WHEN m.estado_interno  = 'ANULADO'           THEN NULL
        WHEN m.compromiso_pago = 'URBANO'            THEN NULL
        WHEN m.compromiso_pago = 'ANULADO'           THEN NULL
        WHEN m.compromiso_pago = 'PAGADO'            THEN NULL
        ELSE m.fecha_cumplido + (CASE m.compromiso_pago
            WHEN 'PAGO A 15 DIAS'         THEN 21
            WHEN 'PAGO A 20 DIAS'         THEN 28
            WHEN 'PAGO A 30 DIAS'         THEN 42
            WHEN 'PAGO A 5-8 DIAS'        THEN 11
            WHEN 'PAGO INMEDIATO'         THEN 0
            WHEN 'CONTRAENTREGA'          THEN 0
            WHEN 'CONTINGENCIA 20-25 DH'  THEN 35
            ELSE 21  -- PRONTO PAGO, PRIORITARIO, OTROS, NULL (tentativo)
        END)
    END AS fecha_estimada_pago
FROM public.manifiestos_flat m;


-- ╔══════════════════════════════════════════════════════════════════════════╗
-- ║ 8. RPCs DE LECTURA                                                       ║
-- ╚══════════════════════════════════════════════════════════════════════════╝

-- ── consulta_manifiestos ────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.consulta_manifiestos(
    p_manifiesto        BIGINT   DEFAULT NULL,
    p_fecha_desde       DATE     DEFAULT NULL,
    p_fecha_hasta       DATE     DEFAULT NULL,
    p_conductor         TEXT     DEFAULT NULL,
    p_cliente           TEXT     DEFAULT NULL,
    p_origen            TEXT     DEFAULT NULL,
    p_destino           TEXT     DEFAULT NULL,
    p_placa             TEXT     DEFAULT NULL,
    p_agencia           TEXT     DEFAULT NULL,
    p_compromiso_pago   TEXT     DEFAULT NULL,
    p_estado_interno    TEXT     DEFAULT NULL,
    p_mes               TEXT     DEFAULT NULL,
    p_año               SMALLINT DEFAULT NULL,
    p_limit             INTEGER  DEFAULT 50,
    p_offset            INTEGER  DEFAULT 0
)
RETURNS SETOF public.v_manifiestos
LANGUAGE sql STABLE
SET search_path = ''
AS $$
    SELECT * FROM public.v_manifiestos
    WHERE (p_manifiesto      IS NULL OR manifiesto            = p_manifiesto)
      AND (p_fecha_desde     IS NULL OR fecha_despacho       >= p_fecha_desde)
      AND (p_fecha_hasta     IS NULL OR fecha_despacho       <= p_fecha_hasta)
      AND (p_conductor       IS NULL OR conductor       ILIKE '%' || p_conductor || '%')
      AND (p_cliente         IS NULL OR cliente         ILIKE '%' || p_cliente   || '%')
      AND (p_origen          IS NULL OR origen          ILIKE '%' || p_origen    || '%')
      AND (p_destino         IS NULL OR destino         ILIKE '%' || p_destino   || '%')
      AND (p_placa           IS NULL OR placa           ILIKE '%' || p_placa     || '%')
      AND (p_agencia         IS NULL OR agencia_despachadora = p_agencia)
      AND (p_compromiso_pago IS NULL OR compromiso_pago      = p_compromiso_pago)
      AND (p_estado_interno  IS NULL OR estado_interno       = p_estado_interno)
      AND (p_mes             IS NULL OR mes                  = p_mes)
      AND (p_año             IS NULL OR año                  = p_año)
    ORDER BY fecha_despacho DESC, manifiesto DESC
    LIMIT  p_limit
    OFFSET p_offset;
$$;


-- ── consulta_totales ────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.consulta_totales(
    p_fecha_desde       DATE     DEFAULT NULL,
    p_fecha_hasta       DATE     DEFAULT NULL,
    p_conductor         TEXT     DEFAULT NULL,
    p_cliente           TEXT     DEFAULT NULL,
    p_agencia           TEXT     DEFAULT NULL,
    p_compromiso_pago   TEXT     DEFAULT NULL,
    p_estado_interno    TEXT     DEFAULT NULL,
    p_mes               TEXT     DEFAULT NULL,
    p_año               SMALLINT DEFAULT NULL
)
RETURNS TABLE (
    total_manifiestos   BIGINT,
    suma_remesas        NUMERIC,
    suma_fletes         NUMERIC,
    suma_anticipos      NUMERIC,
    suma_pagado         NUMERIC,
    pendiente_pagar     NUMERIC
)
LANGUAGE sql STABLE
SET search_path = ''
AS $$
    SELECT
        COUNT(*)::BIGINT,
        COALESCE(SUM(valor_remesa),         0),
        COALESCE(SUM(flete_conductor),      0),
        COALESCE(SUM(anticipo),             0),
        COALESCE(SUM(valor_pagado),         0),
        COALESCE(SUM(saldo), 0) - COALESCE(SUM(valor_pagado), 0)
    FROM public.manifiestos_flat
    WHERE (p_fecha_desde     IS NULL OR fecha_despacho       >= p_fecha_desde)
      AND (p_fecha_hasta     IS NULL OR fecha_despacho       <= p_fecha_hasta)
      AND (p_conductor       IS NULL OR conductor       ILIKE '%' || p_conductor || '%')
      AND (p_cliente         IS NULL OR cliente         ILIKE '%' || p_cliente   || '%')
      AND (p_agencia         IS NULL OR agencia_despachadora = p_agencia)
      AND (p_compromiso_pago IS NULL OR compromiso_pago      = p_compromiso_pago)
      AND (p_estado_interno  IS NULL OR estado_interno       = p_estado_interno)
      AND (p_mes             IS NULL OR mes                  = p_mes)
      AND (p_año             IS NULL OR año                  = p_año);
$$;


-- ── tendencia_anual ─────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.tendencia_anual(p_año INTEGER DEFAULT NULL)
RETURNS TABLE (mes TEXT, facturado NUMERIC, ganancia NUMERIC)
LANGUAGE sql STABLE
SET search_path = ''
AS $$
    SELECT
        mes,
        COALESCE(SUM(valor_remesa),    0) AS facturado,
        COALESCE(SUM(flete_conductor), 0) AS ganancia
    FROM public.manifiestos_flat
    WHERE (p_año IS NULL OR año = p_año)
      AND mes IS NOT NULL
    GROUP BY mes
    ORDER BY MIN(fecha_despacho);
$$;


-- ── get_pendientes_notificacion ──────────────────────────────────────────────
-- Retorna manifiestos no pagados categorizados por el motivo de retención,
-- para que el servicio de notificaciones decida qué plantilla enviar.
-- Categorías: falta_factura, falta_documentacion, novedad_pendiente,
--             plazo_vigente, ya_notificado (si ya se le envió algo < 7 días).
--
-- Guardrail de novedades: valores cortos ≤3 chars (".", "ok", "si") o ruido
-- de clasificación ("TURBO", "URBANO", "TIPO VEHICULO", etc.) se ignoran
-- y caen a la siguiente categoría en orden de prioridad.
CREATE OR REPLACE FUNCTION public.get_pendientes_notificacion()
RETURNS TABLE (
    manifiesto      BIGINT,
    conductor       TEXT,
    celular         TEXT,
    template_name   TEXT,
    fecha_estimada  DATE,
    compromiso_pago TEXT,
    novedades       TEXT,
    factura_no      TEXT,
    fecha_cumplido  DATE,
    saldo           NUMERIC
)
LANGUAGE sql STABLE
SET search_path = ''
AS $$
    WITH base AS (
        SELECT *,
            CASE
                WHEN novedades IS NOT NULL
                     AND TRIM(novedades) != ''
                     AND LENGTH(TRIM(novedades)) > 3
                     AND NOT (
                         LENGTH(TRIM(novedades)) < 60
                         AND UPPER(TRIM(novedades)) ~ '(TIPO VEHICULO|TIPO VEHÍCULO|TURBO|URBANO|URBANOS)'
                     )
                THEN true
                ELSE false
            END AS es_novedad_real
        FROM public.manifiestos_flat
        WHERE fecha_pago IS NULL
          AND estado_interno IS DISTINCT FROM 'ANULADO'
          AND conductor IS NOT NULL
          AND celular IS NOT NULL
          AND celular ~ '^\d{10}$'
    ),
    notificados AS (
        SELECT manifiesto, template_name
        FROM public.messages_sent
        WHERE status = 'sent'
          AND sent_at > now() - INTERVAL '7 days'
    )
    SELECT
        b.manifiesto,
        b.conductor,
        b.celular,
        CASE
            WHEN b.es_novedad_real
                 THEN 'saldo_novedad_pendiente'
            WHEN b.factura_no IS NULL
                 THEN 'saldo_falta_factura'
            WHEN b.fecha_cumplido < CURRENT_DATE - 21
                 THEN 'saldo_falta_documentacion'
            WHEN b.fecha_cumplido IS NOT NULL
                 THEN 'saldo_plazo_vigente'
            ELSE 'saldo_falta_documentacion'
        END,
        CASE WHEN b.fecha_cumplido IS NOT NULL
             THEN (b.fecha_cumplido + CASE b.compromiso_pago
                 WHEN 'PAGO A 15 DIAS'         THEN 21
                 WHEN 'PAGO A 20 DIAS'         THEN 28
                 WHEN 'PAGO A 30 DIAS'         THEN 42
                 WHEN 'PAGO A 5-8 DIAS'        THEN 11
                 WHEN 'PAGO INMEDIATO'         THEN 0
                 WHEN 'CONTRAENTREGA'          THEN 0
                 WHEN 'CONTINGENCIA 20-25 DH'  THEN 35
                 ELSE 21
             END)::DATE
             ELSE NULL
        END,
        b.compromiso_pago,
        b.novedades,
        b.factura_no,
        b.fecha_cumplido,
        COALESCE(b.saldo, 0) - COALESCE(b.valor_pagado, 0)
    FROM base b
    WHERE NOT EXISTS (
        SELECT 1 FROM notificados n
        WHERE n.manifiesto = b.manifiesto
          AND n.template_name = CASE
              WHEN b.es_novedad_real
                   THEN 'saldo_novedad_pendiente'
              WHEN b.factura_no IS NULL
                   THEN 'saldo_falta_factura'
              WHEN b.fecha_cumplido < CURRENT_DATE - 21
                   THEN 'saldo_falta_documentacion'
              WHEN b.fecha_cumplido IS NOT NULL
                   THEN 'saldo_plazo_vigente'
              ELSE 'saldo_falta_documentacion'
          END
    )
    ORDER BY b.manifiesto;
$$;

-- ── dashboard_kpis ──────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.dashboard_kpis(p_mes TEXT DEFAULT NULL, p_año INTEGER DEFAULT NULL)
RETURNS JSON
LANGUAGE sql STABLE
SET search_path = ''
AS $$
    WITH base AS (
        SELECT * FROM public.manifiestos_flat
        WHERE (p_mes IS NULL OR mes = p_mes)
          AND (p_año IS NULL OR año = p_año)
    ),
    activos AS (
        SELECT * FROM base WHERE estado_interno IS DISTINCT FROM 'ANULADO'
    )
    SELECT json_build_object(
        'totalManifiestos',   (SELECT COUNT(*)            FROM base),
        'anulados',           (SELECT COUNT(*)            FROM base    WHERE estado_interno = 'ANULADO'),
        'conductoresActivos', (SELECT COUNT(DISTINCT conductor)        FROM activos WHERE conductor IS NOT NULL),
        'rutasActivas',       (SELECT COUNT(DISTINCT origen || '|' || destino) FROM activos WHERE origen IS NOT NULL AND destino IS NOT NULL),
        'totalRemesas',       (SELECT COALESCE(SUM(valor_remesa),    0) FROM activos),
        'totalFletes',        (SELECT COALESCE(SUM(flete_conductor), 0) FROM activos),
        'totalAnticipo',      (SELECT COALESCE(SUM(anticipo),        0) FROM activos),
        'pendientePagar',     (SELECT COALESCE(SUM(saldo), 0) - COALESCE(SUM(valor_pagado), 0) FROM activos WHERE fecha_pago IS NULL),
        'sinFechaCumplido',   (SELECT COUNT(*)            FROM activos WHERE fecha_cumplido IS NULL),
        'sinFactura',         (SELECT COUNT(*)            FROM activos WHERE factura_no IS NULL),
        'conNovedad',         (SELECT COUNT(*)            FROM activos WHERE novedades IS NOT NULL AND TRIM(novedades) != ''),
        'diasPromFacturar',   (SELECT COALESCE(ROUND(AVG(dias_para_facturar))::INT, 0) FROM activos WHERE dias_para_facturar IS NOT NULL),

        'topClientes',        (SELECT COALESCE(json_agg(sub ORDER BY sub.count DESC), '[]'::json)
                               FROM (SELECT cliente AS nombre, COUNT(*)::INT AS count
                                     FROM base WHERE cliente IS NOT NULL
                                     GROUP BY cliente) sub LIMIT 7),

        'topRutas',           (SELECT COALESCE(json_agg(sub ORDER BY sub.count DESC), '[]'::json)
                               FROM (SELECT origen || ' → ' || destino AS ruta, COUNT(*)::INT AS count
                                     FROM base WHERE origen IS NOT NULL AND destino IS NOT NULL
                                     GROUP BY origen, destino) sub LIMIT 7),

        'topConductores',     (SELECT COALESCE(json_agg(sub ORDER BY sub.count DESC), '[]'::json)
                               FROM (SELECT conductor AS nombre, COUNT(*)::INT AS count
                                     FROM base WHERE conductor IS NOT NULL
                                     GROUP BY conductor) sub LIMIT 7),

        'chartAgencias',      (SELECT COALESCE(json_agg(sub ORDER BY sub.count DESC), '[]'::json)
                               FROM (SELECT COALESCE(agencia_despachadora, 'SIN AGENCIA') AS nombre, COUNT(*)::INT AS count
                                     FROM base GROUP BY agencia_despachadora) sub),

        'chartEstadoInterno', (SELECT COALESCE(json_agg(sub ORDER BY sub.value DESC), '[]'::json)
                               FROM (SELECT COALESCE(estado_interno, 'SIN ESTADO') AS name, COUNT(*)::INT AS value
                                     FROM base GROUP BY estado_interno) sub),

        'estadoPago',         (SELECT COALESCE(json_agg(sub ORDER BY sub.value DESC), '[]'::json)
                               FROM (SELECT COALESCE(compromiso_pago, 'SIN ESTADO') AS name, COUNT(*)::INT AS value
                                     FROM base GROUP BY compromiso_pago) sub)
    );
$$;


-- ── get_catalogos ───────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.get_catalogos()
RETURNS JSON
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = ''
AS $$
    SELECT json_build_object(

        'conductores', (
            SELECT COALESCE(json_agg(c ORDER BY c.nombre), '[]'::json)
            FROM (
                SELECT DISTINCT ON (conductor)
                    conductor        AS nombre,
                    cedula_conductor AS cedula,
                    celular
                FROM public.manifiestos_flat
                WHERE conductor IS NOT NULL AND conductor <> ''
                ORDER BY conductor, actualizado_en DESC NULLS LAST
            ) c
        ),

        'clientes', (
            SELECT COALESCE(json_agg(nombre ORDER BY nombre), '[]'::json)
            FROM (
                SELECT DISTINCT cliente AS nombre
                FROM public.manifiestos_flat
                WHERE cliente IS NOT NULL AND cliente <> ''
            ) c
        ),

        'lugares', (
            SELECT COALESCE(json_agg(nombre ORDER BY nombre), '[]'::json)
            FROM (
                SELECT DISTINCT unnest(ARRAY[origen, destino]) AS nombre
                FROM public.manifiestos_flat
                WHERE origen IS NOT NULL OR destino IS NOT NULL
            ) l
            WHERE nombre IS NOT NULL AND nombre <> ''
        ),

        'responsables', (
            SELECT COALESCE(json_agg(nombre ORDER BY nombre), '[]'::json)
            FROM (
                SELECT DISTINCT unnest(
                    ARRAY[nombre_responsable, responsable, responsable_estado_interno]
                ) AS nombre
                FROM public.manifiestos_flat
            ) r
            WHERE nombre IS NOT NULL AND nombre <> ''
        ),

        'vehiculos', (
            SELECT COALESCE(json_agg(nombre ORDER BY nombre), '[]'::json)
            FROM (
                SELECT DISTINCT placa AS nombre
                FROM public.manifiestos_flat
                WHERE placa IS NOT NULL AND placa <> ''
            ) v
        ),

        'remolques', (
            SELECT COALESCE(json_agg(nombre ORDER BY nombre), '[]'::json)
            FROM (
                SELECT DISTINCT tipo_vehiculo AS nombre
                FROM public.manifiestos_flat
                WHERE tipo_vehiculo IS NOT NULL AND tipo_vehiculo <> ''
            ) r
        ),

        'agencias', (
            SELECT COALESCE(json_agg(nombre ORDER BY nombre), '[]'::json)
            FROM (
                SELECT DISTINCT agencia_despachadora AS nombre
                FROM public.manifiestos_flat
                WHERE agencia_despachadora IS NOT NULL AND agencia_despachadora <> ''
            ) a
        ),

        'propietarios', (
            SELECT COALESCE(json_agg(nombre ORDER BY nombre), '[]'::json)
            FROM (
                SELECT DISTINCT propietario AS nombre
                FROM public.manifiestos_flat
                WHERE propietario IS NOT NULL AND propietario <> ''
            ) p
        ),

        'compromisos_pago', (
            SELECT COALESCE(json_agg(nombre ORDER BY nombre), '[]'::json)
            FROM (
                SELECT DISTINCT compromiso_pago AS nombre
                FROM public.manifiestos_flat
                WHERE compromiso_pago IS NOT NULL AND compromiso_pago <> ''
            ) cp
        )

    )
$$;


-- ── get_usuarios (solo gerencia) ────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.get_usuarios()
RETURNS TABLE(email TEXT, rol TEXT)
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = ''
AS $$
BEGIN
    IF public.user_role() <> 'gerencia' THEN
        RAISE EXCEPTION 'Sin permiso';
    END IF;
    RETURN QUERY
        SELECT
            u.email::text,
            COALESCE(u.raw_app_meta_data->>'role', 'sin rol')::text
        FROM auth.users u
        ORDER BY u.email;
END;
$$;


-- ╔══════════════════════════════════════════════════════════════════════════╗
-- ║ 9. RPCs DE ESCRITURA POR ROL                                             ║
-- ║    Cada rol solo puede llamar su propia RPC. La tabla está bloqueada     ║
-- ║    para UPDATE directo (solo service_role).                              ║
-- ╚══════════════════════════════════════════════════════════════════════════╝

-- ── guardar_digitador (UPSERT — crea o actualiza columnas A–Q) ──────────────
CREATE OR REPLACE FUNCTION public.guardar_digitador(
    p_manifiesto            BIGINT,
    p_archivo_origen        TEXT     DEFAULT NULL,
    p_mes                   TEXT     DEFAULT NULL,
    p_año                   SMALLINT DEFAULT NULL,
    p_periodo               DATE     DEFAULT NULL,
    p_semana                TEXT     DEFAULT NULL,
    p_consecutivo_semanal   INTEGER  DEFAULT NULL,
    p_fecha_despacho        DATE     DEFAULT NULL,
    p_origen                TEXT     DEFAULT NULL,
    p_departamento_origen   TEXT     DEFAULT NULL,
    p_destino               TEXT     DEFAULT NULL,
    p_departamento_destino  TEXT     DEFAULT NULL,
    p_cliente               TEXT     DEFAULT NULL,
    p_remesas               TEXT     DEFAULT NULL,
    p_valor_remesa          NUMERIC  DEFAULT NULL,
    p_flete_conductor       NUMERIC  DEFAULT NULL,
    p_anticipo              NUMERIC  DEFAULT NULL,
    p_placa                 TEXT     DEFAULT NULL,
    p_tipo_vehiculo         TEXT     DEFAULT NULL,
    p_conductor             TEXT     DEFAULT NULL,
    p_celular               TEXT     DEFAULT NULL,
    p_cedula_conductor      TEXT     DEFAULT NULL,
    p_propietario           TEXT     DEFAULT NULL,
    p_agencia_despachadora  TEXT     DEFAULT NULL,
    p_nombre_responsable    TEXT     DEFAULT NULL
)
RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = ''
AS $$
BEGIN
    IF public.user_role() NOT IN ('digitador', 'gerencia') THEN
        RAISE EXCEPTION 'Sin permiso';
    END IF;
    INSERT INTO public.manifiestos_flat (
        manifiesto, archivo_origen, mes, año, periodo, semana, consecutivo_semanal,
        fecha_despacho, origen, departamento_origen, destino, departamento_destino,
        cliente, remesas, valor_remesa, flete_conductor, anticipo,
        placa, tipo_vehiculo, conductor, celular, cedula_conductor,
        propietario, agencia_despachadora, nombre_responsable
    ) VALUES (
        p_manifiesto, p_archivo_origen, p_mes, p_año, p_periodo, p_semana, p_consecutivo_semanal,
        p_fecha_despacho, p_origen, p_departamento_origen, p_destino, p_departamento_destino,
        p_cliente, p_remesas, p_valor_remesa, p_flete_conductor, p_anticipo,
        p_placa, p_tipo_vehiculo, p_conductor, p_celular, p_cedula_conductor,
        p_propietario, p_agencia_despachadora, p_nombre_responsable
    )
    ON CONFLICT (manifiesto) DO UPDATE SET
        archivo_origen        = COALESCE(EXCLUDED.archivo_origen,       public.manifiestos_flat.archivo_origen),
        mes                   = COALESCE(EXCLUDED.mes,                  public.manifiestos_flat.mes),
        año                   = COALESCE(EXCLUDED.año,                  public.manifiestos_flat.año),
        periodo               = COALESCE(EXCLUDED.periodo,              public.manifiestos_flat.periodo),
        semana                = COALESCE(EXCLUDED.semana,               public.manifiestos_flat.semana),
        consecutivo_semanal   = COALESCE(EXCLUDED.consecutivo_semanal,  public.manifiestos_flat.consecutivo_semanal),
        fecha_despacho        = COALESCE(EXCLUDED.fecha_despacho,       public.manifiestos_flat.fecha_despacho),
        origen                = COALESCE(EXCLUDED.origen,               public.manifiestos_flat.origen),
        departamento_origen   = COALESCE(EXCLUDED.departamento_origen,  public.manifiestos_flat.departamento_origen),
        destino               = COALESCE(EXCLUDED.destino,              public.manifiestos_flat.destino),
        departamento_destino  = COALESCE(EXCLUDED.departamento_destino, public.manifiestos_flat.departamento_destino),
        cliente               = COALESCE(EXCLUDED.cliente,              public.manifiestos_flat.cliente),
        remesas               = COALESCE(EXCLUDED.remesas,              public.manifiestos_flat.remesas),
        valor_remesa          = COALESCE(EXCLUDED.valor_remesa,         public.manifiestos_flat.valor_remesa),
        flete_conductor       = COALESCE(EXCLUDED.flete_conductor,      public.manifiestos_flat.flete_conductor),
        anticipo              = COALESCE(EXCLUDED.anticipo,             public.manifiestos_flat.anticipo),
        placa                 = COALESCE(EXCLUDED.placa,                public.manifiestos_flat.placa),
        tipo_vehiculo         = COALESCE(EXCLUDED.tipo_vehiculo,        public.manifiestos_flat.tipo_vehiculo),
        conductor             = COALESCE(EXCLUDED.conductor,            public.manifiestos_flat.conductor),
        celular               = COALESCE(EXCLUDED.celular,              public.manifiestos_flat.celular),
        cedula_conductor      = COALESCE(EXCLUDED.cedula_conductor,     public.manifiestos_flat.cedula_conductor),
        propietario           = COALESCE(EXCLUDED.propietario,          public.manifiestos_flat.propietario),
        agencia_despachadora  = COALESCE(EXCLUDED.agencia_despachadora, public.manifiestos_flat.agencia_despachadora),
        nombre_responsable    = COALESCE(EXCLUDED.nombre_responsable,   public.manifiestos_flat.nombre_responsable),
        actualizado_en        = now();
END;
$$;


-- ── guardar_logistico ───────────────────────────────────────────────────────
-- Acceso: logistico (R-W completo), digitador (R-W completo), tesoreria (solo
-- cols R-W del Drive: fecha_cumplido, compromiso_pago, novedades,
-- estado_interno, responsable_estado_interno), gerencia.
-- Tesorería NO puede tocar campos extra que solo aplican a logístico:
-- novedad_conductor, novedad_empresa, ajustes al flete, consignación a terceros.
-- Esos campos se preservan cuando el caller es tesoreria, ignorando lo que envíe.
-- Roles financiero/administrativo NO van acá: el Drive los limita a editar solo
-- estado_interno — usan guardar_estado_interno() abajo.
-- NULLIF en campos de texto libre: enviar "" desde el frontend equivale a NULL,
-- evita filas espurias en audit_log cuando el usuario abre y guarda sin cambios.
CREATE OR REPLACE FUNCTION public.guardar_logistico(
    p_manifiesto                 BIGINT,
    p_fecha_cumplido             DATE    DEFAULT NULL,
    p_compromiso_pago            TEXT    DEFAULT NULL,
    p_novedades                  TEXT    DEFAULT NULL,
    p_estado_interno             TEXT    DEFAULT NULL,
    p_responsable_estado_interno TEXT    DEFAULT NULL,
    p_novedad_conductor          TEXT    DEFAULT NULL,
    p_novedad_empresa            TEXT    DEFAULT NULL,
    p_ajuste_positivo_flete      NUMERIC DEFAULT NULL,
    p_ajuste_negativo_flete      NUMERIC DEFAULT NULL,
    p_consignacion_a_terceros    NUMERIC DEFAULT NULL
)
RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
    v_es_tesoreria BOOLEAN := public.user_role() = 'tesoreria';
BEGIN
    IF public.user_role() NOT IN ('logistico', 'digitador', 'tesoreria', 'gerencia') THEN
        RAISE EXCEPTION 'Sin permiso';
    END IF;
    UPDATE public.manifiestos_flat SET
        fecha_cumplido             = COALESCE(p_fecha_cumplido,             fecha_cumplido),
        compromiso_pago            = COALESCE(p_compromiso_pago,            compromiso_pago),
        novedades                  = NULLIF(p_novedades,         ''),
        estado_interno             = COALESCE(p_estado_interno,             estado_interno),
        responsable_estado_interno = COALESCE(p_responsable_estado_interno, responsable_estado_interno),
        novedad_conductor          = CASE WHEN v_es_tesoreria THEN novedad_conductor       ELSE NULLIF(p_novedad_conductor, '') END,
        novedad_empresa            = CASE WHEN v_es_tesoreria THEN novedad_empresa         ELSE NULLIF(p_novedad_empresa,   '') END,
        ajuste_positivo_flete      = CASE WHEN v_es_tesoreria THEN ajuste_positivo_flete   ELSE p_ajuste_positivo_flete         END,
        ajuste_negativo_flete      = CASE WHEN v_es_tesoreria THEN ajuste_negativo_flete   ELSE p_ajuste_negativo_flete         END,
        consignacion_a_terceros    = CASE WHEN v_es_tesoreria THEN consignacion_a_terceros ELSE p_consignacion_a_terceros       END,
        actualizado_en             = now()
    WHERE manifiesto = p_manifiesto;
END;
$$;


-- ── guardar_estado_interno ──────────────────────────────────────────────────
-- Acceso: financiero (Maria Elena), administrativo (Oscar), + cualquier rol que
-- ya pueda escribir cumplimiento completo. Solo toca estado_interno y su
-- responsable — el resto de la tab cumplimiento queda intacta.
-- Per USUARIOS DRIVE: financiero y administrativo solo modifican estado_interno;
-- guardar_logistico no los autoriza para evitar que pisen novedades/ajustes.
CREATE OR REPLACE FUNCTION public.guardar_estado_interno(
    p_manifiesto                 BIGINT,
    p_estado_interno             TEXT,
    p_responsable_estado_interno TEXT DEFAULT NULL
)
RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = ''
AS $$
BEGIN
    IF public.user_role() NOT IN ('financiero', 'administrativo', 'logistico', 'digitador', 'tesoreria', 'gerencia') THEN
        RAISE EXCEPTION 'Sin permiso';
    END IF;
    UPDATE public.manifiestos_flat SET
        estado_interno             = COALESCE(p_estado_interno,             estado_interno),
        responsable_estado_interno = COALESCE(p_responsable_estado_interno, responsable_estado_interno),
        actualizado_en             = now()
    WHERE manifiesto = p_manifiesto;
END;
$$;


-- ── guardar_tesoreria ───────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.guardar_tesoreria(
    p_manifiesto         BIGINT,
    p_fecha_pago         DATE    DEFAULT NULL,
    p_valor_pagado       NUMERIC DEFAULT NULL,
    p_entidad_financiera TEXT    DEFAULT NULL,
    p_responsable        TEXT    DEFAULT NULL
)
RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = ''
AS $$
BEGIN
    IF public.user_role() NOT IN ('tesoreria', 'contadora', 'gerencia') THEN
        RAISE EXCEPTION 'Sin permiso';
    END IF;
    UPDATE public.manifiestos_flat SET
        fecha_pago          = COALESCE(p_fecha_pago,         fecha_pago),
        valor_pagado        = COALESCE(p_valor_pagado,       valor_pagado),
        entidad_financiera  = COALESCE(p_entidad_financiera, entidad_financiera),
        responsable         = COALESCE(p_responsable,        responsable),
        actualizado_en      = now()
    WHERE manifiesto = p_manifiesto;
END;
$$;


-- ── guardar_financiero ──────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.guardar_financiero(
    p_manifiesto          BIGINT,
    p_factura_no          TEXT     DEFAULT NULL,
    p_fecha_factura       DATE     DEFAULT NULL,
    p_factura_electronica TEXT     DEFAULT NULL,
    p_mes_facturacion     SMALLINT DEFAULT NULL,
    p_valor_factura       NUMERIC  DEFAULT NULL
)
RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = ''
AS $$
BEGIN
    IF public.user_role() NOT IN ('financiero', 'contadora', 'gerencia') THEN
        RAISE EXCEPTION 'Sin permiso';
    END IF;
    UPDATE public.manifiestos_flat SET
        factura_no          = COALESCE(p_factura_no,          factura_no),
        fecha_factura       = COALESCE(p_fecha_factura,       fecha_factura),
        factura_electronica = COALESCE(p_factura_electronica, factura_electronica),
        mes_facturacion     = COALESCE(p_mes_facturacion,     mes_facturacion),
        valor_factura       = COALESCE(p_valor_factura,       valor_factura),
        actualizado_en      = now()
    WHERE manifiesto = p_manifiesto;
END;
$$;


-- ── borrar_manifiesto (solo gerencia) ───────────────────────────────────────
CREATE OR REPLACE FUNCTION public.borrar_manifiesto(p_manifiesto BIGINT)
RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = ''
AS $$
BEGIN
    IF public.user_role() <> 'gerencia' THEN
        RAISE EXCEPTION 'Sin permiso';
    END IF;
    DELETE FROM public.manifiestos_flat WHERE manifiesto = p_manifiesto;
END;
$$;


-- ╔══════════════════════════════════════════════════════════════════════════╗
-- ║ 10. ROW LEVEL SECURITY                                                   ║
-- ╚══════════════════════════════════════════════════════════════════════════╝

-- ── manifiestos_flat: lectura autenticados, escritura solo service_role ─────
ALTER TABLE public.manifiestos_flat ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "lectura_autenticados"    ON public.manifiestos_flat;
DROP POLICY IF EXISTS "escritura_service_role"  ON public.manifiestos_flat;

CREATE POLICY "lectura_autenticados"
    ON public.manifiestos_flat FOR SELECT
    USING (auth.role() = 'authenticated');

CREATE POLICY "escritura_service_role"
    ON public.manifiestos_flat FOR ALL
    USING      (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');


-- ── audit_log: solo gerencia lee; escritura solo vía trigger ────────────────
ALTER TABLE public.audit_log ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS audit_log_admin_select ON public.audit_log;
DROP POLICY IF EXISTS audit_log_no_writes   ON public.audit_log;

CREATE POLICY audit_log_admin_select ON public.audit_log
    FOR SELECT TO authenticated
    USING (public.user_role() = 'gerencia');

CREATE POLICY audit_log_no_writes ON public.audit_log
    FOR ALL TO authenticated
    USING (false) WITH CHECK (false);


-- ── chatbot_sesiones / processed_messages: solo service_role ────────────────
ALTER TABLE public.chatbot_sesiones    ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.processed_messages  ENABLE ROW LEVEL SECURITY;


-- ── jailbreak_log: solo gerencia lee ────────────────────────────────────────
ALTER TABLE public.jailbreak_log ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS jb_admin_select ON public.jailbreak_log;
CREATE POLICY jb_admin_select ON public.jailbreak_log
    FOR SELECT TO authenticated
    USING (public.user_role() = 'gerencia');


-- ── messages_sent: solo gerencia lee; escritura vía trigger SECURITY DEFINER
-- o service_role (bypassea RLS automáticamente) ──────────────────────────────
ALTER TABLE public.messages_sent ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS ms_gerencia_select ON public.messages_sent;
CREATE POLICY ms_gerencia_select ON public.messages_sent
    FOR SELECT TO authenticated
    USING (public.user_role() = 'gerencia');


-- ╔══════════════════════════════════════════════════════════════════════════╗
-- ║ 11. GRANTS                                                               ║
-- ║    PostgreSQL otorga EXECUTE a PUBLIC por defecto en CREATE FUNCTION,    ║
-- ║    y PUBLIC incluye a `anon`. Hay que REVOKE FROM PUBLIC y GRANT a       ║
-- ║    authenticated explícitamente.                                          ║
-- ╚══════════════════════════════════════════════════════════════════════════╝

-- Schema
GRANT USAGE ON SCHEMA public TO anon, authenticated;

-- Tabla y vista (solo lectura para authenticated)
GRANT SELECT ON public.manifiestos_flat TO authenticated;
GRANT SELECT ON public.v_manifiestos    TO authenticated;
GRANT ALL    ON public.manifiestos_flat TO postgres;  -- RPCs SECURITY DEFINER lo necesitan

-- Audit / chatbot / jailbreak: bloquear escritura a authenticated y anon
REVOKE INSERT, UPDATE, DELETE ON public.audit_log          FROM authenticated, anon, PUBLIC;
REVOKE ALL                    ON public.chatbot_sesiones   FROM PUBLIC, anon, authenticated;
REVOKE ALL                    ON public.processed_messages FROM PUBLIC, anon, authenticated;
REVOKE INSERT, UPDATE, DELETE ON public.jailbreak_log      FROM authenticated, anon, PUBLIC;
REVOKE ALL                    ON public.messages_sent      FROM PUBLIC, anon, authenticated;
GRANT SELECT ON public.messages_sent TO authenticated;
GRANT ALL    ON public.messages_sent TO postgres;

-- Revoke EXECUTE de PUBLIC en TODAS las funciones (defaults son inseguros)
REVOKE EXECUTE ON FUNCTION public.consulta_manifiestos(BIGINT, DATE, DATE, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, SMALLINT, INTEGER, INTEGER) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.consulta_totales(DATE, DATE, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, SMALLINT)                                                  FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.dashboard_kpis(TEXT, INTEGER)                                                                                              FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.tendencia_anual(INTEGER)                                                                                                    FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.get_catalogos()                                                                                                             FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.get_pendientes_notificacion()                                                                                               FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.fn_notify_plazo_vigente()                                                                                                   FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.fn_notify_pago_realizado()                                                                                                   FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.get_usuarios()                                                                                                              FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.guardar_digitador(BIGINT, TEXT, TEXT, SMALLINT, DATE, TEXT, INTEGER, DATE, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, NUMERIC, NUMERIC, NUMERIC, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.guardar_logistico(BIGINT, DATE, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, NUMERIC, NUMERIC, NUMERIC)                              FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.guardar_estado_interno(BIGINT, TEXT, TEXT)                                                                                  FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.guardar_tesoreria(BIGINT, DATE, NUMERIC, TEXT, TEXT)                                                                        FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.guardar_financiero(BIGINT, TEXT, DATE, TEXT, SMALLINT, NUMERIC)                                                             FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.borrar_manifiesto(BIGINT)                                                                                                   FROM PUBLIC;

-- Otorgar a authenticated
GRANT EXECUTE ON FUNCTION public.consulta_manifiestos(BIGINT, DATE, DATE, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, SMALLINT, INTEGER, INTEGER) TO authenticated;
GRANT EXECUTE ON FUNCTION public.consulta_totales(DATE, DATE, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, SMALLINT)                                                  TO authenticated;
GRANT EXECUTE ON FUNCTION public.dashboard_kpis(TEXT, INTEGER)                                                                                              TO authenticated;
GRANT EXECUTE ON FUNCTION public.tendencia_anual(INTEGER)                                                                                                    TO authenticated;
GRANT EXECUTE ON FUNCTION public.get_catalogos()                                                                                                             TO authenticated;
GRANT EXECUTE ON FUNCTION public.get_pendientes_notificacion()                                                                                               TO authenticated;
GRANT EXECUTE ON FUNCTION public.get_usuarios()                                                                                                              TO authenticated;
GRANT EXECUTE ON FUNCTION public.guardar_digitador(BIGINT, TEXT, TEXT, SMALLINT, DATE, TEXT, INTEGER, DATE, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, NUMERIC, NUMERIC, NUMERIC, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT) TO authenticated;
GRANT EXECUTE ON FUNCTION public.guardar_logistico(BIGINT, DATE, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, NUMERIC, NUMERIC, NUMERIC)                              TO authenticated;
GRANT EXECUTE ON FUNCTION public.guardar_estado_interno(BIGINT, TEXT, TEXT)                                                                                  TO authenticated;
GRANT EXECUTE ON FUNCTION public.guardar_tesoreria(BIGINT, DATE, NUMERIC, TEXT, TEXT)                                                                        TO authenticated;
GRANT EXECUTE ON FUNCTION public.guardar_financiero(BIGINT, TEXT, DATE, TEXT, SMALLINT, NUMERIC)                                                             TO authenticated;
GRANT EXECUTE ON FUNCTION public.borrar_manifiesto(BIGINT)                                                                                                   TO authenticated;


-- ╔══════════════════════════════════════════════════════════════════════════╗
-- ║ 12. GESTIÓN DE USUARIOS                                                  ║
-- ║                                                                          ║
-- ║  MÉTODO PREFERIDO: usar make seed-users (etl_individual/seed_users.py)  ║
-- ║  que gestiona creación, actualización y listado de forma idempotente.    ║
-- ║                                                                          ║
-- ║  Estructura de metadata por usuario:                                     ║
-- ║    app_metadata  → { "role": "<ROL>" }          (usado por RLS/RPCs)    ║
-- ║    user_metadata → { "nombre": "...",            (solo display)          ║
-- ║                      "cedula": "...",                                    ║
-- ║                      "cargo":  "..." }                                   ║
-- ║                                                                          ║
-- ║  Roles y columnas del Drive (USUARIOS DRIVE PRODUCCION ALTRANS.xlsx):    ║
-- ║    gerencia       — A–AE completo + eliminar + dashboard KPIs            ║
-- ║    digitador      — A–Q (despacho base) + R–W (cumplimiento) + Excel     ║
-- ║    logistico      — R–W (cumplimiento) · sin A–Q · sin Excel             ║
-- ║    tesoreria      — R–W (cumplimiento) + X–AA (pago conductor)           ║
-- ║    financiero     — V (estado interno) + AB–AE (facturación) + dashboard ║
-- ║    contadora      — X–AA (pago) + AB–AE (facturación)  (pendiente)      ║
-- ║    administrativo — V (estado interno) + dashboard       (pendiente)     ║
-- ║                                                                          ║
-- ║  Si se necesita asignar/corregir un rol manualmente en el SQL Editor:   ║
-- ║                                                                          ║
-- ║    UPDATE auth.users                                                     ║
-- ║    SET raw_app_meta_data =                                               ║
-- ║        COALESCE(raw_app_meta_data, '{}'::jsonb) ||                       ║
-- ║        jsonb_build_object('role', '<ROL>')                               ║
-- ║    WHERE email = '<cedula>@altrans.internal';                            ║
-- ║                                                                          ║
-- ║  Para ver todos los usuarios y sus roles actuales:                       ║
-- ║    SELECT email,                                                         ║
-- ║           raw_app_meta_data->>'role'   AS rol,                           ║
-- ║           raw_user_meta_data->>'nombre' AS nombre                        ║
-- ║    FROM auth.users ORDER BY email;                                       ║
-- ╚══════════════════════════════════════════════════════════════════════════╝
