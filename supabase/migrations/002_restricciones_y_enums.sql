-- 002_restricciones_y_enums.sql
-- Add ENUM types, CHECK constraints, and rename tipo_vehiculo → placa_remolque
-- Apply after schema_consolidated.sql against an existing database.

-- ══════════════════════════════════════════════════════════════════════════════
-- 1. ENUM TYPES (for categorical columns with bounded value sets)
-- ══════════════════════════════════════════════════════════════════════════════

DO $$ BEGIN
    CREATE TYPE compromiso_pago_enum AS ENUM (
        'PAGO A 15 DIAS', 'PAGO A 20 DIAS', 'PAGO A 30 DIAS', 'PAGO A 5-8 DIAS',
        'CONTRAENTREGA', 'PRONTO PAGO', 'PAGO NORMAL', 'URBANO', 'ANULADO',
        'PAGADO', 'PAGO INMEDIATO', 'PRIORITARIO', 'RNDC', 'OTROS',
        'CONTINGENCIA 20-25 DH'
    );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE estado_interno_enum AS ENUM (
        'CUMPLIDO', 'NO SE HA CUMPLIDO', 'ANULADO',
        'PENDIENTE FACTURA ELECTRONICA', 'NOVEDAD PENDIENTE', 'FACTURA RECIBIDA'
    );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE agencia_enum AS ENUM (
        'CALI', 'BOGOTA', 'IPIALES', 'BUENAVENTURA', 'ANULADO'
    );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE entidad_financiera_enum AS ENUM (
        'TRANSF BANCOLOMBIA', 'TRANSF DAVIVIENDA', 'TRANSF BANCO DE BOGOTA',
        'CHEQUE BANCOLOMBIA', 'CHEQUE DAVIVIENDA', 'CHEQUE BANCO DE BOGOTA',
        'CHEQUE', 'TRANSF/CHEQUE', 'ANULADO', 'OTRO'
    );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE responsable_enum AS ENUM (
        'KAROL ARCINIEGAS', 'JOHANA UNIGARRO', 'ELIZABETH SUAREZ',
        'MILENA GUTIERREZ', 'MARIAE', 'FLOTA PROPIA', 'FP', 'ANULADO'
    );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE mes_enum AS ENUM (
        'ENERO','FEBRERO','MARZO','ABRIL','MAYO','JUNIO',
        'JULIO','AGOSTO','SEPTIEMBRE','OCTUBRE','NOVIEMBRE','DICIEMBRE'
    );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- ══════════════════════════════════════════════════════════════════════════════
-- 2. RENAME COLUMN tipo_vehiculo → placa_remolque
-- ══════════════════════════════════════════════════════════════════════════════

-- Drop dependent objects first (views, RPCs, triggers reference the column)
DROP VIEW IF EXISTS public.v_chatbot_manifiestos CASCADE;
DROP VIEW IF EXISTS public.v_manifiestos CASCADE;

DROP FUNCTION IF EXISTS public.guardar_digitador CASCADE;
DROP FUNCTION IF EXISTS public.guardar_digitador_batch CASCADE;
DROP FUNCTION IF EXISTS public.get_catalogos CASCADE;

ALTER TABLE public.manifiestos_flat RENAME COLUMN tipo_vehiculo TO placa_remolque;

-- ══════════════════════════════════════════════════════════════════════════════
-- 3. CHECK CONSTRAINTS (data quality)
-- ══════════════════════════════════════════════════════════════════════════════

ALTER TABLE public.manifiestos_flat
    ADD CONSTRAINT chk_placa_formato
        CHECK (placa IS NULL OR placa ~ '^[A-Z0-9]{4,7}$' OR placa IN ('ANULADO', 'CONS ANULADO')),
    ADD CONSTRAINT chk_cedula_conductor_formato
        CHECK (cedula_conductor IS NULL OR cedula_conductor ~ '^\d{6,10}$'),
    ADD CONSTRAINT chk_año_rango
        CHECK (año IS NULL OR (año >= 2023 AND año <= 2026)),
    ADD CONSTRAINT chk_semana_formato
        CHECK (semana IS NULL OR semana ~ '^Semana \d+$'),
    ADD CONSTRAINT chk_valor_remesa_positivo
        CHECK (valor_remesa IS NULL OR valor_remesa >= 0),
    ADD CONSTRAINT chk_flete_conductor_positivo
        CHECK (flete_conductor IS NULL OR flete_conductor >= 0),
    ADD CONSTRAINT chk_anticipo_positivo
        CHECK (anticipo IS NULL OR anticipo >= 0),
    ADD CONSTRAINT chk_valor_pagado_positivo
        CHECK (valor_pagado IS NULL OR valor_pagado >= 0),
    ADD CONSTRAINT chk_valor_factura_positivo
        CHECK (valor_factura IS NULL OR valor_factura >= 0);

-- ══════════════════════════════════════════════════════════════════════════════
-- 4. CHANGE COLUMN TYPES (TEXT → ENUM via domain cast)
-- ══════════════════════════════════════════════════════════════════════════════

-- Note: We keep TEXT columns but add CHECK constraints referencing the ENUM.
-- This avoids migration complexity with existing data that may have edge values.

ALTER TABLE public.manifiestos_flat
    ADD CONSTRAINT chk_compromiso_pago_valido
        CHECK (compromiso_pago IS NULL OR compromiso_pago::compromiso_pago_enum IS NOT NULL),
    ADD CONSTRAINT chk_estado_interno_valido
        CHECK (estado_interno IS NULL OR estado_interno::estado_interno_enum IS NOT NULL),
    ADD CONSTRAINT chk_agencia_despachadora_valida
        CHECK (agencia_despachadora IS NULL OR agencia_despachadora::agencia_enum IS NOT NULL),
    ADD CONSTRAINT chk_entidad_financiera_valida
        CHECK (entidad_financiera IS NULL OR entidad_financiera::entidad_financiera_enum IS NOT NULL),
    ADD CONSTRAINT chk_responsable_valido
        CHECK (responsable IS NULL OR responsable::responsable_enum IS NOT NULL),
    ADD CONSTRAINT chk_mes_valido
        CHECK (mes IS NULL OR mes::mes_enum IS NOT NULL),
    ADD CONSTRAINT chk_nombre_responsable_valido
        CHECK (nombre_responsable IS NULL OR nombre_responsable IN (
            'ANGELA G', 'ANGIE', 'ANGIE OVIEDO', 'ANULADO', 'BUENAVENTURA',
            'DAVID', 'DIANA G.', 'ELIANA', 'HAIR', 'HECTOR', 'HOJASDEVIDA1',
            'INGRID VANESSA', 'JULIAN', 'KAROL', 'KATTY', 'LILIANA',
            'LILIANA OBREGON', 'LOGISTICACALI2', 'MARCELA', 'OPERATIVO 1',
            'OPERATIVO 2', 'OPERATIVO 3', 'OPERATIVO BUENA', 'RNDC',
            'VANESSA', 'YANETH F', 'YURANY ESTUPINA'
        ));

-- ══════════════════════════════════════════════════════════════════════════════
-- 5. RECREATE v_manifiestos (with placa_remolque instead of tipo_vehiculo)
-- ══════════════════════════════════════════════════════════════════════════════

CREATE VIEW public.v_manifiestos
WITH (security_invoker = true)
AS
SELECT
    m.manifiesto, m.archivo_origen, m.mes, m.año, m.periodo, m.semana,
    m.consecutivo_semanal, m.fecha_despacho, m.origen, m.departamento_origen,
    m.destino, m.departamento_destino, m.cliente, m.remesas, m.valor_remesa,
    m.flete_conductor, m.anticipo, m.placa, m.placa_remolque, m.conductor,
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
            ELSE 21
        END)
    END AS fecha_estimada_pago
FROM public.manifiestos_flat m;

-- ══════════════════════════════════════════════════════════════════════════════
-- 6. RECREATE v_chatbot_manifiestos (with placa_remolque instead of tipo_vehiculo)
-- ══════════════════════════════════════════════════════════════════════════════

CREATE VIEW public.v_chatbot_manifiestos
AS
SELECT
    m.manifiesto, m.fecha_despacho,
    m.origen, m.departamento_origen, m.destino, m.departamento_destino,
    m.cliente,
    m.placa, m.placa_remolque, m.conductor, m.celular, m.cedula_conductor, m.propietario,
    m.flete_conductor, m.saldo, m.valor_pagado, m.fecha_pago,
    m.fecha_cumplido, m.compromiso_pago, m.estado_interno,
    m.novedades, m.novedad_conductor,
    m.mes, m.año,
    CASE WHEN m.fecha_cumplido IS NOT NULL
         THEN CURRENT_DATE - m.fecha_cumplido
    END AS dias_cumplido,
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
            ELSE 21
        END)
    END AS fecha_estimada_pago
FROM public.manifiestos_flat m;

-- ══════════════════════════════════════════════════════════════════════════════
-- 7. RECREATE guardar_digitador (with placa_remolque instead of tipo_vehiculo)
-- ══════════════════════════════════════════════════════════════════════════════

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
    p_placa_remolque              TEXT     DEFAULT NULL,
    p_conductor             TEXT     DEFAULT NULL,
    p_celular               TEXT     DEFAULT NULL,
    p_cedula_conductor      TEXT     DEFAULT NULL,
    p_propietario           TEXT     DEFAULT NULL,
    p_agencia_despachadora  TEXT     DEFAULT NULL,
    p_nombre_responsable    TEXT     DEFAULT NULL,
    p_reteica               NUMERIC  DEFAULT NULL,
    p_r_fopat               NUMERIC  DEFAULT NULL
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
        placa, placa_remolque, conductor, celular, cedula_conductor,
        propietario, agencia_despachadora, nombre_responsable,
        reteica, r_fopat
    ) VALUES (
        p_manifiesto, p_archivo_origen, p_mes, p_año, p_periodo, p_semana, p_consecutivo_semanal,
        p_fecha_despacho, p_origen, p_departamento_origen, p_destino, p_departamento_destino,
        p_cliente, p_remesas, p_valor_remesa, p_flete_conductor, p_anticipo,
        p_placa, p_placa_remolque, p_conductor, p_celular, p_cedula_conductor,
        p_propietario, p_agencia_despachadora, p_nombre_responsable,
        p_reteica, p_r_fopat
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
        placa_remolque         = COALESCE(EXCLUDED.placa_remolque,      public.manifiestos_flat.placa_remolque),
        conductor             = COALESCE(EXCLUDED.conductor,            public.manifiestos_flat.conductor),
        celular               = COALESCE(EXCLUDED.celular,              public.manifiestos_flat.celular),
        cedula_conductor      = COALESCE(EXCLUDED.cedula_conductor,     public.manifiestos_flat.cedula_conductor),
        propietario           = COALESCE(EXCLUDED.propietario,          public.manifiestos_flat.propietario),
        agencia_despachadora  = COALESCE(EXCLUDED.agencia_despachadora, public.manifiestos_flat.agencia_despachadora),
        nombre_responsable    = COALESCE(EXCLUDED.nombre_responsable,   public.manifiestos_flat.nombre_responsable),
        reteica               = COALESCE(EXCLUDED.reteica,              public.manifiestos_flat.reteica),
        r_fopat               = COALESCE(EXCLUDED.r_fopat,              public.manifiestos_flat.r_fopat),
        actualizado_en        = now();
END;
$$;

-- ══════════════════════════════════════════════════════════════════════════════
-- 8. RECREATE guardar_digitador_batch (with placa_remolque instead of tipo_vehiculo)
-- ══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION public.guardar_digitador_batch(
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
    p_placa_remolque              TEXT     DEFAULT NULL,
    p_conductor             TEXT     DEFAULT NULL,
    p_celular               TEXT     DEFAULT NULL,
    p_cedula_conductor      TEXT     DEFAULT NULL,
    p_propietario           TEXT     DEFAULT NULL,
    p_agencia_despachadora  TEXT     DEFAULT NULL,
    p_nombre_responsable    TEXT     DEFAULT NULL,
    p_reteica               NUMERIC  DEFAULT NULL,
    p_r_fopat               NUMERIC  DEFAULT NULL
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
        placa, placa_remolque, conductor, celular, cedula_conductor,
        propietario, agencia_despachadora, nombre_responsable,
        reteica, r_fopat
    ) VALUES (
        p_manifiesto, p_archivo_origen, p_mes, p_año, p_periodo, p_semana, p_consecutivo_semanal,
        p_fecha_despacho, p_origen, p_departamento_origen, p_destino, p_departamento_destino,
        p_cliente, p_remesas, p_valor_remesa, p_flete_conductor, p_anticipo,
        p_placa, p_placa_remolque, p_conductor, p_celular, p_cedula_conductor,
        p_propietario, p_agencia_despachadora, p_nombre_responsable,
        p_reteica, p_r_fopat
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
        placa_remolque         = COALESCE(EXCLUDED.placa_remolque,      public.manifiestos_flat.placa_remolque),
        celular               = COALESCE(EXCLUDED.celular,              public.manifiestos_flat.celular),
        agencia_despachadora  = COALESCE(EXCLUDED.agencia_despachadora, public.manifiestos_flat.agencia_despachadora),
        nombre_responsable    = COALESCE(EXCLUDED.nombre_responsable,   public.manifiestos_flat.nombre_responsable),
        reteica               = COALESCE(EXCLUDED.reteica,              public.manifiestos_flat.reteica),
        r_fopat               = COALESCE(EXCLUDED.r_fopat,              public.manifiestos_flat.r_fopat),
        actualizado_en        = now();
END;
$$;

-- ══════════════════════════════════════════════════════════════════════════════
-- 9. RECREATE get_catalogos (with placa_remolque instead of tipo_vehiculo)
-- ══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION public.get_catalogos()
RETURNS JSON
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = ''
AS $$
    SELECT json_build_object(
        'conductores', (
            SELECT COALESCE(json_agg(nombre ORDER BY nombre), '[]'::json)
            FROM (
                SELECT DISTINCT conductor AS nombre
                FROM public.manifiestos_flat
                WHERE conductor IS NOT NULL AND conductor <> ''
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
                SELECT DISTINCT placa_remolque AS nombre
                FROM public.manifiestos_flat
                WHERE placa_remolque IS NOT NULL AND placa_remolque <> ''
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
        ),
        'facturas_electronicas', (
            SELECT COALESCE(json_agg(nombre ORDER BY nombre), '[]'::json)
            FROM (
                SELECT DISTINCT factura_electronica AS nombre
                FROM public.manifiestos_flat
                WHERE factura_electronica IS NOT NULL AND factura_electronica <> ''
            ) fe
        ),
        'facturas_no', (
            SELECT COALESCE(json_agg(nombre ORDER BY nombre), '[]'::json)
            FROM (
                SELECT DISTINCT factura_no AS nombre
                FROM public.manifiestos_flat
                WHERE factura_no IS NOT NULL AND factura_no <> ''
            ) fn
        )
    )
$$;

-- ══════════════════════════════════════════════════════════════════════════════
-- 10. RECREATE audit function (with placa_remolque instead of tipo_vehiculo)
-- ══════════════════════════════════════════════════════════════════════════════

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
        'celular','placa','placa_remolque','propietario','agencia_despachadora',
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

-- ══════════════════════════════════════════════════════════════════════════════
-- 11. RECREATE triggers
-- ══════════════════════════════════════════════════════════════════════════════

CREATE TRIGGER trg_audit_manifiestos
    AFTER UPDATE ON public.manifiestos_flat
    FOR EACH ROW EXECUTE FUNCTION public.fn_audit_manifiestos();

CREATE TRIGGER trg_audit_manifiestos_delete
    AFTER DELETE ON public.manifiestos_flat
    FOR EACH ROW EXECUTE FUNCTION public.fn_audit_manifiestos_delete();

CREATE TRIGGER trg_notify_plazo_vigente
    AFTER UPDATE OF fecha_cumplido ON public.manifiestos_flat
    FOR EACH ROW EXECUTE FUNCTION public.fn_notify_plazo_vigente();

CREATE TRIGGER trg_notify_pago_realizado
    AFTER UPDATE OF fecha_pago ON public.manifiestos_flat
    FOR EACH ROW EXECUTE FUNCTION public.fn_notify_pago_realizado();

-- ══════════════════════════════════════════════════════════════════════════════
-- 12. RECREATE index on placa_remolque (replaces old tipo_vehiculo index)
-- ══════════════════════════════════════════════════════════════════════════════

DROP INDEX IF EXISTS public.manifiestos_flat_tipo_vehiculo_idx;
CREATE INDEX IF NOT EXISTS idx_mflat_placa_remolque ON public.manifiestos_flat (placa_remolque);