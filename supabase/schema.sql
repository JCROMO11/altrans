-- =============================================================================
-- Altrans — Esquema completo (Big Table)
-- Fuente única de verdad. Ejecutar en un proyecto Supabase nuevo/vacío.
-- Estructura de columnas idéntica al sheet de Abril 2026 / individual_cleaned.csv
-- =============================================================================

-- ── Limpieza de funciones anteriores (evita conflicto de firmas) ──────────────

DROP FUNCTION IF EXISTS public.consulta_manifiestos CASCADE;
DROP FUNCTION IF EXISTS public.consulta_totales CASCADE;
DROP FUNCTION IF EXISTS public.tendencia_anual CASCADE;
DROP FUNCTION IF EXISTS public.guardar_digitador CASCADE;
DROP FUNCTION IF EXISTS public.guardar_operativo CASCADE;
DROP FUNCTION IF EXISTS public.guardar_tesoreria CASCADE;
DROP FUNCTION IF EXISTS public.guardar_financiero CASCADE;
DROP FUNCTION IF EXISTS public.user_role CASCADE;
DROP FUNCTION IF EXISTS public.borrar_manifiesto CASCADE;

-- ── Tabla principal ───────────────────────────────────────────────────────────

CREATE TABLE public.manifiestos_flat (

    -- Identificación
    manifiesto              BIGINT          PRIMARY KEY,

    -- Contexto del sheet de origen
    archivo_origen          TEXT,
    mes                     TEXT,
    año                     SMALLINT,
    periodo                 DATE,
    semana                  TEXT,
    consecutivo_semanal     INTEGER,

    -- Operación
    fecha_despacho          DATE,
    origen                  TEXT,
    departamento_origen     TEXT,
    destino                 TEXT,
    departamento_destino    TEXT,
    cliente                 TEXT,
    remesas                 TEXT,           -- códigos separados por coma

    -- Financiero
    valor_remesa            NUMERIC(14, 2),
    flete_conductor         NUMERIC(14, 2),
    anticipo                NUMERIC(14, 2),

    -- Vehículo y conductor
    placa                   TEXT,
    tipo_vehiculo           TEXT,
    conductor               TEXT,
    celular                 TEXT,
    cedula_conductor        TEXT,
    propietario             TEXT,

    -- Despacho
    agencia_despachadora    TEXT,
    nombre_responsable      TEXT,

    -- Pago al conductor
    fecha_cumplido          DATE,
    compromiso_pago         TEXT,
    novedades               TEXT,
    novedad_conductor       TEXT,
    novedad_empresa         TEXT,
    ajuste_positivo_flete   NUMERIC(14, 2)  CHECK (ajuste_positivo_flete >= 0),
    ajuste_negativo_flete   NUMERIC(14, 2)  CHECK (ajuste_negativo_flete >= 0),
    flete_neto_conductor    NUMERIC(14, 2)  GENERATED ALWAYS AS (
                                CASE WHEN flete_conductor IS NOT NULL
                                     THEN flete_conductor
                                          + COALESCE(ajuste_positivo_flete, 0)
                                          - COALESCE(ajuste_negativo_flete, 0)
                                END
                            ) STORED,
    fecha_pago              DATE,
    valor_pagado            NUMERIC(14, 2),
    entidad_financiera      TEXT,
    responsable             TEXT,

    -- Facturación
    factura_no              TEXT,
    fecha_factura           DATE,
    factura_electronica     TEXT,
    mes_facturacion         SMALLINT,
    estado_interno          TEXT,
    responsable_estado_interno TEXT,

    -- Columna computada (inmutable: no usa CURRENT_DATE)
    dias_para_facturar      INTEGER GENERATED ALWAYS AS (
        CASE WHEN fecha_factura IS NOT NULL AND fecha_despacho IS NOT NULL
             THEN fecha_factura - fecha_despacho
        END
    ) STORED,

    -- Auditoría
    cargado_en              TIMESTAMPTZ     NOT NULL DEFAULT now(),
    actualizado_en          TIMESTAMPTZ     NOT NULL DEFAULT now()
);

-- ── Índices ───────────────────────────────────────────────────────────────────

CREATE INDEX idx_mflat_fecha_despacho   ON public.manifiestos_flat (fecha_despacho);
CREATE INDEX idx_mflat_periodo          ON public.manifiestos_flat (periodo);
CREATE INDEX idx_mflat_año_mes          ON public.manifiestos_flat (año, mes);
CREATE INDEX idx_mflat_cliente          ON public.manifiestos_flat (cliente);
CREATE INDEX idx_mflat_conductor        ON public.manifiestos_flat (conductor);
CREATE INDEX idx_mflat_placa            ON public.manifiestos_flat (placa);
CREATE INDEX idx_mflat_agencia          ON public.manifiestos_flat (agencia_despachadora);
CREATE INDEX idx_mflat_archivo_origen   ON public.manifiestos_flat (archivo_origen);

-- ── Vista: agrega dias_cumplido (usa CURRENT_DATE, no puede ser columna stored) ─

CREATE OR REPLACE VIEW public.v_manifiestos
WITH (security_invoker = true)
AS
SELECT
    *,
    CASE WHEN fecha_cumplido IS NOT NULL
         THEN CURRENT_DATE - fecha_cumplido
    END AS dias_cumplido
FROM public.manifiestos_flat;

-- ── RPC: consulta_manifiestos ─────────────────────────────────────────────────

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

-- ── RPC: consulta_totales ─────────────────────────────────────────────────────

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
        COALESCE(SUM(flete_neto_conductor), 0) - COALESCE(SUM(valor_pagado), 0)
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

-- ── RPC: tendencia_anual ──────────────────────────────────────────────────────

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

-- ── Función de rol (lee el rol del JWT de Supabase) ─────────────────────────

CREATE OR REPLACE FUNCTION public.user_role()
RETURNS TEXT
LANGUAGE sql STABLE
SET search_path = ''
AS $$ SELECT COALESCE((auth.jwt() -> 'app_metadata' ->> 'role'), '') $$;

-- ── RPCs de escritura por rol ─────────────────────────────────────────────────
-- El UPDATE directo a la tabla está restringido a service_role.
-- Cada rol llama su RPC, que solo toca sus columnas autorizadas.

-- Digitador: columnas operacionales del manifiesto (A–Q)
CREATE OR REPLACE FUNCTION public.guardar_digitador(
    p_manifiesto            BIGINT,
    p_archivo_origen        TEXT    DEFAULT NULL,
    p_mes                   TEXT    DEFAULT NULL,
    p_año                   SMALLINT DEFAULT NULL,
    p_periodo               DATE    DEFAULT NULL,
    p_semana                TEXT    DEFAULT NULL,
    p_consecutivo_semanal   INTEGER DEFAULT NULL,
    p_fecha_despacho        DATE    DEFAULT NULL,
    p_origen                TEXT    DEFAULT NULL,
    p_departamento_origen   TEXT    DEFAULT NULL,
    p_destino               TEXT    DEFAULT NULL,
    p_departamento_destino  TEXT    DEFAULT NULL,
    p_cliente               TEXT    DEFAULT NULL,
    p_remesas               TEXT    DEFAULT NULL,
    p_valor_remesa          NUMERIC DEFAULT NULL,
    p_flete_conductor       NUMERIC DEFAULT NULL,
    p_anticipo              NUMERIC DEFAULT NULL,
    p_placa                 TEXT    DEFAULT NULL,
    p_tipo_vehiculo         TEXT    DEFAULT NULL,
    p_conductor             TEXT    DEFAULT NULL,
    p_celular               TEXT    DEFAULT NULL,
    p_cedula_conductor      TEXT    DEFAULT NULL,
    p_propietario           TEXT    DEFAULT NULL,
    p_agencia_despachadora  TEXT    DEFAULT NULL,
    p_nombre_responsable    TEXT    DEFAULT NULL
)
RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER
SET search_path = ''
AS $$
BEGIN
    IF public.user_role() NOT IN ('digitador', 'admin') THEN
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
        archivo_origen        = COALESCE(EXCLUDED.archivo_origen,       manifiestos_flat.archivo_origen),
        mes                   = COALESCE(EXCLUDED.mes,                  manifiestos_flat.mes),
        año                   = COALESCE(EXCLUDED.año,                  manifiestos_flat.año),
        periodo               = COALESCE(EXCLUDED.periodo,              manifiestos_flat.periodo),
        semana                = COALESCE(EXCLUDED.semana,               manifiestos_flat.semana),
        consecutivo_semanal   = COALESCE(EXCLUDED.consecutivo_semanal,  manifiestos_flat.consecutivo_semanal),
        fecha_despacho        = COALESCE(EXCLUDED.fecha_despacho,       manifiestos_flat.fecha_despacho),
        origen                = COALESCE(EXCLUDED.origen,               manifiestos_flat.origen),
        departamento_origen   = COALESCE(EXCLUDED.departamento_origen,  manifiestos_flat.departamento_origen),
        destino               = COALESCE(EXCLUDED.destino,              manifiestos_flat.destino),
        departamento_destino  = COALESCE(EXCLUDED.departamento_destino, manifiestos_flat.departamento_destino),
        cliente               = COALESCE(EXCLUDED.cliente,              manifiestos_flat.cliente),
        remesas               = COALESCE(EXCLUDED.remesas,              manifiestos_flat.remesas),
        valor_remesa          = COALESCE(EXCLUDED.valor_remesa,         manifiestos_flat.valor_remesa),
        flete_conductor       = COALESCE(EXCLUDED.flete_conductor,      manifiestos_flat.flete_conductor),
        anticipo              = COALESCE(EXCLUDED.anticipo,             manifiestos_flat.anticipo),
        placa                 = COALESCE(EXCLUDED.placa,                manifiestos_flat.placa),
        tipo_vehiculo         = COALESCE(EXCLUDED.tipo_vehiculo,        manifiestos_flat.tipo_vehiculo),
        conductor             = COALESCE(EXCLUDED.conductor,            manifiestos_flat.conductor),
        celular               = COALESCE(EXCLUDED.celular,              manifiestos_flat.celular),
        cedula_conductor      = COALESCE(EXCLUDED.cedula_conductor,     manifiestos_flat.cedula_conductor),
        propietario           = COALESCE(EXCLUDED.propietario,          manifiestos_flat.propietario),
        agencia_despachadora  = COALESCE(EXCLUDED.agencia_despachadora, manifiestos_flat.agencia_despachadora),
        nombre_responsable    = COALESCE(EXCLUDED.nombre_responsable,   manifiestos_flat.nombre_responsable),
        actualizado_en        = now();
END;
$$;

-- Operativos Cumplen: fecha_cumplido, compromiso_pago, novedades, estado_interno + ajustes flete
CREATE OR REPLACE FUNCTION public.guardar_operativo(
    p_manifiesto                 BIGINT,
    p_fecha_cumplido             DATE    DEFAULT NULL,
    p_compromiso_pago            TEXT    DEFAULT NULL,
    p_novedades                  TEXT    DEFAULT NULL,
    p_estado_interno             TEXT    DEFAULT NULL,
    p_responsable_estado_interno TEXT    DEFAULT NULL,
    p_novedad_conductor          TEXT    DEFAULT NULL,
    p_novedad_empresa            TEXT    DEFAULT NULL,
    p_ajuste_positivo_flete      NUMERIC DEFAULT NULL,
    p_ajuste_negativo_flete      NUMERIC DEFAULT NULL
)
RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER
SET search_path = ''
AS $$
BEGIN
    IF public.user_role() NOT IN ('operativo', 'admin') THEN
        RAISE EXCEPTION 'Sin permiso';
    END IF;
    UPDATE public.manifiestos_flat SET
        fecha_cumplido               = COALESCE(p_fecha_cumplido,              fecha_cumplido),
        compromiso_pago              = COALESCE(p_compromiso_pago,             compromiso_pago),
        novedades                    = COALESCE(p_novedades,                   novedades),
        estado_interno               = COALESCE(p_estado_interno,              estado_interno),
        responsable_estado_interno   = COALESCE(p_responsable_estado_interno,  responsable_estado_interno),
        novedad_conductor            = COALESCE(p_novedad_conductor,           novedad_conductor),
        novedad_empresa              = COALESCE(p_novedad_empresa,             novedad_empresa),
        ajuste_positivo_flete        = COALESCE(p_ajuste_positivo_flete,       ajuste_positivo_flete),
        ajuste_negativo_flete        = COALESCE(p_ajuste_negativo_flete,       ajuste_negativo_flete),
        actualizado_en               = now()
    WHERE manifiesto = p_manifiesto;
END;
$$;

-- Auxiliar Tesorería: fecha_pago, valor_pagado, entidad_financiera, responsable (X–AA)
CREATE OR REPLACE FUNCTION public.guardar_tesoreria(
    p_manifiesto        BIGINT,
    p_fecha_pago        DATE    DEFAULT NULL,
    p_valor_pagado      NUMERIC DEFAULT NULL,
    p_entidad_financiera TEXT   DEFAULT NULL,
    p_responsable       TEXT    DEFAULT NULL
)
RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER
SET search_path = ''
AS $$
BEGIN
    IF public.user_role() NOT IN ('tesoreria', 'admin') THEN
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

-- Equipo Financiero: factura_no, fecha_factura, factura_electronica, mes_facturacion (AB–AF)
CREATE OR REPLACE FUNCTION public.guardar_financiero(
    p_manifiesto            BIGINT,
    p_factura_no            TEXT     DEFAULT NULL,
    p_fecha_factura         DATE     DEFAULT NULL,
    p_factura_electronica   TEXT     DEFAULT NULL,
    p_mes_facturacion       SMALLINT DEFAULT NULL
)
RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER
SET search_path = ''
AS $$
BEGIN
    IF public.user_role() NOT IN ('financiero', 'admin') THEN
        RAISE EXCEPTION 'Sin permiso';
    END IF;
    UPDATE public.manifiestos_flat SET
        factura_no          = COALESCE(p_factura_no,          factura_no),
        fecha_factura       = COALESCE(p_fecha_factura,       fecha_factura),
        factura_electronica = COALESCE(p_factura_electronica, factura_electronica),
        mes_facturacion     = COALESCE(p_mes_facturacion,     mes_facturacion),
        actualizado_en      = now()
    WHERE manifiesto = p_manifiesto;
END;
$$;

-- Admin: eliminar manifiesto
CREATE OR REPLACE FUNCTION public.borrar_manifiesto(p_manifiesto BIGINT)
RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER
SET search_path = ''
AS $$
BEGIN
    IF public.user_role() <> 'admin' THEN
        RAISE EXCEPTION 'Sin permiso';
    END IF;
    DELETE FROM public.manifiestos_flat WHERE manifiesto = p_manifiesto;
END;
$$;

-- ── Permisos de schema (requerido por PostgREST / Supabase) ──────────────────

GRANT USAGE ON SCHEMA public TO anon, authenticated;

-- ── Permisos de tabla ─────────────────────────────────────────────────────────
-- Lectura directa: solo usuarios autenticados.
-- Escritura directa: bloqueada para todos (se hace vía RPCs SECURITY DEFINER).

GRANT SELECT ON public.manifiestos_flat TO authenticated;
GRANT SELECT ON public.v_manifiestos    TO authenticated;

-- Las RPCs SECURITY DEFINER necesitan que postgres (owner) tenga ALL sobre la tabla.
-- En Supabase esto ya está dado por defecto; se incluye para proyectos propios.
GRANT ALL ON public.manifiestos_flat TO postgres;

-- ── Permisos de funciones de lectura ─────────────────────────────────────────

GRANT EXECUTE ON FUNCTION public.consulta_manifiestos(BIGINT, DATE, DATE, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, SMALLINT, INTEGER, INTEGER) TO authenticated;
GRANT EXECUTE ON FUNCTION public.consulta_totales(DATE, DATE, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, SMALLINT)                                                  TO authenticated;
GRANT EXECUTE ON FUNCTION public.tendencia_anual(INTEGER)                                                                                                    TO authenticated;

-- ── Permisos de funciones de escritura (cada rol solo puede llamar la suya) ──

GRANT EXECUTE ON FUNCTION public.guardar_digitador(BIGINT, TEXT, TEXT, SMALLINT, DATE, TEXT, INTEGER, DATE, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, NUMERIC, NUMERIC, NUMERIC, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT) TO authenticated;
GRANT EXECUTE ON FUNCTION public.guardar_operativo(BIGINT, DATE, TEXT, TEXT, TEXT, TEXT)   TO authenticated;
GRANT EXECUTE ON FUNCTION public.guardar_tesoreria(BIGINT, DATE, NUMERIC, TEXT, TEXT)      TO authenticated;
GRANT EXECUTE ON FUNCTION public.guardar_financiero(BIGINT, TEXT, DATE, TEXT, SMALLINT)    TO authenticated;
GRANT EXECUTE ON FUNCTION public.borrar_manifiesto(BIGINT)                                 TO authenticated;

-- ── RLS ───────────────────────────────────────────────────────────────────────

ALTER TABLE public.manifiestos_flat ENABLE ROW LEVEL SECURITY;

-- Usuarios autenticados pueden leer toda la tabla
CREATE POLICY "lectura_autenticados"
    ON public.manifiestos_flat FOR SELECT
    USING (auth.role() = 'authenticated');

-- Solo service_role (ETL / carga masiva vía DATABASE_URL) puede escribir directo.
-- Los usuarios del dashboard usan las RPCs guardar_* que son SECURITY DEFINER.
CREATE POLICY "escritura_service_role"
    ON public.manifiestos_flat FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

-- ── Asignación de roles por usuario ──────────────────────────────────────────
-- Ejecutar UNA VEZ por usuario en el SQL Editor de Supabase:
--
--   UPDATE auth.users
--   SET raw_app_meta_data = raw_app_meta_data || '{"role":"<ROL>"}'::jsonb
--   WHERE email = '<EMAIL>';
--
-- Roles disponibles: digitador | operativo | tesoreria | financiero | admin
--
-- Ejemplo:
--   UPDATE auth.users SET raw_app_meta_data = raw_app_meta_data || '{"role":"admin"}'::jsonb
--   WHERE email = 'jose@altrans.com.co';
