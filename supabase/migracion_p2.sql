-- Migración p2: columnas nuevas + limpieza novedad_*
-- Ejecutar ANTES de schema_consolidated.sql

ALTER TABLE public.manifiestos_flat
  ADD COLUMN IF NOT EXISTS ajustes_detalle       JSONB,
  ADD COLUMN IF NOT EXISTS saldo_en_planilla     NUMERIC(14, 2)
    GENERATED ALWAYS AS (
      CASE WHEN flete_conductor IS NOT NULL
           THEN flete_conductor
                - ROUND(flete_conductor * 0.01, 2)
                - COALESCE(reteica, 0)
                - COALESCE(r_fopat, 0)
                - COALESCE(anticipo, 0)
      END
    ) STORED;

-- Opcional: eliminar columnas viejas que ya no usa el frontend
ALTER TABLE public.manifiestos_flat
  DROP COLUMN IF EXISTS novedad_conductor,
  DROP COLUMN IF EXISTS novedad_empresa;
