import { supabase } from '../lib/supabase'
import { parseCiudad } from '../lib/geography'

const MESES = ['ENERO','FEBRERO','MARZO','ABRIL','MAYO','JUNIO',
               'JULIO','AGOSTO','SEPTIEMBRE','OCTUBRE','NOVIEMBRE','DICIEMBRE']

export function useManifiesto() {
  const search = async (numero) => {
    const { data, error } = await supabase
      .from('manifiestos_flat')
      .select('*')
      .eq('manifiesto', numero)
      .maybeSingle()
    if (error) throw error
    return data
  }

  const update = async (manifiesto_id, form) => {
    const fecha   = new Date(form.fecha_despacho + 'T12:00:00')
    const mes     = MESES[fecha.getMonth()]
    const año     = fecha.getFullYear()
    const periodo = `${año}-${String(fecha.getMonth() + 1).padStart(2, '0')}-01`

    const codigos = (form.remesas || '').split(/[;,]/).map(s => s.trim()).filter(Boolean)

    const { depto: depto_origen } = parseCiudad(form.origen)
    const { depto: depto_destino } = parseCiudad(form.destino)

    const { error } = await supabase.rpc('guardar_digitador', {
      p_manifiesto:           manifiesto_id,
      p_mes:                  mes,
      p_año:                  año,
      p_periodo:              periodo,
      p_fecha_despacho:       form.fecha_despacho          || null,
      p_origen:               form.origen                  || null,
      p_departamento_origen:  depto_origen                 || null,
      p_destino:              form.destino                 || null,
      p_departamento_destino: depto_destino                || null,
      p_cliente:              form.cliente                 || null,
      p_remesas:              codigos.length ? codigos.join(',') : null,
      p_valor_remesa:         form.valor_remesa    ? Number(form.valor_remesa)    : null,
      p_flete_conductor:      form.flete_conductor ? Number(form.flete_conductor) : null,
      p_anticipo:             form.anticipo        ? Number(form.anticipo)        : null,
      p_placa:                form.placa                   || null,
      p_tipo_vehiculo:        form.tipo_vehiculo            || null,
      p_conductor:            form.conductor               || null,
      p_celular:              form.celular                 || null,
      p_cedula_conductor:     form.cedula_conductor        || null,
      p_propietario:          form.propietario             || null,
      p_agencia_despachadora: form.agencia_despachadora    || null,
      p_nombre_responsable:   form.nombre_responsable      || null,
    })
    if (error) throw error
  }

  const updateLogistico = async (manifiesto_id, form) => {
    const { error } = await supabase.rpc('guardar_logistico', {
      p_manifiesto:                 manifiesto_id,
      p_fecha_cumplido:             form.fecha_cumplido              || null,
      p_compromiso_pago:            form.compromiso_pago             || null,
      p_novedades:                  form.novedades                   ?? null,
      p_estado_interno:             form.estado_interno              || null,
      p_responsable_estado_interno: form.responsable_estado_interno  || null,
      p_novedad_conductor:          form.novedad_conductor           ?? null,
      p_novedad_empresa:            form.novedad_empresa             ?? null,
      p_ajuste_positivo_flete:      form.ajuste_positivo_flete !== '' && form.ajuste_positivo_flete != null ? Number(form.ajuste_positivo_flete) : null,
      p_ajuste_negativo_flete:      form.ajuste_negativo_flete !== '' && form.ajuste_negativo_flete != null ? Number(form.ajuste_negativo_flete) : null,
      p_consignacion_a_terceros:    form.consignacion_a_terceros !== '' && form.consignacion_a_terceros != null ? Number(form.consignacion_a_terceros) : null,
    })
    if (error) throw error
  }

  const updateTesoreria = async (manifiesto_id, form) => {
    const { error } = await supabase.rpc('guardar_tesoreria', {
      p_manifiesto:         manifiesto_id,
      p_fecha_pago:         form.fecha_pago         || null,
      p_valor_pagado:       form.valor_pagado        ? Number(form.valor_pagado) : null,
      p_entidad_financiera: form.entidad_financiera  || null,
      p_responsable:        form.responsable         || null,
    })
    if (error) throw error
  }

  const updateFacturacion = async (manifiesto_id, form) => {
    const { error } = await supabase.rpc('guardar_financiero', {
      p_manifiesto:          manifiesto_id,
      p_factura_no:          form.factura_no          || null,
      p_fecha_factura:       form.fecha_factura        || null,
      p_factura_electronica: form.factura_electronica  || null,
      p_mes_facturacion:     form.mes_facturacion      ? Number(form.mes_facturacion) : null,
      p_valor_factura:       form.valor_factura !== '' && form.valor_factura != null ? Number(form.valor_factura) : null,
    })
    if (error) throw error
  }

  const getManifiestosPorFE = async (factura_electronica) => {
    if (!factura_electronica) return []
    const { data, error } = await supabase.rpc('get_manifiestos_por_fe', {
      p_factura_electronica: factura_electronica,
    })
    if (error) throw error
    return data ?? []
  }

  const remove = async (manifiesto_id) => {
    const { error } = await supabase.rpc('borrar_manifiesto', { p_manifiesto: manifiesto_id })
    if (error) throw error
  }

  // Solo estado_interno + responsable. Para financiero/administrativo, que NO
  // tienen acceso a guardar_logistico (no pueden tocar novedades/ajustes).
  const updateEstadoInterno = async (manifiesto_id, { estado_interno, responsable_estado_interno }) => {
    const { error } = await supabase.rpc('guardar_estado_interno', {
      p_manifiesto:                 manifiesto_id,
      p_estado_interno:             estado_interno             || null,
      p_responsable_estado_interno: responsable_estado_interno || null,
    })
    if (error) throw error
  }

  return { search, update, remove, updateLogistico, updateEstadoInterno, updateTesoreria, updateFacturacion, getManifiestosPorFE }
}
