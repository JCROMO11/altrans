import { supabase } from '../lib/supabase'

const MESES = ['ENERO','FEBRERO','MARZO','ABRIL','MAYO','JUNIO',
               'JULIO','AGOSTO','SEPTIEMBRE','OCTUBRE','NOVIEMBRE','DICIEMBRE']

export function useManifiesto() {
  const search = async (numero) => {
    const { data, error } = await supabase
      .from('manifiestos')
      .select(`
        *,
        conductor:conductores(id, nombre, cedula, celular),
        cliente:clientes(id, nombre),
        origen:lugares!origen_id(id, nombre),
        destino:lugares!destino_id(id, nombre),
        agencia:agencias(id, nombre),
        responsable:responsables(id, nombre),
        vehiculo:vehiculos(placa),
        remesas(id, codigo_remesa),
        pagos_conductor(*),
        facturacion(*)
      `)
      .eq('manifiesto', numero)
      .maybeSingle()
    if (error) throw error
    return data
  }

  const create = async (form) => {
    const fecha = new Date(form.fecha_despacho + 'T12:00:00')
    const mes   = MESES[fecha.getMonth()]
    const año   = fecha.getFullYear()
    const periodo = `${año}-${String(fecha.getMonth() + 1).padStart(2, '0')}-01`

    const payload = {
      manifiesto:      Number(form.manifiesto),
      periodo,
      mes,
      año,
      fecha_despacho:  form.fecha_despacho,
      conductor_id:    form.conductor_id,
      placa:           form.placa           || null,
      placa_remolque:  form.placa_remolque  || null,
      cliente_id:      form.cliente_id,
      origen_id:       form.origen_id,
      destino_id:      form.destino_id,
      agencia_id:      form.agencia_id      || null,
      responsable_id:  form.responsable_id  || null,
      valor_remesa:    form.valor_remesa    ? Number(form.valor_remesa)    : null,
      flete_conductor: form.flete_conductor ? Number(form.flete_conductor) : null,
      anticipo:        form.anticipo        ? Number(form.anticipo)        : null,
    }

    const { data: man, error } = await supabase
      .from('manifiestos').insert(payload).select().single()
    if (error) throw error

    const codigos = (form.remesas || '').split(';').map(s => s.trim()).filter(Boolean)
    if (codigos.length) {
      const { error: re } = await supabase.from('remesas').insert(
        codigos.map(c => ({ manifiesto_id: man.manifiesto, codigo_remesa: c }))
      )
      if (re) throw re
    }
    return man
  }

  const updatePagos = async (manifiesto_id, form) => {
    const payload = {
      manifiesto_id,
      fecha_cumplido:    form.fecha_cumplido    || null,
      estado:            form.estado            || null,
      condicion_pago:    form.condicion_pago    || null,
      novedades:         form.novedades         || null,
      fecha_pago:        form.fecha_pago        || null,
      valor_pagado:      form.valor_pagado      ? Number(form.valor_pagado) : null,
      entidad_financiera: form.entidad_financiera || null,
      responsable_id:    form.responsable_id    || null,
    }
    const { error } = await supabase
      .from('pagos_conductor')
      .upsert(payload, { onConflict: 'manifiesto_id' })
    if (error) throw error
  }

  const updateFacturacion = async (manifiesto_id, form) => {
    const payload = {
      manifiesto_id,
      factura_no:         form.factura_no         || null,
      fecha_factura:      form.fecha_factura       || null,
      factura_electronica: form.factura_electronica || null,
      mes_facturacion:    form.mes_facturacion     ? Number(form.mes_facturacion)    : null,
      estado_interno:     form.estado_interno      || null,
      responsable_id:     form.responsable_id      || null,
    }
    const { error } = await supabase
      .from('facturacion')
      .upsert(payload, { onConflict: 'manifiesto_id' })
    if (error) throw error
  }

  const update = async (manifiesto_id, form) => {
    const fecha = new Date(form.fecha_despacho + 'T12:00:00')
    const mes   = MESES[fecha.getMonth()]
    const año   = fecha.getFullYear()
    const periodo = `${año}-${String(fecha.getMonth() + 1).padStart(2, '0')}-01`

    const payload = {
      periodo, mes, año,
      fecha_despacho:  form.fecha_despacho,
      conductor_id:    form.conductor_id,
      placa:           form.placa           || null,
      placa_remolque:  form.placa_remolque  || null,
      cliente_id:      form.cliente_id,
      origen_id:       form.origen_id,
      destino_id:      form.destino_id,
      agencia_id:      form.agencia_id      || null,
      responsable_id:  form.responsable_id  || null,
      valor_remesa:    form.valor_remesa    ? Number(form.valor_remesa)    : null,
      flete_conductor: form.flete_conductor ? Number(form.flete_conductor) : null,
      anticipo:        form.anticipo        ? Number(form.anticipo)        : null,
    }
    const { error } = await supabase.from('manifiestos').update(payload).eq('manifiesto', manifiesto_id)
    if (error) throw error

    await supabase.from('remesas').delete().eq('manifiesto_id', manifiesto_id)
    const codigos = (form.remesas || '').split(';').map(s => s.trim()).filter(Boolean)
    if (codigos.length) {
      const { error: re } = await supabase.from('remesas').insert(
        codigos.map(c => ({ manifiesto_id, codigo_remesa: c }))
      )
      if (re) throw re
    }
  }

  const remove = async (manifiesto_id) => {
    await supabase.from('remesas').delete().eq('manifiesto_id', manifiesto_id)
    await supabase.from('pagos_conductor').delete().eq('manifiesto_id', manifiesto_id)
    await supabase.from('facturacion').delete().eq('manifiesto_id', manifiesto_id)
    const { error } = await supabase.from('manifiestos').delete().eq('manifiesto', manifiesto_id)
    if (error) throw error
  }

  return { search, create, update, remove, updatePagos, updateFacturacion }
}
