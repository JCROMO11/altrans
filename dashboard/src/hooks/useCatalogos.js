import { useState, useEffect, useCallback } from 'react'
import { supabase } from '../lib/supabase'

export function useCatalogos() {
  const [catalogos, setCatalogos] = useState({
    conductores: [], clientes: [], lugares: [],
    responsables: [], vehiculos: [], remolques: [], agencias: [],
  })
  const [loading, setLoading] = useState(true)

  const fetch = useCallback(async () => {
    const [c, cl, l, r, v, rm, a, freq] = await Promise.all([
      supabase.from('conductores').select('id, nombre, cedula, celular').order('nombre'),
      supabase.from('clientes').select('id, nombre').order('nombre'),
      supabase.from('lugares').select('id, nombre').order('nombre'),
      supabase.from('responsables').select('id, nombre').order('nombre'),
      supabase.from('vehiculos').select('placa').order('placa'),
      supabase.from('remolques').select('placa').order('placa'),
      supabase.from('agencias').select('id, nombre').order('nombre'),
      Promise.resolve(supabase.rpc('catalog_frecuencias')).catch(() => ({ data: null })),
    ])

    const freqMap = {}
    for (const row of (freq.data ?? [])) {
      if (!freqMap[row.tipo]) freqMap[row.tipo] = {}
      const key = row.ref_id != null ? row.ref_id : row.ref_placa
      freqMap[row.tipo][key] = (freqMap[row.tipo][key] ?? 0) + Number(row.cnt)
    }

    const sortByFreq = (items, tipo, keyFn) =>
      [...items].sort((a, b) => (freqMap[tipo]?.[keyFn(b)] ?? 0) - (freqMap[tipo]?.[keyFn(a)] ?? 0))

    setCatalogos({
      conductores:  sortByFreq(c.data ?? [],  'conductor',   x => x.id),
      clientes:     sortByFreq(cl.data ?? [], 'cliente',     x => x.id),
      lugares:      sortByFreq(l.data ?? [],  'lugar',       x => x.id),
      responsables: sortByFreq(r.data ?? [],  'responsable', x => x.id),
      vehiculos:    sortByFreq(v.data ?? [],  'vehiculo',    x => x.placa),
      remolques:    rm.data ?? [],
      agencias:     a.data ?? [],
    })
    setLoading(false)
  }, [])

  useEffect(() => { fetch() }, [fetch])

  const createConductor = async (nombre, extras = {}) => {
    const { data, error } = await supabase.from('conductores')
      .insert({ nombre, cedula: extras.cedula || null, celular: extras.celular || null })
      .select().single()
    if (error) throw error
    await fetch()
    return data
  }
  const updateConductor = async (id, updates) => {
    const { error } = await supabase.from('conductores').update(updates).eq('id', id)
    if (error) throw error
    await fetch()
  }

  const createCliente = async (nombre) => {
    const { data, error } = await supabase.from('clientes').insert({ nombre }).select().single()
    if (error) throw error
    await fetch()
    return data
  }
  const createLugar = async (nombre) => {
    const { data, error } = await supabase.from('lugares').insert({ nombre }).select().single()
    if (error) throw error
    await fetch()
    return data
  }
  const createResponsable = async (nombre) => {
    const { data, error } = await supabase.from('responsables').insert({ nombre }).select().single()
    if (error) throw error
    await fetch()
    return data
  }
  const createVehiculo = async (placa) => {
    const { data, error } = await supabase.from('vehiculos').insert({ placa }).select().single()
    if (error) throw error
    await fetch()
    return data
  }

  const createRemolque = async (placa) => {
    const { data, error } = await supabase.from('remolques').insert({ placa }).select().single()
    if (error) throw error
    await fetch()
    return data
  }

  return { catalogos, loading, createConductor, updateConductor, createCliente, createLugar, createResponsable, createVehiculo, createRemolque }
}
