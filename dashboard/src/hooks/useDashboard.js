import { useState, useEffect } from 'react'
import { supabase } from '../lib/supabase'

const MESES_ORDER = ['ENERO','FEBRERO','MARZO','ABRIL','MAYO','JUNIO','JULIO','AGOSTO','SEPTIEMBRE','OCTUBRE','NOVIEMBRE','DICIEMBRE']

// PostgREST default max_rows ~1000; paginamos con range() para traer todo el período
const PAGE = 1000

async function fetchAll(build) {
  let all = []
  let from = 0
  while (true) {
    const { data, error } = await build().range(from, from + PAGE - 1)
    if (error) throw error
    if (!data?.length) break
    all = all.concat(data)
    if (data.length < PAGE) break
    from += PAGE
  }
  return all
}

// .in() también está sujeto al límite por página: dividimos ids en chunks
async function fetchIn(table, select, col, ids) {
  if (!ids.length) return []
  const CHUNK = 500
  const chunks = []
  for (let i = 0; i < ids.length; i += CHUNK) chunks.push(ids.slice(i, i + CHUNK))
  const results = await Promise.all(
    chunks.map(c => fetchAll(() => supabase.from(table).select(select).in(col, c)))
  )
  return results.flat()
}

export function useDashboard(mes, año) {
  const [data, setData]       = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    let cancelled = false

    async function fetchData() {
      setLoading(true)
      try {
        const manifiestos = await fetchAll(() => {
          let q = supabase
            .from('manifiestos')
            .select('manifiesto, conductor_id, cliente_id, origen_id, destino_id, agencia_id, valor_remesa, flete_conductor, anticipo')
          if (mes) q = q.eq('mes', mes)
          if (año) q = q.eq('año', año)
          return q
        })
        if (cancelled) return

        const ids          = manifiestos.map(m => m.manifiesto)
        const conductorIds = [...new Set(manifiestos.map(m => m.conductor_id).filter(Boolean))]
        const clienteIds   = [...new Set(manifiestos.map(m => m.cliente_id).filter(Boolean))]
        const agenciaIds   = [...new Set(manifiestos.map(m => m.agencia_id).filter(Boolean))]
        const lugarIds     = [...new Set([...manifiestos.map(m => m.origen_id), ...manifiestos.map(m => m.destino_id)].filter(Boolean))]

        const [pagos, facturacion, conductoresData, clientesData, agenciasData, lugaresData, anualRes] =
          await Promise.all([
            fetchIn('pagos_conductor', 'manifiesto_id, fecha_cumplido, estado, novedades, valor_pagado', 'manifiesto_id', ids),
            fetchIn('facturacion',     'manifiesto_id, factura_no, dias_para_facturar, estado_interno',  'manifiesto_id', ids),
            fetchIn('conductores', 'id, nombre', 'id', conductorIds),
            fetchIn('clientes',    'id, nombre', 'id', clienteIds),
            fetchIn('agencias',    'id, nombre', 'id', agenciaIds),
            fetchIn('lugares',     'id, nombre', 'id', lugarIds),
            supabase.rpc('tendencia_anual', { p_año: año ?? null }),
          ])
        if (cancelled) return

        const anualData = anualRes.data ?? []

        const pagosMap   = Object.fromEntries(pagos.map(p => [p.manifiesto_id, p]))
        const factMap    = Object.fromEntries(facturacion.map(f => [f.manifiesto_id, f]))
        const condMap    = Object.fromEntries(conductoresData.map(c => [c.id, c.nombre]))
        const clienteMap = Object.fromEntries(clientesData.map(c => [c.id, c.nombre]))
        const agenciaMap = Object.fromEntries(agenciasData.map(a => [a.id, a.nombre]))
        const lugarMap   = Object.fromEntries(lugaresData.map(l => [l.id, l.nombre]))

        const totalManifiestos   = manifiestos.length
        const anulados           = manifiestos.filter(m => pagosMap[m.manifiesto]?.estado === 'ANULADO').length
        const conductoresActivos = new Set(manifiestos.map(m => m.conductor_id)).size
        const rutasActivas       = new Set(manifiestos.map(m => `${m.origen_id}-${m.destino_id}`)).size
        const totalRemesas       = manifiestos.reduce((s, m) => s + (m.valor_remesa    ?? 0), 0)
        const totalFletes        = manifiestos.reduce((s, m) => s + (m.flete_conductor ?? 0), 0)
        const totalAnticipo      = manifiestos.reduce((s, m) => s + (m.anticipo        ?? 0), 0)

        const pendientePagar = manifiestos.reduce((s, m) => {
          const pago = pagosMap[m.manifiesto]
          if (!pago || pago.estado === 'ANULADO' || pago.estado === 'PAGADO') return s
          return s + (m.flete_conductor ?? 0) - (m.anticipo ?? 0) - (pago.valor_pagado ?? 0)
        }, 0)

        const sinFechaCumplido = pagos.filter(p => !p.fecha_cumplido && p.estado !== 'ANULADO').length
        const sinFactura = manifiestos.filter(m => {
          if (pagosMap[m.manifiesto]?.estado === 'ANULADO') return false
          return !factMap[m.manifiesto]?.factura_no
        }).length
        const conNovedad = pagos.filter(p => p.novedades?.trim() && p.estado !== 'ANULADO').length
        const factsConDias = facturacion.filter(f => f.dias_para_facturar != null)
        const diasPromFacturar = factsConDias.length
          ? Math.round(factsConDias.reduce((s, f) => s + Number(f.dias_para_facturar), 0) / factsConDias.length)
          : 0

        const anualMap  = Object.fromEntries(anualData.map(r => [r.mes, r]))
        const lineChart = MESES_ORDER.map(m => ({
          mes:       m.slice(0, 3),
          facturado: Number(anualMap[m]?.facturado ?? 0),
          ganancia:  Number(anualMap[m]?.ganancia  ?? 0),
        }))

        const estadoPago = Object.entries(
          pagos.reduce((acc, p) => {
            const k = p.estado ?? 'SIN ESTADO'
            acc[k] = (acc[k] ?? 0) + 1
            return acc
          }, {})
        ).map(([name, value]) => ({ name, value }))
          .sort((a, b) => b.value - a.value)

        const clienteCount = {}
        manifiestos.forEach(m => {
          const nombre = clienteMap[m.cliente_id] ?? `#${m.cliente_id}`
          clienteCount[nombre] = (clienteCount[nombre] ?? 0) + 1
        })
        const topClientes = Object.entries(clienteCount)
          .sort((a, b) => b[1] - a[1]).slice(0, 7)
          .map(([nombre, count]) => ({ nombre, count }))

        const rutaCount = {}
        manifiestos.forEach(m => {
          const ruta = `${lugarMap[m.origen_id] ?? m.origen_id} → ${lugarMap[m.destino_id] ?? m.destino_id}`
          rutaCount[ruta] = (rutaCount[ruta] ?? 0) + 1
        })
        const topRutas = Object.entries(rutaCount)
          .sort((a, b) => b[1] - a[1]).slice(0, 7)
          .map(([ruta, count]) => ({ ruta, count }))

        const agenciaCount = {}
        manifiestos.forEach(m => {
          const nombre = agenciaMap[m.agencia_id] ?? `#${m.agencia_id}`
          agenciaCount[nombre] = (agenciaCount[nombre] ?? 0) + 1
        })
        const chartAgencias = Object.entries(agenciaCount)
          .sort((a, b) => b[1] - a[1])
          .map(([nombre, count]) => ({ nombre, count }))

        const estadoInternoCount = {}
        facturacion.forEach(f => {
          const k = f.estado_interno ?? 'SIN ESTADO'
          estadoInternoCount[k] = (estadoInternoCount[k] ?? 0) + 1
        })
        const chartEstadoInterno = Object.entries(estadoInternoCount)
          .sort((a, b) => b[1] - a[1])
          .map(([name, value]) => ({ name, value }))

        const conductorCount = {}
        manifiestos.forEach(m => {
          const nombre = condMap[m.conductor_id] ?? `#${m.conductor_id}`
          conductorCount[nombre] = (conductorCount[nombre] ?? 0) + 1
        })
        const topConductores = Object.entries(conductorCount)
          .sort((a, b) => b[1] - a[1]).slice(0, 7)
          .map(([nombre, count]) => ({ nombre, count }))

        if (cancelled) return
        setData({
          totalManifiestos, anulados, conductoresActivos, rutasActivas,
          totalRemesas, totalFletes, totalAnticipo, pendientePagar,
          sinFechaCumplido, sinFactura, conNovedad, diasPromFacturar,
          lineChart, estadoPago, topClientes, topRutas,
          chartAgencias, chartEstadoInterno, topConductores,
        })
      } catch (err) {
        if (!cancelled) console.error('useDashboard:', err)
      } finally {
        if (!cancelled) setLoading(false)
      }
    }

    fetchData()
    return () => { cancelled = true }
  }, [mes, año])

  return { data, loading }
}
