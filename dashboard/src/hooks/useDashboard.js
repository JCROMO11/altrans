import { useState, useEffect } from 'react'
import { supabase } from '../lib/supabase'

const MESES_ORDER = ['ENERO','FEBRERO','MARZO','ABRIL','MAYO','JUNIO','JULIO','AGOSTO','SEPTIEMBRE','OCTUBRE','NOVIEMBRE','DICIEMBRE']
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

export function useDashboard(mes, año) {
  const [data, setData]       = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    let cancelled = false

    async function fetchData() {
      setLoading(true)
      try {
        const [rows, anualRes] = await Promise.all([
          fetchAll(() => {
            let q = supabase
              .from('manifiestos_flat')
              .select('manifiesto, conductor, cliente, agencia_despachadora, origen, destino, valor_remesa, flete_conductor, anticipo, fecha_despacho, fecha_factura, compromiso_pago, fecha_cumplido, novedades, fecha_pago, valor_pagado, estado_interno, factura_no, dias_para_facturar')
            if (mes) q = q.eq('mes', mes)
            if (año) q = q.eq('año', año)
            return q
          }),
          supabase.rpc('tendencia_anual', { p_año: año ?? null }),
        ])
        if (cancelled) return

        const anualData = anualRes.data ?? []

        const totalManifiestos   = rows.length
        const anulados           = rows.filter(r => r.compromiso_pago === 'ANULADO').length
        const conductoresActivos = new Set(rows.map(r => r.conductor).filter(Boolean)).size
        const rutasActivas       = new Set(rows.map(r => `${r.origen}-${r.destino}`)).size
        const totalRemesas       = rows.reduce((s, r) => s + (r.valor_remesa    ?? 0), 0)
        const totalFletes        = rows.reduce((s, r) => s + (r.flete_conductor ?? 0), 0)
        const totalAnticipo      = rows.reduce((s, r) => s + (r.anticipo        ?? 0), 0)

        const pendientePagar = rows.reduce((s, r) => {
          if (r.compromiso_pago === 'ANULADO' || r.compromiso_pago === 'PAGADO') return s
          return s + (r.flete_conductor ?? 0) - (r.anticipo ?? 0) - (r.valor_pagado ?? 0)
        }, 0)

        const sinFechaCumplido = rows.filter(r => !r.fecha_cumplido && r.compromiso_pago !== 'ANULADO').length
        const sinFactura       = rows.filter(r => r.compromiso_pago !== 'ANULADO' && !r.factura_no).length
        const conNovedad       = rows.filter(r => r.novedades?.trim() && r.compromiso_pago !== 'ANULADO').length

        const factsConDias = rows.filter(r => r.dias_para_facturar != null)
        const diasPromFacturar = factsConDias.length
          ? Math.round(factsConDias.reduce((s, r) => s + r.dias_para_facturar, 0) / factsConDias.length)
          : 0

        const anualMap  = Object.fromEntries(anualData.map(r => [r.mes, r]))
        const lineChart = MESES_ORDER.map(m => ({
          mes:       m.slice(0, 3),
          facturado: Number(anualMap[m]?.facturado ?? 0),
          ganancia:  Number(anualMap[m]?.ganancia  ?? 0),
        }))

        const estadoPago = Object.entries(
          rows.reduce((acc, r) => {
            const k = r.compromiso_pago ?? 'SIN ESTADO'
            acc[k] = (acc[k] ?? 0) + 1
            return acc
          }, {})
        ).map(([name, value]) => ({ name, value })).sort((a, b) => b.value - a.value)

        const clienteCount = {}
        rows.forEach(r => {
          const k = r.cliente ?? 'SIN CLIENTE'
          clienteCount[k] = (clienteCount[k] ?? 0) + 1
        })
        const topClientes = Object.entries(clienteCount)
          .sort((a, b) => b[1] - a[1]).slice(0, 7)
          .map(([nombre, count]) => ({ nombre, count }))

        const rutaCount = {}
        rows.forEach(r => {
          const ruta = r.origen && r.destino ? `${r.origen} → ${r.destino}` : '—'
          rutaCount[ruta] = (rutaCount[ruta] ?? 0) + 1
        })
        const topRutas = Object.entries(rutaCount)
          .sort((a, b) => b[1] - a[1]).slice(0, 7)
          .map(([ruta, count]) => ({ ruta, count }))

        const agenciaCount = {}
        rows.forEach(r => {
          const k = r.agencia_despachadora ?? 'SIN AGENCIA'
          agenciaCount[k] = (agenciaCount[k] ?? 0) + 1
        })
        const chartAgencias = Object.entries(agenciaCount)
          .sort((a, b) => b[1] - a[1])
          .map(([nombre, count]) => ({ nombre, count }))

        const estadoInternoCount = {}
        rows.forEach(r => {
          const k = r.estado_interno ?? 'SIN ESTADO'
          estadoInternoCount[k] = (estadoInternoCount[k] ?? 0) + 1
        })
        const chartEstadoInterno = Object.entries(estadoInternoCount)
          .sort((a, b) => b[1] - a[1])
          .map(([name, value]) => ({ name, value }))

        const conductorCount = {}
        rows.forEach(r => {
          const k = r.conductor ?? 'SIN CONDUCTOR'
          conductorCount[k] = (conductorCount[k] ?? 0) + 1
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
