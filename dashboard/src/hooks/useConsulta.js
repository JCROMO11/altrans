import { useState, useCallback } from 'react'
import { supabase } from '../lib/supabase'

const PAGE_SIZE = 50

export function useConsulta() {
  const [rows,    setRows]    = useState([])
  const [totals,  setTotals]  = useState(null)
  const [loading, setLoading] = useState(false)
  const [page,    setPage]    = useState(0)
  const [hasMore, setHasMore] = useState(false)
  const [lastFilters, setLastFilters] = useState(null)

  const buildParams = (filters, pageNum) => ({
    p_manifiesto:      filters.manifiesto      ? Number(filters.manifiesto) : null,
    p_fecha_desde:     filters.fecha_desde     || null,
    p_fecha_hasta:     filters.fecha_hasta     || null,
    p_conductor:       filters.conductor       || null,
    p_cliente:         filters.cliente         || null,
    p_origen:          filters.origen          || null,
    p_destino:         filters.destino         || null,
    p_placa:           filters.placa           || null,
    p_compromiso_pago: filters.compromiso_pago || null,
    p_estado_interno:  filters.estado_interno  || null,
    p_mes:             filters.mes             || null,
    p_año:             filters.año             ? Number(filters.año) : null,
    p_limit:           PAGE_SIZE + 1,
    p_offset:          pageNum * PAGE_SIZE,
  })

  const buscar = useCallback(async (filters, pageNum = 0) => {
    setLoading(true)
    setLastFilters(filters)
    setPage(pageNum)

    const rowParams = buildParams(filters, pageNum)
    const totParams = {
      p_fecha_desde:     rowParams.p_fecha_desde,
      p_fecha_hasta:     rowParams.p_fecha_hasta,
      p_conductor:       rowParams.p_conductor,
      p_cliente:         rowParams.p_cliente,
      p_compromiso_pago: rowParams.p_compromiso_pago,
      p_estado_interno:  rowParams.p_estado_interno,
      p_mes:             rowParams.p_mes,
      p_año:             rowParams.p_año,
    }

    const [rowRes, totRes] = await Promise.all([
      supabase.rpc('consulta_manifiestos', rowParams),
      pageNum === 0
        ? supabase.rpc('consulta_totales', totParams)
        : Promise.resolve({ data: null }),
    ])

    const fetched = rowRes.data ?? []
    setHasMore(fetched.length > PAGE_SIZE)
    setRows(fetched.slice(0, PAGE_SIZE))
    if (totRes.data) setTotals(totRes.data[0] ?? null)
    setLoading(false)
  }, [])

  const nextPage = () => { if (hasMore && lastFilters) buscar(lastFilters, page + 1) }
  const prevPage = () => { if (page > 0 && lastFilters) buscar(lastFilters, page - 1) }

  return { rows, totals, loading, page, hasMore, buscar, nextPage, prevPage }
}
