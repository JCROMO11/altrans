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
    p_manifiesto:     filters.manifiesto     ? Number(filters.manifiesto) : null,
    p_fecha_desde:    filters.fecha_desde    || null,
    p_fecha_hasta:    filters.fecha_hasta    || null,
    p_conductor_id:   filters.conductor_id   || null,
    p_cliente_id:     filters.cliente_id     || null,
    p_origen_id:      filters.origen_id      || null,
    p_destino_id:     filters.destino_id     || null,
    p_estado_pago:    filters.estado_pago    || null,
    p_estado_interno: filters.estado_interno || null,
    p_limit:          PAGE_SIZE + 1,
    p_offset:         pageNum * PAGE_SIZE,
  })

  const buscar = useCallback(async (filters, pageNum = 0) => {
    setLoading(true)
    setLastFilters(filters)
    setPage(pageNum)

    const rowParams = buildParams(filters, pageNum)
    const totParams = {
      p_manifiesto:     rowParams.p_manifiesto,
      p_fecha_desde:    rowParams.p_fecha_desde,
      p_fecha_hasta:    rowParams.p_fecha_hasta,
      p_conductor_id:   rowParams.p_conductor_id,
      p_cliente_id:     rowParams.p_cliente_id,
      p_origen_id:      rowParams.p_origen_id,
      p_destino_id:     rowParams.p_destino_id,
      p_estado_pago:    rowParams.p_estado_pago,
      p_estado_interno: rowParams.p_estado_interno,
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

  const nextPage = () => {
    if (hasMore && lastFilters) buscar(lastFilters, page + 1)
  }
  const prevPage = () => {
    if (page > 0 && lastFilters) buscar(lastFilters, page - 1)
  }

  return { rows, totals, loading, page, hasMore, buscar, nextPage, prevPage }
}
