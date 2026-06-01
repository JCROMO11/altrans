import { describe, it, expect, vi, beforeEach } from 'vitest'
import { renderHook, waitFor } from '@testing-library/react'
import { supabase } from '../lib/supabase'
import { useDashboard } from '../hooks/useDashboard'

// Fixture de filas mínimas, suficientes para validar la lógica de agregación.
const baseRow = {
  manifiesto: 100, conductor: 'JUAN PEREZ', cliente: 'ACME',
  agencia_despachadora: 'CALI', origen: 'CALI', destino: 'BOGOTA',
  valor_remesa: 1000000, flete_conductor: 500000,
  // saldo ya descuenta retención (1% = 5k) y anticipo (100k): 500k-5k-100k=395k
  saldo: 395000, anticipo: 100000,
  fecha_despacho: '2026-05-01', fecha_factura: null, compromiso_pago: 'PAGO A 15 DIAS',
  fecha_cumplido: '2026-05-02', novedades: null, fecha_pago: null,
  valor_pagado: null, estado_interno: 'CUMPLIDO', factura_no: 'F-001',
  dias_para_facturar: 5,
}
const rowAnulado = { ...baseRow, manifiesto: 101, estado_interno: 'ANULADO', conductor: 'NADIE', valor_remesa: 9_999_999 }
const rowPagado  = { ...baseRow, manifiesto: 102, fecha_pago: '2026-05-10', valor_pagado: 380000 }
const rowSinFactura = { ...baseRow, manifiesto: 103, factura_no: null }
const rowConNovedad = { ...baseRow, manifiesto: 104, novedades: 'REAJUSTE' }

function mockSupabaseChain(rows, anualData = []) {
  // useDashboard hace: supabase.from(...).select(...).eq(...).eq(...).range(from, to)
  // y en paralelo supabase.rpc('tendencia_anual', ...)
  const chain = {
    select: vi.fn().mockReturnThis(),
    eq:     vi.fn().mockReturnThis(),
    range:  vi.fn((from) => {
      if (from === 0) return Promise.resolve({ data: rows, error: null })
      return Promise.resolve({ data: [], error: null })
    }),
  }
  supabase.from.mockReturnValue(chain)
  supabase.rpc.mockResolvedValue({ data: anualData, error: null })
  return chain
}

describe('useDashboard', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('excluye ANULADOS de totales y métricas activas', async () => {
    mockSupabaseChain([baseRow, rowAnulado, rowPagado])
    const { result } = renderHook(() => useDashboard('MAYO', 2026))
    await waitFor(() => expect(result.current.loading).toBe(false))

    const d = result.current.data
    expect(d.totalManifiestos).toBe(3)  // count total sí incluye anulados
    expect(d.anulados).toBe(1)
    // Las sumas SÍ excluyen anulados — bug que se arregló
    expect(d.totalRemesas).toBe(2_000_000) // 2 × 1M, no 12M
    expect(d.conductoresActivos).toBe(1)   // JUAN PEREZ, no incluye 'NADIE' (anulado)
  })

  it('pendientePagar usa saldo cuando existe', async () => {
    // baseRow: neto=395k (ya con retención + anticipo descontados), no pagado.
    // El hook NO vuelve a restar anticipo → 395k pendiente.
    // rowPagado: tiene fecha_pago → excluido
    // rowAnulado: anulado → excluido
    mockSupabaseChain([baseRow, rowAnulado, rowPagado])
    const { result } = renderHook(() => useDashboard('MAYO', 2026))
    await waitFor(() => expect(result.current.loading).toBe(false))

    expect(result.current.data.pendientePagar).toBe(395_000)
  })

  it('cae a flete_conductor si saldo es null', async () => {
    const sinNeto = { ...baseRow, saldo: null }
    mockSupabaseChain([sinNeto])
    const { result } = renderHook(() => useDashboard('MAYO', 2026))
    await waitFor(() => expect(result.current.loading).toBe(false))

    // Fallback a flete_conductor 500k (sin neto disponible); el hook ya no
    // resta anticipo aparte → 500k.
    expect(result.current.data.pendientePagar).toBe(500_000)
  })

  it('cuenta sinFactura, conNovedad excluyendo anulados', async () => {
    mockSupabaseChain([baseRow, rowAnulado, rowSinFactura, rowConNovedad])
    const { result } = renderHook(() => useDashboard('MAYO', 2026))
    await waitFor(() => expect(result.current.loading).toBe(false))

    expect(result.current.data.sinFactura).toBe(1)
    expect(result.current.data.conNovedad).toBe(1)
  })

  it('lineChart usa los 12 meses ordenados desde tendencia_anual', async () => {
    mockSupabaseChain([baseRow], [
      { mes: 'ENERO', facturado: 100, ganancia: 10 },
      { mes: 'MAYO',  facturado: 500, ganancia: 50 },
    ])
    const { result } = renderHook(() => useDashboard(null, 2026))
    await waitFor(() => expect(result.current.loading).toBe(false))

    const lc = result.current.data.lineChart
    expect(lc).toHaveLength(12)
    expect(lc[0]).toEqual({ mes: 'ENE', facturado: 100, ganancia: 10 })
    expect(lc[4]).toEqual({ mes: 'MAY', facturado: 500, ganancia: 50 })
    expect(lc[1]).toEqual({ mes: 'FEB', facturado: 0,   ganancia: 0  })
  })
})
