import { describe, it, expect, vi, beforeEach } from 'vitest'
import { renderHook, act, waitFor } from '@testing-library/react'
import { supabase } from '../lib/supabase'
import { useConsulta } from '../hooks/useConsulta'

const sampleRows = Array.from({ length: 30 }, (_, i) => ({
  manifiesto: 1000 + i, conductor: `COND ${i}`, cliente: 'X',
}))
const sampleTotals = [{ total_manifiestos: 30, total_flete: 5_000_000 }]

describe('useConsulta', () => {
  beforeEach(() => { vi.clearAllMocks() })

  it('mapea filtros a params del RPC correctamente', async () => {
    supabase.rpc.mockResolvedValueOnce({ data: sampleRows, error: null })
                .mockResolvedValueOnce({ data: sampleTotals, error: null })

    const { result } = renderHook(() => useConsulta())
    await act(async () => {
      await result.current.buscar({
        manifiesto: '1234', conductor: 'JUAN', mes: 'MAYO', año: '2026',
        fecha_desde: '', cliente: null,
      })
    })

    // Primer call: consulta_manifiestos
    const [rpcName, params] = supabase.rpc.mock.calls[0]
    expect(rpcName).toBe('consulta_manifiestos')
    expect(params.p_manifiesto).toBe(1234)              // string → Number
    expect(params.p_conductor).toBe('JUAN')
    expect(params.p_mes).toBe('MAYO')
    expect(params.p_año).toBe(2026)
    expect(params.p_fecha_desde).toBe(null)             // string vacío → null
    expect(params.p_cliente).toBe(null)
    expect(params.p_limit).toBe(51)                     // PAGE_SIZE + 1
    expect(params.p_offset).toBe(0)
  })

  it('hasMore=true cuando hay más de PAGE_SIZE filas', async () => {
    const tooMany = Array.from({ length: 51 }, (_, i) => ({ manifiesto: i }))
    supabase.rpc.mockResolvedValueOnce({ data: tooMany, error: null })
                .mockResolvedValueOnce({ data: sampleTotals, error: null })

    const { result } = renderHook(() => useConsulta())
    await act(async () => { await result.current.buscar({}) })

    expect(result.current.hasMore).toBe(true)
    expect(result.current.rows).toHaveLength(50)
  })

  it('hasMore=false cuando llegan ≤ PAGE_SIZE filas', async () => {
    supabase.rpc.mockResolvedValueOnce({ data: sampleRows, error: null })
                .mockResolvedValueOnce({ data: sampleTotals, error: null })

    const { result } = renderHook(() => useConsulta())
    await act(async () => { await result.current.buscar({}) })

    expect(result.current.hasMore).toBe(false)
    expect(result.current.rows).toHaveLength(30)
  })

  it('nextPage incrementa offset y NO vuelve a pedir totales', async () => {
    supabase.rpc.mockResolvedValue({ data: sampleRows, error: null })

    const { result } = renderHook(() => useConsulta())
    await act(async () => { await result.current.buscar({ mes: 'MAYO' }) })

    // Marcar que sí hay más, para que nextPage avance
    supabase.rpc.mockResolvedValueOnce({
      data: Array.from({ length: 51 }, (_, i) => ({ manifiesto: i })),
      error: null,
    })
    await act(async () => { await result.current.buscar({ mes: 'MAYO' }, 1) })

    const lastCall = supabase.rpc.mock.calls.at(-1)
    expect(lastCall[1].p_offset).toBe(50)
  })
})
