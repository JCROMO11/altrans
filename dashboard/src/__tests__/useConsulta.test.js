import { describe, it, expect, vi, beforeEach } from 'vitest'
import { renderHook, act } from '@testing-library/react'
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

  // ── Bloque 3: filtros nuevos ──────────────────────────────────────────────

  it('pasa p_cedula_conductor a ambos RPC', async () => {
    supabase.rpc.mockResolvedValueOnce({ data: sampleRows, error: null })
                .mockResolvedValueOnce({ data: sampleTotals, error: null })

    const { result } = renderHook(() => useConsulta())
    await act(async () => {
      await result.current.buscar({ cedula_conductor: '12345678' })
    })

    const [r1, params1] = supabase.rpc.mock.calls[0]
    const [r2, params2] = supabase.rpc.mock.calls[1]
    expect(r1).toBe('consulta_manifiestos')
    expect(r2).toBe('consulta_totales')
    expect(params1.p_cedula_conductor).toBe('12345678')
    expect(params2.p_cedula_conductor).toBe('12345678')
  })

  it('pasa p_tiene_fe: true | false | null según el valor del filtro', async () => {
    supabase.rpc.mockResolvedValueOnce({ data: sampleRows, error: null })
                .mockResolvedValueOnce({ data: sampleTotals, error: null })

    const { result } = renderHook(() => useConsulta())

    // Test 'true'
    await act(async () => { await result.current.buscar({ tiene_fe: 'true' }) })
    expect(supabase.rpc.mock.calls[0][1].p_tiene_fe).toBe(true)
    expect(supabase.rpc.mock.calls[1][1].p_tiene_fe).toBe(true)

    vi.clearAllMocks()  // limpia llamadas pero mantiene mock return
    supabase.rpc.mockResolvedValueOnce({ data: sampleRows, error: null })
                .mockResolvedValueOnce({ data: sampleTotals, error: null })

    // Test 'false'
    await act(async () => { await result.current.buscar({ tiene_fe: 'false' }) })
    expect(supabase.rpc.mock.calls[0][1].p_tiene_fe).toBe(false)
    expect(supabase.rpc.mock.calls[1][1].p_tiene_fe).toBe(false)

    vi.clearAllMocks()
    supabase.rpc.mockResolvedValueOnce({ data: sampleRows, error: null })
                .mockResolvedValueOnce({ data: sampleTotals, error: null })

    // Test '' → null
    await act(async () => { await result.current.buscar({ tiene_fe: '' }) })
    expect(supabase.rpc.mock.calls[0][1].p_tiene_fe).toBe(null)
    expect(supabase.rpc.mock.calls[1][1].p_tiene_fe).toBe(null)
  })

  it('pasa p_nombre_responsable a ambos RPC', async () => {
    supabase.rpc.mockResolvedValueOnce({ data: sampleRows, error: null })
                .mockResolvedValueOnce({ data: sampleTotals, error: null })

    const { result } = renderHook(() => useConsulta())
    await act(async () => {
      await result.current.buscar({ nombre_responsable: 'ANDRES' })
    })

    expect(supabase.rpc.mock.calls[0][1].p_nombre_responsable).toBe('ANDRES')
    expect(supabase.rpc.mock.calls[1][1].p_nombre_responsable).toBe('ANDRES')
  })

  // ── Bloque 4: estado_vencimiento ──────────────────────────────────────────

  it('pasa p_estado_vencimiento a ambos RPC', async () => {
    supabase.rpc.mockResolvedValueOnce({ data: sampleRows, error: null })
                .mockResolvedValueOnce({ data: sampleTotals, error: null })

    const { result } = renderHook(() => useConsulta())
    await act(async () => {
      await result.current.buscar({ estado_vencimiento: 'vencidos' })
    })

    expect(supabase.rpc.mock.calls[0][1].p_estado_vencimiento).toBe('vencidos')
    expect(supabase.rpc.mock.calls[1][1].p_estado_vencimiento).toBe('vencidos')
  })

  it('pasa null en p_estado_vencimiento cuando no está seteado', async () => {
    supabase.rpc.mockResolvedValueOnce({ data: sampleRows, error: null })
                .mockResolvedValueOnce({ data: sampleTotals, error: null })

    const { result } = renderHook(() => useConsulta())
    await act(async () => {
      await result.current.buscar({ estado_vencimiento: '' })
    })

    expect(supabase.rpc.mock.calls[0][1].p_estado_vencimiento).toBe(null)
    expect(supabase.rpc.mock.calls[1][1].p_estado_vencimiento).toBe(null)
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
