import { describe, it, expect, vi, beforeEach } from 'vitest'
import { renderHook, waitFor } from '@testing-library/react'
import { supabase } from '../lib/supabase'
import { useDashboard } from '../hooks/useDashboard'

function mockRpc(dashboardKpisData, anualData = []) {
  supabase.rpc.mockImplementation((fnName, params) => {
    if (fnName === 'dashboard_kpis') {
      return Promise.resolve({ data: dashboardKpisData, error: null })
    }
    if (fnName === 'tendencia_anual') {
      return Promise.resolve({ data: anualData, error: null })
    }
    return Promise.resolve({ data: null, error: null })
  })
}

describe('useDashboard', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('llama a dashboard_kpis y tendencia_anual con los parametros correctos', async () => {
    mockRpc({}, [])
    const { result } = renderHook(() => useDashboard('MAYO', 2026))
    await waitFor(() => expect(result.current.loading).toBe(false))

    expect(supabase.rpc).toHaveBeenCalledWith('dashboard_kpis', { p_mes: 'MAYO', p_año: 2026 })
    expect(supabase.rpc).toHaveBeenCalledWith('tendencia_anual', { p_año: 2026 })
  })

  it('pasa null cuando no hay filtro de mes/año', async () => {
    mockRpc({}, [])
    const { result } = renderHook(() => useDashboard(null, null))
    await waitFor(() => expect(result.current.loading).toBe(false))

    expect(supabase.rpc).toHaveBeenCalledWith('dashboard_kpis', { p_mes: null, p_año: null })
    expect(supabase.rpc).toHaveBeenCalledWith('tendencia_anual', { p_año: null })
  })

  it('pasa las metricas del RPC directamente al data del hook', async () => {
    const kpis = {
      totalManifiestos: 5,
      anulados: 1,
      conductoresActivos: 3,
      rutasActivas: 2,
      totalRemesas: 3_000_000,
      totalFletes: 1_500_000,
      totalAnticipo: 300_000,
      pendientePagar: 395_000,
      sinFechaCumplido: 0,
      sinFactura: 1,
      conNovedad: 1,
      diasPromFacturar: 5,
      topClientes: [{ nombre: 'ACME', count: 2 }],
      topRutas: [{ ruta: 'CALI → BOGOTA', count: 2 }],
      topConductores: [{ nombre: 'JUAN PEREZ', count: 2 }],
      chartAgencias: [{ nombre: 'CALI', count: 2 }],
      chartEstadoInterno: [{ name: 'CUMPLIDO', value: 2 }],
      estadoPago: [{ name: 'PAGO A 15 DIAS', value: 2 }],
    }
    mockRpc(kpis)
    const { result } = renderHook(() => useDashboard('MAYO', 2026))
    await waitFor(() => expect(result.current.loading).toBe(false))

    const d = result.current.data
    expect(d.totalManifiestos).toBe(5)
    expect(d.anulados).toBe(1)
    expect(d.conductoresActivos).toBe(3)
    expect(d.rutasActivas).toBe(2)
    expect(d.totalRemesas).toBe(3_000_000)
    expect(d.totalFletes).toBe(1_500_000)
    expect(d.totalAnticipo).toBe(300_000)
    expect(d.pendientePagar).toBe(395_000)
    expect(d.sinFactura).toBe(1)
    expect(d.conNovedad).toBe(1)
    expect(d.diasPromFacturar).toBe(5)
    expect(d.topClientes).toEqual([{ nombre: 'ACME', count: 2 }])
    expect(d.estadoPago).toEqual([{ name: 'PAGO A 15 DIAS', value: 2 }])
  })

  it('construye lineChart con los 12 meses ordenados desde tendencia_anual', async () => {
    mockRpc({}, [
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
    expect(lc[11]).toEqual({ mes: 'DIC', facturado: 0,  ganancia: 0  })
  })

  it('maneja RPC con data null gracefully', async () => {
    supabase.rpc.mockResolvedValue({ data: null, error: null })
    const { result } = renderHook(() => useDashboard('MAYO', 2026))
    await waitFor(() => expect(result.current.loading).toBe(false))

    expect(result.current.data).not.toBeNull()
    expect(result.current.data.totalManifiestos).toBeUndefined()
    expect(result.current.data.lineChart).toHaveLength(12)
  })

  it('maneja error de RPC sin crashear', async () => {
    supabase.rpc.mockRejectedValue(new Error('DB timeout'))
    const { result } = renderHook(() => useDashboard('MAYO', 2026))
    await waitFor(() => expect(result.current.loading).toBe(false))

    expect(result.current.data).toBeNull()
  })
})
