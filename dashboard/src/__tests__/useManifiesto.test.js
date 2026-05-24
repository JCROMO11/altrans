import { describe, it, expect, vi, beforeEach } from 'vitest'
import { renderHook, act } from '@testing-library/react'
import { supabase } from '../lib/supabase'
import { useManifiesto } from '../hooks/useManifiesto'

describe('useManifiesto.update', () => {
  beforeEach(() => { vi.clearAllMocks() })

  it('deriva mes/año/periodo desde fecha_despacho y parsea ciudades', async () => {
    supabase.rpc.mockResolvedValue({ error: null })

    const { result } = renderHook(() => useManifiesto())
    await act(async () => {
      await result.current.update(12345, {
        fecha_despacho: '2026-05-14',
        origen:  'Bogotá (Cund)',
        destino: 'Cali (Vall)',
        valor_remesa: '1500000',
        flete_conductor: '800000',
        anticipo: '200000',
        remesas: 'R-1, R-2 ; R-3',
        placa: 'ABC123',
        conductor: 'JUAN',
        cedula_conductor: '111',
      })
    })

    expect(supabase.rpc).toHaveBeenCalledWith('guardar_digitador', expect.objectContaining({
      p_manifiesto: 12345,
      p_mes:        'MAYO',
      p_año:        2026,
      p_periodo:    '2026-05-01',
      p_departamento_origen:  'Cundinamarca',
      p_departamento_destino: 'Valle del Cauca',
      p_valor_remesa:    1500000,
      p_flete_conductor: 800000,
      p_anticipo:        200000,
      p_remesas:         'R-1,R-2,R-3',
    }))
  })

  it('numéricos vacíos → null en el RPC', async () => {
    supabase.rpc.mockResolvedValue({ error: null })

    const { result } = renderHook(() => useManifiesto())
    await act(async () => {
      await result.current.update(99, {
        fecha_despacho: '2026-01-01',
        origen: 'BOGOTA', destino: 'CALI',
        valor_remesa: '', flete_conductor: '', anticipo: '',
      })
    })

    const params = supabase.rpc.mock.calls[0][1]
    expect(params.p_valor_remesa).toBe(null)
    expect(params.p_flete_conductor).toBe(null)
    expect(params.p_anticipo).toBe(null)
  })

  it('updateLogistico envía ajustes y consignacion como número o null', async () => {
    supabase.rpc.mockResolvedValue({ error: null })

    const { result } = renderHook(() => useManifiesto())
    await act(async () => {
      await result.current.updateLogistico(50, {
        ajuste_positivo_flete: '100000',
        ajuste_negativo_flete: '',
        consignacion_a_terceros: 0,
      })
    })

    const params = supabase.rpc.mock.calls[0][1]
    expect(params.p_ajuste_positivo_flete).toBe(100000)
    expect(params.p_ajuste_negativo_flete).toBe(null)
    expect(params.p_consignacion_a_terceros).toBe(0)
  })

  it('updateTesoreria propaga error del RPC', async () => {
    supabase.rpc.mockResolvedValue({ error: new Error('rpc fail') })
    const { result } = renderHook(() => useManifiesto())
    await expect(
      result.current.updateTesoreria(1, { fecha_pago: '2026-05-01', valor_pagado: '500000' })
    ).rejects.toThrow('rpc fail')
  })
})
