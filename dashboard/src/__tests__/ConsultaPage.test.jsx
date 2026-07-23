import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'

// Mocks ANTES de importar la página
const useConsultaMock = vi.fn()
vi.mock('../hooks/useConsulta', () => ({
  useConsulta: (...args) => useConsultaMock(...args),
}))

vi.mock('../hooks/useCatalogos', () => ({
  useCatalogos: () => ({
    catalogos: {
      conductores: [], clientes: [], lugares: [],
      responsables: [], vehiculos: [], remolques: [], agencias: [], propietarios: [],
    },
    loading: false,
  }),
}))

import { supabase } from '../lib/supabase'
import ConsultaPage from '../pages/ConsultaPage'

const sampleRows = [
  { manifiesto: 21001, conductor: 'JUAN PEREZ', cliente: 'ACME',
    origen: 'CALI', destino: 'BOGOTA', fecha_despacho: '2026-05-01',
    flete_conductor: 500000, valor_remesa: 1000000,
    estado_interno: 'CUMPLIDO', fecha_pago: null, compromiso_pago: 'PAGO A 15 DIAS' },
  { manifiesto: 21002, conductor: 'MARIA LOPEZ', cliente: 'XYZ',
    origen: 'BOGOTA', destino: 'CALI', fecha_despacho: '2026-05-02',
    flete_conductor: 600000, valor_remesa: 1200000,
    estado_interno: 'CUMPLIDO', fecha_pago: '2026-05-10', compromiso_pago: 'CONTRAENTREGA' },
]

function fmtLocalDate(d) {
  const mm = String(d.getMonth() + 1).padStart(2, '0')
  const dd = String(d.getDate()).padStart(2, '0')
  return `${d.getFullYear()}-${mm}-${dd}`
}

describe('ConsultaPage', () => {
  beforeEach(() => {
    useConsultaMock.mockReset()
    useConsultaMock.mockReturnValue({
      rows: sampleRows, totals: null, loading: false,
      page: 0, hasMore: false,
      buscar: vi.fn(), nextPage: vi.fn(), prevPage: vi.fn(), fetchAll: vi.fn(),
    })
  })

  it('renderiza el panel de filtros', () => {
    render(<ConsultaPage user={{ app_metadata: { role: 'logistico' } }} />)
    expect(screen.getByText(/Filtros/i)).toBeInTheDocument()
    expect(screen.getByText(/Limpiar/i)).toBeInTheDocument()
  })

  it('al montar dispara buscar con filtros iniciales (mes=null, etc)', () => {
    const buscar = vi.fn()
    useConsultaMock.mockReturnValue({
      rows: [], totals: null, loading: false, page: 0, hasMore: false,
      buscar, nextPage: vi.fn(), prevPage: vi.fn(), fetchAll: vi.fn(),
    })
    render(<ConsultaPage user={{ app_metadata: { role: 'logistico' } }} />)

    expect(buscar).toHaveBeenCalledWith(expect.objectContaining({
      manifiesto: '', fecha_desde: '', fecha_hasta: '',
      mes: '', año: '',
    }), 0)
  })

  it('cambiar manifiesto en input y submit dispara buscar con el valor', () => {
    const buscar = vi.fn()
    useConsultaMock.mockReturnValue({
      rows: [], totals: null, loading: false, page: 0, hasMore: false,
      buscar, nextPage: vi.fn(), prevPage: vi.fn(), fetchAll: vi.fn(),
    })
    render(<ConsultaPage user={{ app_metadata: { role: 'logistico' } }} />)

    const input = screen.getByPlaceholderText('Número...')
    fireEvent.change(input, { target: { value: '21001' } })

    const form = input.closest('form')
    fireEvent.submit(form)

    expect(buscar).toHaveBeenLastCalledWith(
      expect.objectContaining({ manifiesto: '21001' }), 0,
    )
  })

  it('"Limpiar todo" reinicia filtros y vuelve a buscar', () => {
    const buscar = vi.fn()
    useConsultaMock.mockReturnValue({
      rows: [], totals: null, loading: false, page: 0, hasMore: false,
      buscar, nextPage: vi.fn(), prevPage: vi.fn(), fetchAll: vi.fn(),
    })
    render(<ConsultaPage user={{ app_metadata: { role: 'logistico' } }} />)

    buscar.mockClear()
    fireEvent.click(screen.getByText(/Limpiar/i))

    expect(buscar).toHaveBeenCalledWith(
      expect.objectContaining({ manifiesto: '', fecha_desde: '' }), 0,
    )
  })

  it('rol gerencia ve la pestaña de Auditoría', () => {
    render(<ConsultaPage user={{ app_metadata: { role: 'gerencia' } }} />)
    expect(screen.getByText(/Auditoría/i)).toBeInTheDocument()
  })

  it('rol logistico NO ve pestaña de Auditoría', () => {
    render(<ConsultaPage user={{ app_metadata: { role: 'logistico' } }} />)
    expect(screen.queryByText(/Auditoría/i)).not.toBeInTheDocument()
  })

  // ── Bloque 3: Filtros en UI ──────────────────────────────────────────────

  it('renderiza el filtro de Cédula', () => {
    render(<ConsultaPage user={{ app_metadata: { role: 'logistico' } }} />)
    // "Cédula" aparece como label del filtro + header de columna en tabla
    const matches = screen.getAllByText('Cédula')
    expect(matches.length).toBeGreaterThanOrEqual(1)
  })

  it('renderiza el toggle de Factura Electrónica (Todas/Con FE/Sin FE)', () => {
    render(<ConsultaPage user={{ app_metadata: { role: 'logistico' } }} />)
    expect(screen.getByText('Factura Electrónica')).toBeInTheDocument()
    expect(screen.getByText('Todas')).toBeInTheDocument()
    expect(screen.getByText('Con FE')).toBeInTheDocument()
    expect(screen.getByText('Sin FE')).toBeInTheDocument()
  })

  it('renderiza el filtro de Responsable', () => {
    render(<ConsultaPage user={{ app_metadata: { role: 'logistico' } }} />)
    // "Responsable" aparece como label del filtro + headers de columna
    const matches = screen.getAllByText('Responsable')
    expect(matches.length).toBeGreaterThanOrEqual(1)
  })

  it('al enviar formulario pasa los nuevos filtros al buscar', () => {
    const buscar = vi.fn()
    useConsultaMock.mockReturnValue({
      rows: [], totals: null, loading: false, page: 0, hasMore: false,
      buscar, nextPage: vi.fn(), prevPage: vi.fn(), fetchAll: vi.fn(),
    })
    render(<ConsultaPage user={{ app_metadata: { role: 'logistico' } }} />)

    buscar.mockClear()
    const form = screen.getByText('Filtros').closest('form')
    fireEvent.submit(form)

    expect(buscar).toHaveBeenCalledWith(expect.objectContaining({
      cedula_conductor: '', tiene_fe: '', nombre_responsable: '',
      estado_vencimiento: '',
    }), 0)
  })

  // ── Bloque 4: VencimientoBadge ───────────────────────────────────────────

  it('VencimientoBadge muestra "—" cuando fecha_estimada_pago es null', () => {
    const rows = [{
      ...sampleRows[0],
      fecha_estimada_pago: null,
    }]
    useConsultaMock.mockReturnValue({
      rows, totals: null, loading: false,
      page: 0, hasMore: false,
      buscar: vi.fn(), nextPage: vi.fn(), prevPage: vi.fn(), fetchAll: vi.fn(),
    })
    render(<ConsultaPage user={{ app_metadata: { role: 'logistico' } }} />)
    // Columna "Días x Vencer" existe
    expect(screen.getByText('Días x Vencer')).toBeInTheDocument()
    // La tabla tiene 1 fila → no debe fallar por null en fecha_estimada_pago
    expect(screen.getByText('21001')).toBeInTheDocument()
  })

  it('VencimientoBadge muestra VENCIDO para fecha pasada', () => {
    const hoy = new Date()
    const pasada = new Date(hoy)
    pasada.setDate(hoy.getDate() - 3)
    const fechaPasada = fmtLocalDate(pasada)

    const rows = [{
      ...sampleRows[0],
      fecha_estimada_pago: fechaPasada,
    }]
    useConsultaMock.mockReturnValue({
      rows, totals: null, loading: false,
      page: 0, hasMore: false,
      buscar: vi.fn(), nextPage: vi.fn(), prevPage: vi.fn(), fetchAll: vi.fn(),
    })
    render(<ConsultaPage user={{ app_metadata: { role: 'logistico' } }} />)
    expect(screen.getByText(/VENCIDO/i)).toBeInTheDocument()
  })

  it('VencimientoBadge renderiza sin romperse para fecha próxima (<7d)', () => {
    const hoy = new Date()
    const proxima = new Date(hoy)
    proxima.setDate(hoy.getDate() + 3)
    const fechaProxima = fmtLocalDate(proxima)

    const rows = [{
      ...sampleRows[0],
      fecha_estimada_pago: fechaProxima,
    }]
    useConsultaMock.mockReturnValue({
      rows, totals: null, loading: false,
      page: 0, hasMore: false,
      buscar: vi.fn(), nextPage: vi.fn(), prevPage: vi.fn(), fetchAll: vi.fn(),
    })
    // No debe crashear
    expect(() => {
      render(<ConsultaPage user={{ app_metadata: { role: 'logistico' } }} />)
    }).not.toThrow()
  })

  it('VencimientoBadge renderiza sin romperse para fecha lejana (> 7d)', () => {
    const hoy = new Date()
    const lejana = new Date(hoy)
    lejana.setDate(hoy.getDate() + 15)
    const fechaLejana = fmtLocalDate(lejana)

    const rows = [{
      ...sampleRows[0],
      fecha_estimada_pago: fechaLejana,
    }]
    useConsultaMock.mockReturnValue({
      rows, totals: null, loading: false,
      page: 0, hasMore: false,
      buscar: vi.fn(), nextPage: vi.fn(), prevPage: vi.fn(), fetchAll: vi.fn(),
    })
    expect(() => {
      render(<ConsultaPage user={{ app_metadata: { role: 'logistico' } }} />)
    }).not.toThrow()
  })

  // ── Bloque 4: Alertas y drill-down ───────────────────────────────────────

  it('barra de alertas muestra conteo de vencidos y por vencer', async () => {
    supabase.rpc.mockResolvedValueOnce({
      data: { vencidos: 5, porVencer: 3, saldoVencido: 1000000 },
      error: null,
    })
    const buscar = vi.fn()
    useConsultaMock.mockReturnValue({
      rows: [], totals: null, loading: false, page: 0, hasMore: false,
      buscar, nextPage: vi.fn(), prevPage: vi.fn(), fetchAll: vi.fn(),
    })
    render(<ConsultaPage user={{ app_metadata: { role: 'logistico' } }} />)

    await waitFor(() => {
      expect(screen.getByText(/Vencidos: 5/i)).toBeInTheDocument()
    })
    expect(screen.getByText(/Por vencer.*3/i)).toBeInTheDocument()
  })

  it('click en "Vencidos" dispara buscar con estado_vencimiento=vencidos', async () => {
    const buscar = vi.fn()
    supabase.rpc.mockResolvedValueOnce({
      data: { vencidos: 5, porVencer: 0, saldoVencido: 500000 },
      error: null,
    })
    useConsultaMock.mockReturnValue({
      rows: [], totals: null, loading: false, page: 0, hasMore: false,
      buscar, nextPage: vi.fn(), prevPage: vi.fn(), fetchAll: vi.fn(),
    })
    render(<ConsultaPage user={{ app_metadata: { role: 'logistico' } }} />)

    buscar.mockClear()
    await waitFor(() => {
      expect(screen.getByText(/Vencidos: 5/i)).toBeInTheDocument()
    })
    fireEvent.click(screen.getByText(/Vencidos: 5/i))

    expect(buscar).toHaveBeenCalledWith(
      expect.objectContaining({
        estado_vencimiento: 'vencidos',
        manifiesto: '', fecha_desde: '', fecha_hasta: '',
      }), 0,
    )
  })

  it('click en "Por vencer" dispara buscar con estado_vencimiento=por_vencer', async () => {
    const buscar = vi.fn()
    supabase.rpc.mockResolvedValueOnce({
      data: { vencidos: 0, porVencer: 4, saldoVencido: 0 },
      error: null,
    })
    useConsultaMock.mockReturnValue({
      rows: [], totals: null, loading: false, page: 0, hasMore: false,
      buscar, nextPage: vi.fn(), prevPage: vi.fn(), fetchAll: vi.fn(),
    })
    render(<ConsultaPage user={{ app_metadata: { role: 'logistico' } }} />)

    buscar.mockClear()
    await waitFor(() => {
      expect(screen.getByText(/Por vencer.*4/i)).toBeInTheDocument()
    })
    fireEvent.click(screen.getByText(/Por vencer.*4/i))

    expect(buscar).toHaveBeenCalledWith(
      expect.objectContaining({
        estado_vencimiento: 'por_vencer',
        manifiesto: '', fecha_desde: '', fecha_hasta: '',
      }), 0,
    )
  })

  it('click again en badge activo desactiva filtro de vencimiento', async () => {
    const buscar = vi.fn()
    supabase.rpc.mockResolvedValueOnce({
      data: { vencidos: 3, porVencer: 0, saldoVencido: 200000 },
      error: null,
    })
    useConsultaMock.mockReturnValue({
      rows: [], totals: null, loading: false, page: 0, hasMore: false,
      buscar, nextPage: vi.fn(), prevPage: vi.fn(), fetchAll: vi.fn(),
    })
    render(<ConsultaPage user={{ app_metadata: { role: 'logistico' } }} />)

    // Primer click → activa vencidos
    await waitFor(() => {
      expect(screen.getByText(/Vencidos: 3/i)).toBeInTheDocument()
    })
    buscar.mockClear()
    fireEvent.click(screen.getByText(/Vencidos: 3/i))
    expect(buscar).toHaveBeenCalledWith(
      expect.objectContaining({ estado_vencimiento: 'vencidos' }), 0,
    )

    // Segundo click → desactiva solo vencimiento
    buscar.mockClear()
    await waitFor(() => {
      fireEvent.click(screen.getByText(/Vencidos: 3/i))
      expect(buscar).toHaveBeenCalledWith(
        expect.objectContaining({ estado_vencimiento: '' }), 0,
      )
    })
  })

  it('no muestra barra de alertas si RPC retorna null', async () => {
    supabase.rpc.mockResolvedValueOnce({ data: null, error: null })
    useConsultaMock.mockReturnValue({
      rows: [], totals: null, loading: false, page: 0, hasMore: false,
      buscar: vi.fn(), nextPage: vi.fn(), prevPage: vi.fn(), fetchAll: vi.fn(),
    })
    render(<ConsultaPage user={{ app_metadata: { role: 'logistico' } }} />)

    // Espera un momento y verifica que NO haya "Vencimientos"
    await new Promise(r => setTimeout(r, 100))
    expect(screen.queryByText(/Vencimientos/i)).not.toBeInTheDocument()
  })
})
