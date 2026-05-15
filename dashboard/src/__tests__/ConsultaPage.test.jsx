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

describe('ConsultaPage', () => {
  beforeEach(() => {
    useConsultaMock.mockReset()
    useConsultaMock.mockReturnValue({
      rows: sampleRows, totals: null, loading: false,
      page: 0, hasMore: false,
      buscar: vi.fn(), nextPage: vi.fn(), prevPage: vi.fn(),
    })
  })

  it('renderiza el panel de filtros', () => {
    render(<ConsultaPage user={{ app_metadata: { role: 'operativo' } }} />)
    expect(screen.getByText(/Filtros de consulta/i)).toBeInTheDocument()
    expect(screen.getByText(/Limpiar todo/i)).toBeInTheDocument()
  })

  it('al montar dispara buscar con filtros iniciales (mes=null, etc)', () => {
    const buscar = vi.fn()
    useConsultaMock.mockReturnValue({
      rows: [], totals: null, loading: false, page: 0, hasMore: false,
      buscar, nextPage: vi.fn(), prevPage: vi.fn(),
    })
    render(<ConsultaPage user={{ app_metadata: { role: 'operativo' } }} />)

    expect(buscar).toHaveBeenCalledWith(expect.objectContaining({
      manifiesto: '', fecha_desde: '', fecha_hasta: '',
      mes: '', año: '',
    }), 0)
  })

  it('cambiar manifiesto en input y submit dispara buscar con el valor', () => {
    const buscar = vi.fn()
    useConsultaMock.mockReturnValue({
      rows: [], totals: null, loading: false, page: 0, hasMore: false,
      buscar, nextPage: vi.fn(), prevPage: vi.fn(),
    })
    render(<ConsultaPage user={{ app_metadata: { role: 'operativo' } }} />)

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
      buscar, nextPage: vi.fn(), prevPage: vi.fn(),
    })
    render(<ConsultaPage user={{ app_metadata: { role: 'operativo' } }} />)

    buscar.mockClear()
    fireEvent.click(screen.getByText(/Limpiar todo/i))

    expect(buscar).toHaveBeenCalledWith(
      expect.objectContaining({ manifiesto: '', fecha_desde: '' }), 0,
    )
  })

  it('rol admin ve la pestaña de Auditoría', () => {
    render(<ConsultaPage user={{ app_metadata: { role: 'admin' } }} />)
    expect(screen.getByText(/Auditoría/i)).toBeInTheDocument()
  })

  it('rol operativo NO ve pestaña de Auditoría', () => {
    render(<ConsultaPage user={{ app_metadata: { role: 'operativo' } }} />)
    expect(screen.queryByText(/Auditoría/i)).not.toBeInTheDocument()
  })
})
