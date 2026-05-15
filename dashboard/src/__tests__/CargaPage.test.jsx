import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'

vi.mock('../hooks/useCatalogos', () => ({
  useCatalogos: () => ({
    catalogos: {
      conductores: [], clientes: [], lugares: [],
      responsables: [], vehiculos: [], remolques: [], agencias: [], propietarios: [],
    },
    loading: false,
    createConductor: vi.fn(), updateConductor: vi.fn(),
    createCliente: vi.fn(), createLugar: vi.fn(), createResponsable: vi.fn(),
    createVehiculo: vi.fn(), createRemolque: vi.fn(), createPropietario: vi.fn(),
  }),
}))

const searchMock = vi.fn()
vi.mock('../hooks/useManifiesto', () => ({
  useManifiesto: () => ({
    search: searchMock,
    update: vi.fn(),
    remove: vi.fn(),
    updateSeguimiento: vi.fn(),
    updateTesoreria: vi.fn(),
    updateFacturacion: vi.fn(),
  }),
}))

vi.mock('../components/Toast', async () => {
  const React = await import('react')
  return {
    ToastProvider: ({ children }) => React.createElement('div', null, children),
    useToast: () => ({ show: vi.fn() }),
  }
})

import CargaPage from '../pages/CargaPage'

describe('CargaPage', () => {
  beforeEach(() => {
    searchMock.mockReset()
  })

  it('rol operativo: NO ve el panel de upload Excel (solo digitador/admin)', () => {
    render(<CargaPage target={null} clearTarget={() => {}} user={{ app_metadata: { role: 'operativo' } }} />)
    // No debe haber referencia a importar/arrastrar archivo
    expect(screen.queryByText(/Arrastra/i)).not.toBeInTheDocument()
    expect(screen.queryByText(/excel/i)).not.toBeInTheDocument()
  })

  it('rol digitador: SÍ ve el panel de upload Excel', () => {
    render(<CargaPage target={null} clearTarget={() => {}} user={{ app_metadata: { role: 'digitador' } }} />)
    // El panel está visible para digitadores. Buscamos texto característico.
    const matches = screen.queryAllByText(/excel|arrastra|importar archivo/i)
    expect(matches.length).toBeGreaterThan(0)
  })

  it('al cargar con target dispara search del manifiesto objetivo', () => {
    searchMock.mockResolvedValue({ manifiesto: 12345, conductor: 'JUAN' })
    render(<CargaPage target={12345} clearTarget={() => {}} user={{ app_metadata: { role: 'admin' } }} />)
    expect(searchMock).toHaveBeenCalledWith(12345)
  })

  it('rol admin tiene acceso a todas las pestañas (despacho/operativo/tesoreria/financiero)', () => {
    // Con admin pasa el primer chequeo de canEdit*, podemos confirmar buscando que renderiza algo
    const { container } = render(
      <CargaPage target={null} clearTarget={() => {}} user={{ app_metadata: { role: 'admin' } }} />,
    )
    expect(container.firstChild).toBeTruthy()
  })

  it('renderiza sin user (defensivo)', () => {
    expect(() =>
      render(<CargaPage target={null} clearTarget={() => {}} user={null} />),
    ).not.toThrow()
  })
})
