import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'

// Mock del hook ANTES de importar la página (evita el fetch real)
const mockData = {
  totalManifiestos: 100, anulados: 5,
  conductoresActivos: 20, rutasActivas: 15,
  totalRemesas: 50_000_000, totalFletes: 30_000_000,
  totalAnticipo: 10_000_000, pendientePagar: 8_000_000,
  sinFechaCumplido: 3, sinFactura: 7, conNovedad: 2, diasPromFacturar: 5,
  vencidos: 8, porVencer: 4, saldoVencido: 2_500_000,
  lineChart: [
    { mes: 'ENE', facturado: 0, ganancia: 0 },
    { mes: 'FEB', facturado: 0, ganancia: 0 },
    { mes: 'MAR', facturado: 100, ganancia: 10 },
    { mes: 'ABR', facturado: 200, ganancia: 20 },
    { mes: 'MAY', facturado: 300, ganancia: 30 },
    { mes: 'JUN', facturado: 0, ganancia: 0 },
    { mes: 'JUL', facturado: 0, ganancia: 0 },
    { mes: 'AGO', facturado: 0, ganancia: 0 },
    { mes: 'SEP', facturado: 0, ganancia: 0 },
    { mes: 'OCT', facturado: 0, ganancia: 0 },
    { mes: 'NOV', facturado: 0, ganancia: 0 },
    { mes: 'DIC', facturado: 0, ganancia: 0 },
  ],
  estadoPago: [{ name: 'PAGADO', value: 50 }, { name: 'PENDIENTE', value: 30 }],
  topClientes:    [{ nombre: 'ACME', count: 30 }],
  topRutas:       [{ ruta: 'CALI → BOGOTA', count: 50 }],
  chartAgencias:  [{ nombre: 'CALI', count: 80 }],
  chartEstadoInterno: [{ name: 'CUMPLIDO', value: 90 }],
  topConductores: [{ nombre: 'JUAN', count: 25 }],
}

const useDashboardMock = vi.fn()
vi.mock('../hooks/useDashboard', () => ({
  useDashboard: (...args) => useDashboardMock(...args),
}))

// Stub recharts: jsdom no mide tamaño, ResponsiveContainer no renderiza hijos.
// Reemplazamos los exports concretos que usa DashboardPage por divs simples.
vi.mock('recharts', () => {
  const Stub = ({ children }) => <div data-testid="chart">{children}</div>
  return {
    ResponsiveContainer: Stub,
    LineChart: Stub, Line: Stub,
    BarChart: Stub,  Bar:  Stub,
    PieChart: Stub,  Pie:  Stub,
    XAxis: Stub, YAxis: Stub,
    CartesianGrid: Stub, Tooltip: Stub, Legend: Stub, Cell: Stub,
  }
})

import DashboardPage from '../pages/DashboardPage'

const defaultUser = { app_metadata: { role: 'gerencia' } }

describe('DashboardPage', () => {
  beforeEach(() => {
    useDashboardMock.mockReset()
  })

  it('renderiza KPIs financieros y operativos en formato $', async () => {
    useDashboardMock.mockReturnValue({ data: mockData, loading: false })

    render(<DashboardPage user={defaultUser} />)

    await waitFor(() => {
      // Formato COP: $50.000.000 (o variantes)
      expect(screen.getByText(/Total remesas/i)).toBeInTheDocument()
      expect(screen.getByText(/Total fletes/i)).toBeInTheDocument()
      expect(screen.getByText(/Pendiente por pagar/i)).toBeInTheDocument()
    })

    // Operativos
    expect(screen.getByText('Manifiestos')).toBeInTheDocument()
    expect(screen.getByText('100')).toBeInTheDocument()  // totalManifiestos
    expect(screen.getByText('5')).toBeInTheDocument()    // anulados
    expect(screen.getByText('Anulados')).toBeInTheDocument()
  })

  it('cambiar mes/año dispara nueva llamada al hook', () => {
    useDashboardMock.mockReturnValue({ data: mockData, loading: false })
    render(<DashboardPage user={defaultUser} />)

    // Hay un botón "Ene" en los filtros de mes
    const eneBtn = screen.getAllByText('Ene')[0]
    fireEvent.click(eneBtn)

    // El hook se vuelve a invocar con ENERO
    expect(useDashboardMock).toHaveBeenCalledWith('ENERO', expect.any(Number))
  })

  it('botón "Todos los meses" pone mes=null', () => {
    useDashboardMock.mockReturnValue({ data: mockData, loading: false })
    render(<DashboardPage user={defaultUser} />)

    // hay un "Todos" para mes — está en el segundo grupo
    const todosBtns = screen.getAllByText('Todos')
    // El segundo "Todos" es el de meses (el primero es de años)
    fireEvent.click(todosBtns[1])

    expect(useDashboardMock).toHaveBeenLastCalledWith(null, expect.any(Number))
  })

  it('con data=null muestra placeholders (no rompe)', () => {
    useDashboardMock.mockReturnValue({ data: null, loading: true })
    // No debe lanzar
    expect(() => render(<DashboardPage user={defaultUser} />)).not.toThrow()
  })

  // ── Bloque 4: KPIs de Vencimientos ───────────────────────────────────────

  it('renderiza sección Vencimientos con 3 KPIs', async () => {
    useDashboardMock.mockReturnValue({ data: mockData, loading: false })
    render(<DashboardPage user={defaultUser} />)

    await waitFor(() => {
      expect(screen.getByText(/Vencimientos/i)).toBeInTheDocument()
    })
    expect(screen.getByText('Vencidos')).toBeInTheDocument()
    expect(screen.getByText('Por vencer (<7d)')).toBeInTheDocument()
    expect(screen.getByText('Al día')).toBeInTheDocument()
  })

  it('KPI Vencidos muestra el valor del RPC', () => {
    useDashboardMock.mockReturnValue({ data: mockData, loading: false })
    render(<DashboardPage user={defaultUser} />)
    expect(screen.getByText('8')).toBeInTheDocument()
    expect(screen.getByText('4')).toBeInTheDocument()
  })

  it('KPI Al día se calcula como total - anulados - vencidos - porVencer', () => {
    // mockData: total=100, anulados=5, vencidos=8, porVencer=4
    // Al día = 100 - 5 - 8 - 4 = 83
    useDashboardMock.mockReturnValue({ data: mockData, loading: false })
    render(<DashboardPage user={defaultUser} />)
    expect(screen.getByText('83')).toBeInTheDocument()
  })

  it('KPI Al día nunca es negativo (Math.max con 0)', () => {
    const inviable = {
      ...mockData,
      totalManifiestos: 5, anulados: 0, vencidos: 8, porVencer: 2,
    }
    useDashboardMock.mockReturnValue({ data: inviable, loading: false })
    render(<DashboardPage user={defaultUser} />)
    // Al día = max(0, 5 - 0 - 8 - 2) = 0. Tanto Vencidos como Al día son 0.
    const ceros = screen.getAllByText('0')
    expect(ceros.length).toBeGreaterThanOrEqual(1)
  })

  it('si data no tiene vencidos/porVencer usa 0 por defecto', () => {
    // Sin los nuevos campos: deben caer a 0
    const v4 = {
      ...mockData,
      vencidos: undefined, porVencer: undefined,
    }
    useDashboardMock.mockReturnValue({ data: v4, loading: false })
    expect(() => render(<DashboardPage user={defaultUser} />)).not.toThrow()
    // total=100 anulados=5 → 100 - 5 = 95
    expect(screen.getByText('95')).toBeInTheDocument()
  })

  it('roles sin privilegios NO ven el Dashboard (ven lock)', () => {
    useDashboardMock.mockReturnValue({ data: mockData, loading: false })
    render(<DashboardPage user={{ app_metadata: { role: 'logistico' } }} />)
    expect(screen.getByText(/Acceso restringido/i)).toBeInTheDocument()
    expect(screen.queryByText('Vencidos')).not.toBeInTheDocument()
  })
})
