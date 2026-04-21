import { useState } from 'react'
import {
  ResponsiveContainer, LineChart, Line, BarChart, Bar,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  PieChart, Pie, Cell,
} from 'recharts'
import { useDashboard } from '../hooks/useDashboard'

const MESES       = ['ENERO','FEBRERO','MARZO','ABRIL','MAYO','JUNIO','JULIO','AGOSTO','SEPTIEMBRE','OCTUBRE','NOVIEMBRE','DICIEMBRE']
const MESES_CORTO = ['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic']
const AÑOS        = [2023, 2024, 2025, 2026]

// Paleta corporativa Altrans
const BLUE   = '#1E6FBF'
const GOLD   = '#C9A84C'
const ALERT  = '#E05252'
const TICK   = '#F0F4F8'
const GRID   = '#2A3F52'
const TT_BG  = '#1B2B3B'
const TT_BDR = '#2A3F52'

const CHART_COLORS = [BLUE, GOLD, '#22c55e', ALERT, '#a855f7', '#14b8a6', '#f97316', '#ec4899', '#6366f1', '#84cc16']

const TOOLTIP_STYLE = { borderRadius: 8, fontSize: 12, background: TT_BG, border: `1px solid ${TT_BDR}`, color: TICK }
const TICK_SM  = { fontSize: 11, fill: TICK }
const TICK_XS  = { fontSize: 10, fill: TICK }
const TICK_XXS = { fontSize: 9,  fill: TICK }

const fmtCOP = v => new Intl.NumberFormat('es-CO', { style: 'currency', currency: 'COP', maximumFractionDigits: 0 }).format(v)
const fmtK   = v => v >= 1_000_000 ? `$${(v / 1_000_000).toFixed(1)}M` : v >= 1_000 ? `$${(v / 1_000).toFixed(0)}K` : String(v)

function Skeleton() {
  return <div className="h-7 w-28 rounded bg-muted animate-pulse" />
}

function KpiCard({ label, value, textColor, borderColor, loading }) {
  return (
    <div
      className="rounded-lg border bg-card p-4 flex flex-col gap-2 border-l-[3px] shadow-md"
      style={{ borderColor: TT_BDR, borderLeftColor: borderColor }}
    >
      <p className="text-[11px] font-medium text-muted-foreground uppercase tracking-wide">{label}</p>
      {loading ? <Skeleton /> : <p className="text-2xl font-semibold" style={{ color: textColor }}>{value}</p>}
    </div>
  )
}

function SectionLabel({ children }) {
  return (
    <div className="flex items-center gap-3 -mb-2">
      <p className="text-[10px] font-bold text-muted-foreground uppercase tracking-widest">{children}</p>
      <div className="flex-1 h-px" style={{ background: GRID }} />
    </div>
  )
}

function ChartCard({ title, children }) {
  return (
    <div className="rounded-lg p-4 flex flex-col gap-3 shadow-md" style={{ background: TT_BG, border: `1px solid ${TT_BDR}` }}>
      <p className="text-sm font-semibold" style={{ color: TICK }}>{title}</p>
      <div className="-mx-4" style={{ height: 1, background: GRID }} />
      {children}
    </div>
  )
}

function FilterPill({ label, active, onClick }) {
  return (
    <button
      onClick={onClick}
      className="px-3 py-1 rounded-full text-xs font-medium transition-all duration-150"
      style={active
        ? { background: BLUE, color: TICK }
        : { background: '#162232', color: '#8FA3B1' }
      }
    >
      {label}
    </button>
  )
}

const hoy = new Date()

export default function DashboardPage() {
  const [mesIdx, setMesIdx] = useState(hoy.getMonth())
  const [año,    setAño]    = useState(hoy.getFullYear())

  const mes = mesIdx !== null ? MESES[mesIdx] : null

  const { data, loading } = useDashboard(mes, año)

  const periodoLabel = [mes ?? 'Todos los meses', año ? String(año) : 'Todos los años'].join(' · ')

  const financieros = [
    { label: 'Total remesas',       value: fmtCOP(data?.totalRemesas   ?? 0), textColor: GOLD,  borderColor: GOLD  },
    { label: 'Total fletes',        value: fmtCOP(data?.totalFletes    ?? 0), textColor: GOLD,  borderColor: GOLD  },
    { label: 'Total anticipos',     value: fmtCOP(data?.totalAnticipo  ?? 0), textColor: GOLD,  borderColor: GOLD  },
    { label: 'Pendiente por pagar', value: fmtCOP(data?.pendientePagar ?? 0), textColor: GOLD,  borderColor: GOLD  },
  ]

  const operativos = [
    { label: 'Manifiestos',         value: data?.totalManifiestos,  textColor: TICK,  borderColor: BLUE  },
    { label: 'Anulados',            value: data?.anulados,           textColor: ALERT, borderColor: ALERT },
    { label: 'Conductores activos', value: data?.conductoresActivos, textColor: TICK,  borderColor: BLUE  },
    { label: 'Rutas activas',       value: data?.rutasActivas,       textColor: TICK,  borderColor: BLUE  },
  ]

  return (
    <div className="flex flex-col gap-6">

      {/* Filtros */}
      <div className="flex flex-col gap-3 pb-1">
        <p className="text-xs text-muted-foreground">{periodoLabel}</p>

        <div className="flex gap-1.5 items-center">
          <span className="text-[10px] font-semibold text-muted-foreground uppercase tracking-widest w-8">Año</span>
          <FilterPill label="Todos" active={año === null} onClick={() => setAño(null)} />
          {AÑOS.map(a => <FilterPill key={a} label={String(a)} active={año === a} onClick={() => setAño(a)} />)}
        </div>

        <div className="flex gap-1.5 items-center flex-wrap">
          <span className="text-[10px] font-semibold text-muted-foreground uppercase tracking-widest w-8">Mes</span>
          <FilterPill label="Todos" active={mesIdx === null} onClick={() => setMesIdx(null)} />
          {MESES_CORTO.map((label, i) => (
            <FilterPill key={i} label={label} active={mesIdx === i} onClick={() => setMesIdx(i)} />
          ))}
        </div>
      </div>

      {/* Row 1: KPIs financieros */}
      <SectionLabel>Financiero</SectionLabel>
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
        {financieros.map(k => <KpiCard key={k.label} loading={loading} {...k} />)}
      </div>

      {/* Row 2: KPIs operativos */}
      <SectionLabel>Operativo</SectionLabel>
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
        {operativos.map(k => <KpiCard key={k.label} loading={loading} {...k} />)}
      </div>

      {/* Row 3: Línea tendencia */}
      <SectionLabel>Tendencia anual</SectionLabel>
      <ChartCard title={`Facturado vs Ganancia bruta${año ? ` — ${año}` : ''}`}>
        <ResponsiveContainer width="100%" height={260}>
          <LineChart data={data?.lineChart ?? []} margin={{ top: 5, right: 20, left: 10, bottom: 5 }}>
            <CartesianGrid stroke={GRID} strokeDasharray="3 3" opacity={0.6} />
            <XAxis dataKey="mes" tick={TICK_SM} tickLine={false} axisLine={false} />
            <YAxis tickFormatter={fmtK} tick={TICK_SM} width={70} axisLine={false} tickLine={false} />
            <Tooltip contentStyle={TOOLTIP_STYLE}
              formatter={(v, name) => [fmtCOP(v), name === 'facturado' ? 'Facturado' : 'Ganancia bruta']} />
            <Legend formatter={v => v === 'facturado' ? 'Facturado' : 'Ganancia bruta'} />
            <Line type="monotone" dataKey="facturado" stroke={BLUE} strokeWidth={2.5} dot={false} />
            <Line type="monotone" dataKey="ganancia"  stroke={GOLD} strokeWidth={2.5} dot={false} strokeDasharray="5 3" />
          </LineChart>
        </ResponsiveContainer>
      </ChartCard>

      {/* Row 4 */}
      <SectionLabel>Distribución del período</SectionLabel>
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">

        <ChartCard title="Estado de pago">
          <ResponsiveContainer width="100%" height={240}>
            <PieChart>
              <Pie data={data?.estadoPago ?? []} dataKey="value" nameKey="name"
                cx="50%" cy="48%" innerRadius={52} outerRadius={88}
                label={({ percent }) => percent > 0.06 ? `${(percent * 100).toFixed(0)}%` : ''}
                labelLine={false} paddingAngle={2}
              >
                {(data?.estadoPago ?? []).map((_, i) => <Cell key={i} fill={CHART_COLORS[i % CHART_COLORS.length]} />)}
              </Pie>
              <Tooltip contentStyle={TOOLTIP_STYLE} />
            </PieChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="Top clientes">
          <ResponsiveContainer width="100%" height={240}>
            <BarChart layout="vertical" data={data?.topClientes ?? []} margin={{ left: 0, right: 16, top: 4, bottom: 4 }}>
              <CartesianGrid stroke={GRID} strokeDasharray="3 3" opacity={0.6} horizontal={false} />
              <XAxis type="number" tick={TICK_SM} axisLine={false} tickLine={false} />
              <YAxis type="category" dataKey="nombre" width={110} tick={TICK_XS} axisLine={false} tickLine={false} />
              <Tooltip contentStyle={TOOLTIP_STYLE} />
              <Bar dataKey="count" name="Manifiestos" fill={BLUE} radius={[0, 6, 6, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="Top rutas">
          <ResponsiveContainer width="100%" height={240}>
            <BarChart layout="vertical" data={data?.topRutas ?? []} margin={{ left: 0, right: 16, top: 4, bottom: 4 }}>
              <CartesianGrid stroke={GRID} strokeDasharray="3 3" opacity={0.6} horizontal={false} />
              <XAxis type="number" tick={TICK_SM} axisLine={false} tickLine={false} />
              <YAxis type="category" dataKey="ruta" width={150} tick={TICK_XXS} axisLine={false} tickLine={false} />
              <Tooltip contentStyle={TOOLTIP_STYLE} />
              <Bar dataKey="count" name="Manifiestos" fill={GOLD} radius={[0, 6, 6, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

      </div>

      {/* Row 5 */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">

        <ChartCard title="Por agencia">
          <ResponsiveContainer width="100%" height={240}>
            <BarChart data={data?.chartAgencias ?? []} margin={{ top: 8, right: 16, bottom: 4 }}>
              <CartesianGrid stroke={GRID} strokeDasharray="3 3" opacity={0.6} vertical={false} />
              <XAxis dataKey="nombre" tick={TICK_SM} axisLine={false} tickLine={false} />
              <YAxis tick={TICK_SM} axisLine={false} tickLine={false} />
              <Tooltip contentStyle={TOOLTIP_STYLE} />
              <Bar dataKey="count" name="Manifiestos" fill={BLUE} radius={[6, 6, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="Estado interno">
          <ResponsiveContainer width="100%" height={240}>
            <PieChart>
              <Pie data={data?.chartEstadoInterno ?? []} dataKey="value" nameKey="name"
                cx="50%" cy="48%" innerRadius={52} outerRadius={88}
                label={({ percent }) => percent > 0.06 ? `${(percent * 100).toFixed(0)}%` : ''}
                labelLine={false} paddingAngle={2}
              >
                {(data?.chartEstadoInterno ?? []).map((_, i) => <Cell key={i} fill={CHART_COLORS[i % CHART_COLORS.length]} />)}
              </Pie>
              <Tooltip contentStyle={TOOLTIP_STYLE}
                formatter={(v, name) => [v, name.length > 24 ? name.slice(0, 24) + '…' : name]} />
            </PieChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="Top conductores">
          <ResponsiveContainer width="100%" height={240}>
            <BarChart layout="vertical" data={data?.topConductores ?? []} margin={{ left: 0, right: 16, top: 4, bottom: 4 }}>
              <CartesianGrid stroke={GRID} strokeDasharray="3 3" opacity={0.6} horizontal={false} />
              <XAxis type="number" tick={TICK_SM} axisLine={false} tickLine={false} />
              <YAxis type="category" dataKey="nombre" width={120} tick={TICK_XS} axisLine={false} tickLine={false} />
              <Tooltip contentStyle={TOOLTIP_STYLE} />
              <Bar dataKey="count" name="Manifiestos" fill={GOLD} radius={[0, 6, 6, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

      </div>

    </div>
  )
}
