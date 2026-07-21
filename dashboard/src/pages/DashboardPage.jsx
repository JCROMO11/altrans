import { useState } from 'react'
import { Lock } from 'lucide-react'
import {
  ResponsiveContainer, LineChart, Line, BarChart, Bar,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  PieChart, Pie, Cell,
} from 'recharts'
import { useDashboard } from '../hooks/useDashboard'

const MESES       = ['ENERO','FEBRERO','MARZO','ABRIL','MAYO','JUNIO','JULIO','AGOSTO','SEPTIEMBRE','OCTUBRE','NOVIEMBRE','DICIEMBRE']
const MESES_CORTO = ['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic']
const AÑOS_BASE   = 2023
const AÑOS        = Array.from({ length: new Date().getFullYear() - AÑOS_BASE + 1 }, (_, i) => AÑOS_BASE + i)

// Paleta corporativa Altrans
const BLUE   = '#1E6FBF'
const GOLD   = '#C9A84C'
const ALERT  = '#E05252'
const GREEN  = '#16A34A'
const AMBAR  = '#F59E0B'
const TICK   = '#0F172A'
const GRID   = '#E2E8F0'
const TT_BG  = '#FFFFFF'
const TT_BDR = '#E2E8F0'

const CHART_COLORS = [BLUE, GOLD, '#22c55e', ALERT, '#a855f7', '#14b8a6', '#f97316', '#ec4899', '#6366f1', '#84cc16']

const TOOLTIP_STYLE = { borderRadius: 8, fontSize: 12, background: TT_BG, border: `1px solid ${TT_BDR}`, color: TICK }
const TICK_SM  = { fontSize: 11, fill: TICK }
const TICK_XS  = { fontSize: 10, fill: TICK }
const TICK_XXS = { fontSize: 9,  fill: TICK }

const fmtCOP = v => new Intl.NumberFormat('es-CO', { style: 'currency', currency: 'COP', maximumFractionDigits: 0 }).format(v)
const fmtK   = v => v >= 1_000_000 ? `$${(v / 1_000_000).toFixed(1)}M` : v >= 1_000 ? `$${(v / 1_000).toFixed(0)}K` : String(v)

// Formato compacto para KPIs grandes: > 10M usa abreviatura, si no formato COP completo.
const fmtKpi = v => {
  if (typeof v !== 'number' || !isFinite(v)) return fmtCOP(0)
  if (v >= 1_000_000_000) return `$${(v / 1_000_000_000).toFixed(2)}MM`
  if (v >= 10_000_000)    return `$${(v / 1_000_000).toFixed(1)}M`
  return fmtCOP(v)
}

function Skeleton() {
  return <div className="h-7 w-28 rounded bg-muted animate-pulse" />
}

function ChartSkeleton({ height = 240 }) {
  // Barras grises animadas mientras carga
  const bars = [60, 80, 45, 90, 55, 75, 40, 85]
  return (
    <div className="flex items-end gap-2 px-2 animate-pulse" style={{ height }}>
      {bars.map((h, i) => (
        <div key={i} className="flex-1 rounded-t bg-muted" style={{ height: `${h}%`, opacity: 0.4 + (i % 3) * 0.2 }} />
      ))}
    </div>
  )
}

function KpiCard({ label, value, textColor, borderColor, loading }) {
  return (
    <div
      className="rounded-lg border p-4 flex flex-col gap-2 border-l-[3px] shadow-md transition-all duration-200 hover:shadow-lg hover:-translate-y-0.5"
      style={{
        borderColor: TT_BDR,
        borderLeftColor: borderColor,
        background: `linear-gradient(135deg, ${borderColor}0D 0%, #FFFFFF 60%)`,
      }}
    >
      <p className="text-[11px] font-medium text-muted-foreground uppercase tracking-wide">{label}</p>
      {loading ? <Skeleton /> : <p className="text-2xl font-semibold tracking-tight" style={{ color: textColor }}>{value}</p>}
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
    <div className="rounded-lg p-4 flex flex-col gap-3 shadow-md transition-shadow hover:shadow-lg" style={{ background: 'linear-gradient(135deg, #FFFFFF 0%, #F8FAFC 100%)', border: `1px solid ${TT_BDR}` }}>
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
        ? { background: 'linear-gradient(135deg, #1E6FBF 0%, #6366F1 100%)', color: '#FFFFFF', boxShadow: '0 2px 8px 0 rgba(30,111,191,0.22)' }
        : { background: '#F1F5F9', color: '#64748B' }
      }
    >
      {label}
    </button>
  )
}

const hoy = new Date()

const KPI_ROLES = ['gerencia', 'financiero', 'administrativo']

export default function DashboardPage({ user }) {
  const rol = user?.app_metadata?.role || ''
  const [mesIdx, setMesIdx] = useState(hoy.getMonth())
  const [año,    setAño]    = useState(hoy.getFullYear())

  const mes = mesIdx !== null ? MESES[mesIdx] : null

  const { data, loading } = useDashboard(mes, año)

  const periodoLabel = [mes ?? 'Todos los meses', año ? String(año) : 'Todos los años'].join(' · ')

  if (!KPI_ROLES.includes(rol)) {
    return (
      <div className="flex items-center justify-center" style={{ minHeight: 420 }}>
        <div className="flex flex-col items-center gap-5 text-center max-w-xs px-8 py-12 rounded-2xl"
          style={{
            background: 'linear-gradient(135deg, #EFF6FF 0%, #F5F3FF 50%, #FDF4FF 100%)',
            border: '1px solid #BFDBFE',
            boxShadow: '0 4px 24px 0 rgba(99,102,241,0.08)',
          }}>
          <div className="w-16 h-16 rounded-2xl flex items-center justify-center"
            style={{
              background: 'linear-gradient(135deg, #3B82F6 0%, #8B5CF6 100%)',
              boxShadow: '0 4px 16px 0 rgba(139,92,246,0.30)',
            }}>
            <Lock size={26} color="#FFFFFF" strokeWidth={2.5} />
          </div>
          <div className="flex flex-col gap-2">
            <p className="text-base font-bold" style={{ color: '#1E1B4B' }}>
              Acceso restringido
            </p>
            <p className="text-sm leading-relaxed" style={{ color: '#4338CA' }}>
              Esta sección no está disponible para tu perfil.
            </p>
            <p className="text-xs mt-1" style={{ color: '#6B7280' }}>
              Contacta a gerencia si necesitas acceso.
            </p>
          </div>
        </div>
      </div>
    )
  }

  const financieros = [
    { label: 'Total remesas',       value: fmtKpi(data?.totalRemesas   ?? 0), textColor: GOLD,  borderColor: GOLD  },
    { label: 'Total fletes',        value: fmtKpi(data?.totalFletes    ?? 0), textColor: GOLD,  borderColor: GOLD  },
    { label: 'Total anticipos',     value: fmtKpi(data?.totalAnticipo  ?? 0), textColor: GOLD,  borderColor: GOLD  },
    { label: 'Pendiente por pagar', value: fmtKpi(data?.pendientePagar ?? 0), textColor: GOLD,  borderColor: GOLD  },
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
        <p className="text-base font-semibold" style={{ color: TICK }}>{periodoLabel}</p>

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

      {/* Row 3: KPIs de vencimientos */}
      <SectionLabel>Vencimientos</SectionLabel>
      <div className="grid grid-cols-2 sm:grid-cols-3 gap-4">
        <KpiCard label="Vencidos"
          value={data?.vencidos ?? 0}
          textColor={ALERT} borderColor={ALERT}
          loading={loading} />
        <KpiCard label="Por vencer (&lt;7d)"
          value={data?.porVencer ?? 0}
          textColor={AMBAR} borderColor={AMBAR}
          loading={loading} />
        <KpiCard label="Al día"
          value={Math.max(0, (data?.totalManifiestos ?? 0) - (data?.anulados ?? 0) - (data?.vencidos ?? 0) - (data?.porVencer ?? 0))}
          textColor={GREEN} borderColor={GREEN}
          loading={loading} />
      </div>

      {/* Row 4: Línea tendencia */}
      <SectionLabel>Tendencia anual</SectionLabel>
      <ChartCard title={`Facturado vs Ganancia bruta${año ? ` — ${año}` : ''}`}>
        {loading ? <ChartSkeleton /> : (
          <ResponsiveContainer width="100%" height={240}>
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
        )}
      </ChartCard>

      {/* Row 4 */}
      <SectionLabel>Distribución del período</SectionLabel>
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">

        <ChartCard title="Compromiso de pago">
          {loading ? <ChartSkeleton /> : (
            <ResponsiveContainer width="100%" height={230}>
              <PieChart margin={{ top: 24, right: 32, bottom: 24, left: 32 }}>
                <Pie data={data?.estadoPago ?? []} dataKey="value" nameKey="name"
                  cx="50%" cy="50%" innerRadius={58} outerRadius={92}
                  label={({ cx, cy, midAngle, innerRadius, outerRadius, percent }) => {
                    if (percent < 0.05) return null
                    const r = innerRadius + (outerRadius - innerRadius) * 0.5
                    const x = cx + r * Math.cos(-midAngle * Math.PI / 180)
                    const y = cy + r * Math.sin(-midAngle * Math.PI / 180)
                    return (
                      <text x={x} y={y} fill="#fff" textAnchor="middle" dominantBaseline="central"
                        style={{ fontSize: 11, fontWeight: 700, pointerEvents: 'none' }}>
                        {`${(percent * 100).toFixed(0)}%`}
                      </text>
                    )
                  }}
                  labelLine={false} paddingAngle={3}
                >
                  {(data?.estadoPago ?? []).map((_, i) => <Cell key={i} fill={CHART_COLORS[i % CHART_COLORS.length]} />)}
                </Pie>
                <Tooltip
                  contentStyle={{ ...TOOLTIP_STYLE, borderRadius: 10, padding: '10px 14px', minWidth: 160 }}
                  itemStyle={{ fontSize: 12, color: TICK, fontWeight: 600 }}
                  labelStyle={{ display: 'none' }}
                  formatter={(value, name) => [`${value} ${value === 1 ? 'manifiesto' : 'manifiestos'}`, name]}
                />
              </PieChart>
            </ResponsiveContainer>
          )}
        </ChartCard>

        <ChartCard title="Top clientes">
          {loading ? <ChartSkeleton /> : (
            <ResponsiveContainer width="100%" height={240}>
              <BarChart layout="vertical" data={data?.topClientes ?? []} margin={{ left: 0, right: 16, top: 4, bottom: 4 }}>
                <CartesianGrid stroke={GRID} strokeDasharray="3 3" opacity={0.6} horizontal={false} />
                <XAxis type="number" tick={TICK_SM} axisLine={false} tickLine={false} />
                <YAxis type="category" dataKey="nombre" width={110} tick={TICK_XS} axisLine={false} tickLine={false} />
                <Tooltip contentStyle={TOOLTIP_STYLE} />
                <Bar dataKey="count" name="Manifiestos" fill={BLUE} radius={[0, 6, 6, 0]} />
              </BarChart>
            </ResponsiveContainer>
          )}
        </ChartCard>

        <ChartCard title="Top rutas">
          {loading ? <ChartSkeleton /> : (
            <ResponsiveContainer width="100%" height={240}>
              <BarChart layout="vertical" data={data?.topRutas ?? []} margin={{ left: 0, right: 16, top: 4, bottom: 4 }}>
                <CartesianGrid stroke={GRID} strokeDasharray="3 3" opacity={0.6} horizontal={false} />
                <XAxis type="number" tick={TICK_SM} axisLine={false} tickLine={false} />
                <YAxis type="category" dataKey="ruta" width={150} tick={TICK_XXS} axisLine={false} tickLine={false} />
                <Tooltip contentStyle={TOOLTIP_STYLE} />
                <Bar dataKey="count" name="Manifiestos" fill={GOLD} radius={[0, 6, 6, 0]} />
              </BarChart>
            </ResponsiveContainer>
          )}
        </ChartCard>

      </div>

      {/* Row 5 */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">

        <ChartCard title="Por agencia">
          {loading ? <ChartSkeleton /> : (
            <ResponsiveContainer width="100%" height={240}>
              <BarChart data={data?.chartAgencias ?? []} margin={{ top: 8, right: 16, bottom: 4 }}>
                <CartesianGrid stroke={GRID} strokeDasharray="3 3" opacity={0.6} vertical={false} />
                <XAxis dataKey="nombre" tick={TICK_SM} axisLine={false} tickLine={false} />
                <YAxis tick={TICK_SM} axisLine={false} tickLine={false} />
                <Tooltip contentStyle={TOOLTIP_STYLE} />
                <Bar dataKey="count" name="Manifiestos" fill={BLUE} radius={[6, 6, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          )}
        </ChartCard>

        <ChartCard title="Estado interno">
          {loading ? <ChartSkeleton /> : (
            <ResponsiveContainer width="100%" height={230}>
              <PieChart margin={{ top: 24, right: 32, bottom: 24, left: 32 }}>
                <Pie data={data?.chartEstadoInterno ?? []} dataKey="value" nameKey="name"
                  cx="50%" cy="50%" innerRadius={58} outerRadius={92}
                  label={({ cx, cy, midAngle, innerRadius, outerRadius, percent }) => {
                    if (percent < 0.05) return null
                    const r = innerRadius + (outerRadius - innerRadius) * 0.5
                    const x = cx + r * Math.cos(-midAngle * Math.PI / 180)
                    const y = cy + r * Math.sin(-midAngle * Math.PI / 180)
                    return (
                      <text x={x} y={y} fill="#fff" textAnchor="middle" dominantBaseline="central"
                        style={{ fontSize: 11, fontWeight: 700, pointerEvents: 'none' }}>
                        {`${(percent * 100).toFixed(0)}%`}
                      </text>
                    )
                  }}
                  labelLine={false} paddingAngle={3}
                >
                  {(data?.chartEstadoInterno ?? []).map((_, i) => <Cell key={i} fill={CHART_COLORS[i % CHART_COLORS.length]} />)}
                </Pie>
                <Tooltip
                  contentStyle={{ ...TOOLTIP_STYLE, borderRadius: 10, padding: '10px 14px', minWidth: 160 }}
                  itemStyle={{ fontSize: 12, color: TICK, fontWeight: 600 }}
                  labelStyle={{ display: 'none' }}
                  formatter={(value, name) => [`${value} ${value === 1 ? 'manifiesto' : 'manifiestos'}`, name]}
                />
              </PieChart>
            </ResponsiveContainer>
          )}
        </ChartCard>

        <ChartCard title="Top conductores">
          {loading ? <ChartSkeleton /> : (
            <ResponsiveContainer width="100%" height={240}>
              <BarChart layout="vertical" data={data?.topConductores ?? []} margin={{ left: 0, right: 16, top: 4, bottom: 4 }}>
                <CartesianGrid stroke={GRID} strokeDasharray="3 3" opacity={0.6} horizontal={false} />
                <XAxis type="number" tick={TICK_SM} axisLine={false} tickLine={false} />
                <YAxis type="category" dataKey="nombre" width={120} tick={TICK_XS} axisLine={false} tickLine={false} />
                <Tooltip contentStyle={TOOLTIP_STYLE} />
                <Bar dataKey="count" name="Manifiestos" fill={GOLD} radius={[0, 6, 6, 0]} />
              </BarChart>
            </ResponsiveContainer>
          )}
        </ChartCard>

      </div>

    </div>
  )
}
