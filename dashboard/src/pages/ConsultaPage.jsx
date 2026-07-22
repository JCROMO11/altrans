import { useState, useEffect, useRef, useCallback } from 'react'
import { Search, ChevronDown, Check, ChevronLeft, ChevronRight, ExternalLink, AlertTriangle, ShieldCheck, Download } from 'lucide-react'
import * as XLSX from 'xlsx'
import { supabase } from '../lib/supabase'
import { useCatalogos } from '../hooks/useCatalogos'
import { useConsulta }  from '../hooks/useConsulta'

// ── Theme ─────────────────────────────────────────────────────────────────────
const BG    = '#FFFFFF'
const BDR   = '#E2E8F0'
const TICK  = '#0F172A'
const BLUE   = '#1E6FBF'
const GOLD   = '#C9A84C'
const MUTED  = '#64748B'
const RED    = '#DC2626'
const GREEN  = '#16A34A'
const AMBAR  = '#F59E0B'
const BTN_GRAD   = 'linear-gradient(135deg, #1E6FBF 0%, #6366F1 100%)'
const BTN_SHADOW = '0 2px 8px 0 rgba(30,111,191,0.22)'

const ESTADO_PAGO_COLOR = { 'PAGADO': GREEN, 'ANULADO': RED, 'PRIORITARIO': GOLD }
const ESTADO_INTERNO_COLOR = {
  'CUMPLIDO': GREEN, 'ANULADO': RED,
  'NOVEDAD PENDIENTE': GOLD, 'PENDIENTE FACTURA ELECTRONICA': GOLD,
}
const estadoPagoColor    = v => ESTADO_PAGO_COLOR[v]    ?? MUTED
const estadoInternoColor = v => ESTADO_INTERNO_COLOR[v] ?? MUTED

const SIDEBAR_COLOR = {
  'CUMPLIDO':                       '#FFFF00',
  'ANULADO':                        '#00FF00',
  'PENDIENTE FACTURA ELECTRONICA':  '#00FFFF',
  'FACTURA RECIBIDA':               '#FF00FF',
  'NOVEDAD PENDIENTE':              '#D5A6BD',
}

const MESES_OPTS = [
  'ENERO','FEBRERO','MARZO','ABRIL','MAYO','JUNIO',
  'JULIO','AGOSTO','SEPTIEMBRE','OCTUBRE','NOVIEMBRE','DICIEMBRE',
]
const AÑOS_OPTS = ['2023','2024','2025','2026','2027','2028','2029','2030',
  '2031','2032','2033','2034','2035','2036','2037','2038','2039','2040',
  '2041','2042','2043','2044','2045']
const ESTADO_INTERNO_OPTS = ['CUMPLIDO','NO SE HA CUMPLIDO','PENDIENTE FACTURA ELECTRONICA',
  'FACTURA RECIBIDA','NOVEDAD PENDIENTE','ANULADO']

function blendOnWhite(hex, alpha) {
  const r = parseInt(hex.slice(1, 3), 16)
  const g = parseInt(hex.slice(3, 5), 16)
  const b = parseInt(hex.slice(5, 7), 16)
  return `rgb(${Math.round(r * alpha + 255 * (1 - alpha))},${Math.round(g * alpha + 255 * (1 - alpha))},${Math.round(b * alpha + 255 * (1 - alpha))})`
}

const fmt     = n  => n == null ? '—' : Number(n).toLocaleString('es-CO', { minimumFractionDigits: 0 })
const fmtDate = d  => {
  if (!d) return '—'
  const [y, m, day] = d.split('-')
  return `${day}/${m}/${y}`
}
const money = n => n == null ? '—' : `$${fmt(n)}`

const inputCls = `w-full rounded-md border px-3 py-2 text-sm focus:outline-none focus:ring-1
  focus:ring-[#1E6FBF] transition-colors bg-transparent text-[#0F172A] placeholder:text-[#64748B]`

// ── Primitives ────────────────────────────────────────────────────────────────
function Field({ label, children }) {
  return (
    <div>
      <label className="block text-[10px] font-bold uppercase tracking-wider mb-1.5"
        style={{ color: MUTED }}>{label}</label>
      {children}
    </div>
  )
}

function FilterSelect({ label, value, onChange, options, placeholder = 'Todos' }) {
  const [open, setOpen] = useState(false)
  return (
    <Field label={label}>
      <div className="relative">
        <button type="button"
          onClick={() => setOpen(v => !v)}
          onBlur={() => setTimeout(() => setOpen(false), 150)}
          className="w-full flex items-center justify-between px-3 py-2 text-sm rounded-md border focus:outline-none focus:ring-1 focus:ring-[#1E6FBF] transition-colors"
          style={{ borderColor: BDR, background: BG, color: value ? TICK : MUTED }}>
          <span className="truncate">{value || placeholder}</span>
          <ChevronDown size={13} style={{
            color: MUTED, flexShrink: 0,
            transform: open ? 'rotate(180deg)' : 'none', transition: 'transform 0.2s',
          }} />
        </button>
        {open && (
          <div className="absolute z-50 w-full mt-1 rounded-xl shadow-xl overflow-hidden max-h-56 overflow-y-auto"
            style={{ background: '#FFFFFF', border: `1px solid ${BDR}` }}>
            <button type="button" onMouseDown={() => { onChange(''); setOpen(false) }}
              className="w-full text-left px-3 py-2 text-sm hover:bg-black/5"
              style={{ color: MUTED }}>{placeholder}</button>
            {options.map(o => (
              <button key={o} type="button" onMouseDown={() => { onChange(o); setOpen(false) }}
                className="w-full text-left px-3 py-2 text-sm hover:bg-black/5 flex items-center justify-between"
                style={{ color: TICK }}>
                <span>{o}</span>
                {value === o && <Check size={11} style={{ color: BLUE, flexShrink: 0 }} />}
              </button>
            ))}
          </div>
        )}
      </div>
    </Field>
  )
}

function FilterAutocomplete({ label, items, labelKey = 'nombre', idKey = 'id', searchKey, value, onChange, placeholder = 'Todos' }) {
  const [query, setQuery] = useState('')
  const [open,  setOpen]  = useState(false)
  const inputRef = useRef(null)
  const sk = searchKey || labelKey

  const selected = items.find(i => i[idKey] === value)
  const display  = selected ? selected[labelKey] : ''
  const filtered = query.length < 1
    ? items.slice(0, 60)
    : items.filter(i => String(i[sk] ?? '').toLowerCase().includes(query.toLowerCase())).slice(0, 60)

  const pick = item => { onChange(item ? item[idKey] : null); setQuery(''); setOpen(false) }

  return (
    <Field label={label}>
      <div className="relative">
        <div className="relative flex items-center">
          <input ref={inputRef} className={inputCls}
            style={{ borderColor: BDR, paddingRight: display ? '2rem' : '0.75rem' }}
            placeholder={display || placeholder}
            value={open ? query : (display || '')}
            onFocus={() => { setQuery(''); setOpen(true) }}
            onChange={e => setQuery(e.target.value)}
            onBlur={() => setTimeout(() => setOpen(false), 150)} />
          {display && !open && (
            <button type="button" onMouseDown={() => pick(null)}
              className="absolute right-2 text-xs" style={{ color: MUTED }}>✕</button>
          )}
        </div>
        {open && (
          <div className="absolute z-50 w-full mt-1 rounded-xl shadow-xl overflow-hidden max-h-56 overflow-y-auto"
            style={{ background: '#FFFFFF', border: `1px solid ${BDR}` }}>
            <button type="button" onMouseDown={() => pick(null)}
              className="w-full text-left px-3 py-2 text-sm hover:bg-black/5"
              style={{ color: MUTED }}>{placeholder}</button>
            {filtered.map(item => (
              <button key={item[idKey]} type="button" onMouseDown={() => pick(item)}
                className="w-full text-left px-3 py-2 text-sm hover:bg-black/5 flex items-center justify-between"
                style={{ color: TICK }}>
                <span>
                  <span>{item[labelKey]}</span>
                  {item.nombre && String(item.nombre) !== String(item[labelKey]) && (
                    <span className="ml-2 text-xs" style={{ color: MUTED }}>{item.nombre}</span>
                  )}
                </span>
                {item[idKey] === value && <Check size={11} style={{ color: BLUE, flexShrink: 0 }} />}
              </button>
            ))}
          </div>
        )}
      </div>
    </Field>
  )
}

function EstadoBadge({ value, colorFn }) {
  if (!value) return <span style={{ color: MUTED }}>—</span>
  const color = colorFn(value)
  return (
    <span className="text-[10px] font-bold px-2 py-0.5 rounded-full whitespace-nowrap"
      style={{ background: color + '22', color, border: `1px solid ${color}55` }}>
      {value}
    </span>
  )
}

function PlazoBadge({ diasCumplidos, fechaPago }) {
  if (!diasCumplidos || fechaPago) return null
  if (diasCumplidos <= 20) return null
  return (
    <span className="animate-pulse" title={`Pago extemporáneo · ${diasCumplidos} días`}>
      <AlertTriangle size={13} style={{ color: RED }} />
    </span>
  )
}

function VencimientoBadge({ fechaEstimada }) {
  if (!fechaEstimada) return <span style={{ color: MUTED }}>—</span>
  const hoy = new Date()
  const f = new Date(fechaEstimada + 'T00:00:00')
  const diff = Math.round((f - hoy) / (1000 * 60 * 60 * 24))
  if (diff <= 0) {
    const d = Math.abs(diff)
    return (
      <span className="text-[10px] font-bold px-2 py-0.5 rounded-full whitespace-nowrap"
        style={{ background: RED + '22', color: RED, border: `1px solid ${RED}55` }}>
        VENCIDO {d}d
      </span>
    )
  }
  if (diff <= 7) {
    return (
      <span className="text-[10px] font-bold px-2 py-0.5 rounded-full whitespace-nowrap"
        style={{ background: AMBAR + '22', color: AMBAR, border: `1px solid ${AMBAR}55` }}>
        {diff}d
      </span>
    )
  }
  return <span style={{ color: TICK }}>{diff}d</span>
}

// ── Celda de tabla ────────────────────────────────────────────────────────────
function Td({ children, right, mono, muted, nowrap = true, highlight, sticky, bg, width, style: extraStyle }) {
  const title = nowrap && typeof children === 'string' ? children : undefined
  return (
    <td
      title={title}
      className={`px-3 py-2 text-xs${nowrap ? ' whitespace-nowrap' : ''}${right ? ' text-right' : ''}${mono ? ' font-mono tabular-nums' : ''}${sticky ? ' sticky left-0 z-10' : ''}`}
      style={{
        color: highlight ?? (muted ? MUTED : TICK),
        borderRight: `1px solid #CBD5E1`,
        ...(width ? { width, minWidth: width, maxWidth: width, overflow: 'hidden', textOverflow: 'ellipsis' } : {}),
        ...(bg ? { background: bg } : {}),
        ...(sticky ? { boxShadow: '2px 0 4px rgba(0,0,0,0.06)' } : {}),
        ...extraStyle,
      }}>
      {children}
    </td>
  )
}

// ── Encabezado de columna ─────────────────────────────────────────────────────
function Th({ children, sticky, width }) {
  return (
    <th className={`px-3 py-2.5 text-[10px] font-bold uppercase tracking-wider whitespace-nowrap text-left${sticky ? ' sticky left-0' : ''}`}
      style={{
        color: MUTED,
        background: '#F1F5F9',
        borderBottom: `1px solid #CBD5E1`,
        borderRight: `1px solid #CBD5E1`,
        zIndex: sticky ? 25 : undefined,
        ...(width ? { width, minWidth: width, maxWidth: width } : {}),
        ...(sticky ? { boxShadow: '2px 0 4px rgba(0,0,0,0.06)' } : {}),
      }}>
      {children}
    </th>
  )
}

// ── Campos auditables ─────────────────────────────────────────────────────────
const CAMPOS_OPTS = [
  'fecha_despacho','origen','destino','cliente','conductor','cedula_conductor',
  'celular','placa','tipo_vehiculo','propietario','agencia_despachadora',
  'nombre_responsable','valor_remesa','flete_conductor','anticipo','remesas',
  'fecha_cumplido','compromiso_pago','novedades','estado_interno',
  'responsable_estado_interno','novedad_conductor','novedad_empresa',
  'ajuste_positivo_flete','ajuste_negativo_flete','consignacion_a_terceros',
  'saldo','fecha_pago','valor_pagado','entidad_financiera',
  'responsable','factura_no','fecha_factura','factura_electronica',
  'mes_facturacion','valor_factura',
]

const AUDIT_PAGE = 50

function AuditoriaPanel() {
  const [filters, setFilters] = useState({
    manifiesto: '', campo: '', usuario: '', fecha_desde: '', fecha_hasta: '',
  })
  const [rows,      setRows]      = useState([])
  const [loading,   setLoading]   = useState(false)
  const [page,      setPage]      = useState(0)
  const [hasMore,   setHasMore]   = useState(false)
  const [error,     setError]     = useState(null)
  const [usuarios,  setUsuarios]  = useState([])

  // Carga todos los usuarios registrados con su rol
  useEffect(() => {
    supabase.rpc('get_usuarios').then(({ data }) => {
      if (!data) return
      setUsuarios(data) // [{ email, rol }]
    })
  }, [])

  const set = (k, v) => setFilters(f => ({ ...f, [k]: v }))

  const buscar = useCallback(async (f, pg) => {
    setLoading(true)
    setError(null)
    const from = pg * AUDIT_PAGE
    const to   = from + AUDIT_PAGE

    let q = supabase
      .from('audit_log')
      .select('id, manifiesto, campo, valor_anterior, valor_nuevo, usuario, ejecutado_en')
      .order('ejecutado_en', { ascending: false })
      .range(from, to)

    if (f.manifiesto) q = q.eq('manifiesto', Number(f.manifiesto))
    if (f.campo)      q = q.eq('campo', f.campo)
    if (f.usuario)    q = q.eq('usuario', f.usuario)
    if (f.fecha_desde) q = q.gte('ejecutado_en', f.fecha_desde)
    if (f.fecha_hasta) q = q.lte('ejecutado_en', f.fecha_hasta + 'T23:59:59')

    const { data, error: err } = await q
    if (err) { setError(err.message); setLoading(false); return }
    const fetched = data ?? []
    setHasMore(fetched.length > AUDIT_PAGE)
    setRows(fetched.slice(0, AUDIT_PAGE))
    setPage(pg)
    setLoading(false)
  }, [])

  const handleSearch = e => { e?.preventDefault(); buscar(filters, 0) }
  const clearAll = () => {
    const empty = { manifiesto: '', campo: '', usuario: '', fecha_desde: '', fecha_hasta: '' }
    setFilters(empty)
    buscar(empty, 0)
  }

  const fmtTs = ts => {
    if (!ts) return '—'
    const d = new Date(ts)
    return d.toLocaleString('es-CO', { dateStyle: 'short', timeStyle: 'short' })
  }

  const ValorCell = ({ v }) => {
    if (v == null) return <span style={{ color: MUTED }}>null</span>
    if (v === '')  return <span style={{ color: MUTED }}>vacío</span>
    return <span>{v}</span>
  }

  return (
    <div className="flex flex-col gap-6 pb-8">

      {/* Filtros */}
      <form onSubmit={handleSearch}
        className="rounded-2xl p-5" style={{ background: BG, border: `1px solid ${BDR}` }}>
        <div className="flex items-center justify-between mb-4">
          <span className="text-sm font-semibold" style={{ color: TICK }}>Filtros de auditoría</span>
          <button type="button" onClick={clearAll}
            className="text-xs hover:opacity-80" style={{ color: MUTED }}>Limpiar todo</button>
        </div>
        <div className="grid grid-cols-2 md:grid-cols-5 gap-4 mb-5">
          <Field label="Manifiesto">
            <input className={inputCls} style={{ borderColor: BDR }}
              type="number" placeholder="Número..."
              value={filters.manifiesto}
              onChange={e => set('manifiesto', e.target.value)} />
          </Field>
          <Field label="Campo modificado">
            <div className="relative">
              <select
                className={inputCls}
                style={{ borderColor: BDR, appearance: 'none', paddingRight: '2rem' }}
                value={filters.campo}
                onChange={e => set('campo', e.target.value)}>
                <option value="">Todos</option>
                {CAMPOS_OPTS.map(c => <option key={c} value={c}>{c}</option>)}
              </select>
              <ChevronDown size={13} style={{ color: MUTED, position: 'absolute', right: '0.75rem', top: '50%', transform: 'translateY(-50%)', pointerEvents: 'none' }} />
            </div>
          </Field>
          <Field label="Usuario">
            <div className="relative">
              <select
                className={inputCls}
                style={{ borderColor: BDR, appearance: 'none', paddingRight: '2rem' }}
                value={filters.usuario}
                onChange={e => set('usuario', e.target.value)}>
                <option value="">Todos</option>
                {usuarios.map(u => (
                  <option key={u.email} value={u.email}>{u.email} ({u.rol})</option>
                ))}
              </select>
              <ChevronDown size={13} style={{ color: MUTED, position: 'absolute', right: '0.75rem', top: '50%', transform: 'translateY(-50%)', pointerEvents: 'none' }} />
            </div>
          </Field>
          <Field label="Fecha desde">
            <input className={inputCls} style={{ borderColor: BDR }}
              type="date" value={filters.fecha_desde}
              onChange={e => set('fecha_desde', e.target.value)} />
          </Field>
          <Field label="Fecha hasta">
            <input className={inputCls} style={{ borderColor: BDR }}
              type="date" value={filters.fecha_hasta}
              onChange={e => set('fecha_hasta', e.target.value)} />
          </Field>
        </div>
        <button type="submit"
          className="flex items-center gap-2 px-5 py-2 rounded-xl text-sm font-semibold transition-opacity hover:opacity-90"
          style={{ background: BTN_GRAD, color: '#fff', boxShadow: BTN_SHADOW }}>
          <Search size={14} /> Consultar
        </button>
      </form>

      {/* Tabla */}
      <div className="rounded-2xl overflow-hidden" style={{ border: `1px solid ${BDR}` }}>
        {error ? (
          <div className="flex items-center justify-center h-32">
            <span className="text-sm" style={{ color: RED }}>{error}</span>
          </div>
        ) : loading ? (
          <div className="flex items-center justify-center h-32">
            <span className="text-sm animate-pulse" style={{ color: MUTED }}>Cargando...</span>
          </div>
        ) : rows.length === 0 ? (
          <div className="flex items-center justify-center h-32">
            <span className="text-sm" style={{ color: MUTED }}>
              Aplica filtros y presiona Consultar para ver el historial.
            </span>
          </div>
        ) : (
          <div style={{ maxHeight: '68vh', overflow: 'auto' }}>
            <table className="text-sm border-collapse w-full">
              <thead style={{ position: 'sticky', top: 0, zIndex: 15 }}>
                <tr style={{ background: '#F1F5F9' }}>
                  <Th width="90px">Manifiesto</Th>
                  <Th width="200px">Campo</Th>
                  <Th width="220px">Valor anterior</Th>
                  <Th width="220px">Valor nuevo</Th>
                  <Th width="200px">Usuario</Th>
                  <Th width="155px">Fecha / Hora</Th>
                </tr>
              </thead>
              <tbody>
                {rows.map((r, i) => (
                  <tr key={r.id} style={{ background: i % 2 === 0 ? BG : '#F8FAFC' }}>
                    <Td mono highlight={GOLD} width="90px">{r.manifiesto}</Td>
                    <Td mono width="200px">{r.campo}</Td>
                    <td className="px-3 py-2 text-xs" style={{ width: '220px', minWidth: '220px', maxWidth: '220px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', borderRight: '1px solid #CBD5E1', color: RED + 'CC' }}>
                      <ValorCell v={r.valor_anterior} />
                    </td>
                    <td className="px-3 py-2 text-xs" style={{ width: '220px', minWidth: '220px', maxWidth: '220px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', borderRight: '1px solid #CBD5E1', color: GREEN }}>
                      <ValorCell v={r.valor_nuevo} />
                    </td>
                    <Td muted width="200px">{r.usuario ?? '—'}</Td>
                    <Td muted width="155px">{fmtTs(r.ejecutado_en)}</Td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {!loading && rows.length > 0 && (
          <div className="flex items-center justify-between px-4 py-3"
            style={{ borderTop: `1px solid ${BDR}`, background: '#F1F5F9' }}>
            <span className="text-xs" style={{ color: MUTED }}>
              Página {page + 1}{hasMore ? '+' : ''} · {rows.length} registros
            </span>
            <div className="flex gap-2">
              <button type="button" onClick={() => buscar(filters, page - 1)} disabled={page === 0}
                className="flex items-center gap-1 px-3 py-1.5 text-xs rounded-lg transition-opacity disabled:opacity-30"
                style={{ background: BG, border: `1px solid ${BDR}`, color: TICK }}>
                <ChevronLeft size={12} /> Anterior
              </button>
              <button type="button" onClick={() => buscar(filters, page + 1)} disabled={!hasMore}
                className="flex items-center gap-1 px-3 py-1.5 text-xs rounded-lg transition-opacity disabled:opacity-30"
                style={{ background: BG, border: `1px solid ${BDR}`, color: TICK }}>
                Siguiente <ChevronRight size={12} />
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}


// ── Main Component ────────────────────────────────────────────────────────────
export default function ConsultaPage({ openEnCarga, user }) {
  const rol = user?.app_metadata?.role || ''
  const canVerValorFactura = ['financiero', 'contadora', 'administrativo', 'gerencia'].includes(rol)
  const isGerencia = rol === 'gerencia'

  const { catalogos } = useCatalogos()
  const { rows, loading, page, hasMore, buscar } = useConsulta()

  const FILTERS_INIT = {
    manifiesto: '', fecha_desde: '', fecha_hasta: '',
    conductor: null, cliente: null, origen: null, destino: null,
    compromiso_pago: '', estado_interno: '', placa: '', mes: '', año: '',
    cedula_conductor: '', tiene_fe: '', nombre_responsable: '',
    estado_vencimiento: '',
  }

  const [filters,   setFilters]   = useState(FILTERS_INIT)
  const [activeTab, setActiveTab] = useState('manifiestos')

  const set = (k, v) => setFilters(f => ({ ...f, [k]: v }))

  const [alertas, setAlertas] = useState(null)
  useEffect(() => {
    supabase.rpc('consulta_alertas_vencimiento').then(({ data }) => {
      if (data) setAlertas(data)
    })
  }, [])

  // Carga automática al abrir
  useEffect(() => { buscar(FILTERS_INIT, 0) }, []) // eslint-disable-line react-hooks/exhaustive-deps

  const handleSearch = e => {
    e?.preventDefault()
    buscar(filters, 0)
  }

  const handleNext = () => { buscar(filters, page + 1) }
  const handlePrev = () => { buscar(filters, page - 1) }

  const clearAll = () => {
    setFilters(FILTERS_INIT)
    buscar(FILTERS_INIT, 0)
  }

  const filtrarPorVencimiento = tipo => {
    if (filters.estado_vencimiento === tipo) {
      const reset = { ...FILTERS_INIT }
      setFilters(reset)
      buscar(reset, 0)
    } else {
      const reset = { ...FILTERS_INIT, estado_vencimiento: tipo }
      setFilters(reset)
      buscar(reset, 0)
    }
  }

  // ── Export helpers ─────────────────────────────────────────────────────────
  const EXPORT_COLS = [
    ['Manifiesto',              'manifiesto',             'raw'],
    ['Remesas',                 'remesas',                'raw'],
    ['Fecha Despacho',          'fecha_despacho',         'date'],
    ['Origen',                  'origen',                 'raw'],
    ['Dpto. Origen',            'departamento_origen',    'raw'],
    ['Destino',                 'destino',                'raw'],
    ['Dpto. Destino',           'departamento_destino',   'raw'],
    ['Cliente',                 'cliente',                 'raw'],
    ['Valor Remesa',            'valor_remesa',           'money'],
    ['Flete Neto',              'flete_conductor',        'money'],
    ['Anticipo',                'anticipo',               'money'],
    ['Placa',                   'placa',                  'raw'],
    ['Remolque',                'tipo_vehiculo',          'raw'],
    ['Conductor',               'conductor',              'raw'],
    ['Celular',                 'celular',                'raw'],
    ['Cédula',                  'cedula_conductor',       'raw'],
    ['Propietario',             'propietario',            'raw'],
    ['Agencia Despachadora',     'agencia_despachadora',   'raw'],
    ['Responsable',             'nombre_responsable',     'raw'],
    ['Fecha Cumplido',          'fecha_cumplido',         'date'],
    ['Días Cumplido',           'dias_cumplido',          'raw'],
    ['Compromiso Pago',         'compromiso_pago',        'raw'],
    ['Días x Vencer',           'dias_para_facturar',     'raw'],
    ['Novedades',               'novedades',              'raw'],
    ['Novedad Conductor',       'novedad_conductor',      'raw'],
    ['Novedad Empresa',         'novedad_empresa',        'raw'],
    ['Reajuste',                'ajuste_positivo_flete',  'money'],
    ['Descuento',               'ajuste_negativo_flete',  'money'],
    ['Consignación Terceros',   'consignacion_a_terceros','money'],
    ['Saldo',                   'saldo',                  'money'],
    ['Estado Interno',          'estado_interno',         'raw'],
    ['Resp. Estado Interno',    'responsable_estado_interno','raw'],
    ['Fecha Pago',              'fecha_pago',             'date'],
    ['Valor Pagado',            'valor_pagado',           'money'],
    ['Entidad Financiera',      'entidad_financiera',     'raw'],
    ['Responsable Pago',        'responsable',            'raw'],
    ['Factura No',              'factura_no',             'raw'],
    ['Fecha Emisión',           'fecha_factura',          'date'],
    ['Factura Electrónica',     'factura_electronica',    'raw'],
    ['Valor Factura',           'valor_factura',          'money'],
    ['Días Fact.',              'dias_para_facturar',     'raw'],
  ]

  const fmtExport = (val, typ) => {
    if (val == null || val === '') return ''
    if (typ === 'money') return Number(val)
    if (typ === 'date') return val
    return String(val)
  }

  const buildExportData = () => rows.map(r => {
    const obj = {}
    EXPORT_COLS.forEach(([label, key, typ]) => { obj[label] = fmtExport(r[key], typ) })
    return obj
  })

  const downloadBlob = (content, filename, mimeType) => {
    const blob = new Blob(['\ufeff' + content], { type: mimeType + ';charset=utf-8;' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url; a.download = filename; a.click()
    URL.revokeObjectURL(url)
  }

  const exportCSV = () => {
    if (!rows.length) return
    const data = buildExportData()
    const headers = EXPORT_COLS.map(([label]) => label)
    const csvRows = data.map(obj =>
      headers.map(h => {
        const v = obj[h]
        if (v == null || v === '') return ''
        const s = String(v)
        return '"' + s.replace(/"/g, '""') + '"'
      }).join(',')
    )
    downloadBlob([headers.join(','), ...csvRows].join('\n'), 'consulta_manifiestos.csv', 'text/csv')
  }

  const exportExcel = () => {
    if (!rows.length) return
    const ws = XLSX.utils.json_to_sheet(buildExportData())
    const wb = XLSX.utils.book_new()
    XLSX.utils.book_append_sheet(wb, ws, 'Manifiestos')
    XLSX.writeFile(wb, 'consulta_manifiestos.xlsx')
  }

  return (
    <div className="flex flex-col gap-6 pb-8">

      {/* Tab bar — Auditoría solo visible para admin */}
      {isGerencia && (
        <div className="flex gap-1 p-1 rounded-xl self-start" style={{ background: '#F1F5F9', border: `1px solid ${BDR}` }}>
          {[
            { id: 'manifiestos', label: 'Manifiestos' },
            { id: 'auditoria',   label: 'Auditoría', icon: <ShieldCheck size={13} /> },
          ].map(tab => (
            <button key={tab.id} type="button"
              onClick={() => setActiveTab(tab.id)}
              className="flex items-center gap-1.5 px-4 py-1.5 text-sm font-medium rounded-lg transition-all"
              style={{
                background: activeTab === tab.id ? BTN_GRAD : 'transparent',
                color:      activeTab === tab.id ? '#FFFFFF' : MUTED,
                boxShadow:  activeTab === tab.id ? BTN_SHADOW : 'none',
              }}>
              {tab.icon}{tab.label}
            </button>
          ))}
        </div>
      )}

      {activeTab === 'auditoria' && isGerencia
        ? <AuditoriaPanel />
        : <>

      {/* Alertas de vencimiento */}
      {alertas && (
        <div className="flex gap-3 items-center rounded-2xl px-5 py-3 mb-2"
          style={{ background: BG, border: `1px solid ${BDR}` }}>
          <span className="text-[10px] font-bold uppercase tracking-wider" style={{ color: MUTED }}>Vencimientos</span>
          <button type="button" onClick={() => filtrarPorVencimiento('vencidos')}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-semibold transition-all"
            style={filters.estado_vencimiento === 'vencidos'
              ? { background: 'linear-gradient(135deg, #E05252 0%, #EF4444 100%)', color: '#fff', boxShadow: '0 2px 8px 0 rgba(224,82,82,0.25)' }
              : alertas.vencidos > 0
                ? { background: RED + '18', color: RED, border: `1px solid ${RED}44` }
                : { background: '#F1F5F9', color: MUTED } }
            disabled={!alertas.vencidos > 0}>
            <AlertTriangle size={12} /> Vencidos: {alertas.vencidos ?? '—'}
            {alertas.saldoVencido > 0 && (
              <span className="ml-1 text-[10px] opacity-75">· {money(alertas.saldoVencido)}</span>
            )}
          </button>
          <button type="button" onClick={() => filtrarPorVencimiento('por_vencer')}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-semibold transition-all"
            style={filters.estado_vencimiento === 'por_vencer'
              ? { background: 'linear-gradient(135deg, #F59E0B 0%, #F97316 100%)', color: '#fff', boxShadow: '0 2px 8px 0 rgba(245,158,11,0.25)' }
              : alertas.porVencer > 0
                ? { background: AMBAR + '18', color: AMBAR, border: `1px solid ${AMBAR}44` }
                : { background: '#F1F5F9', color: MUTED } }
            disabled={!alertas.porVencer > 0}>
            Por vencer (&lt;7d): {alertas.porVencer ?? '—'}
          </button>
        </div>
      )}

      {/* Filter panel */}
      <form onSubmit={handleSearch}
        className="rounded-2xl p-5" style={{ background: BG, border: `1px solid ${BDR}` }}>
        <div className="flex items-center justify-between mb-4">
          <span className="text-sm font-semibold" style={{ color: TICK }}>Filtros de consulta</span>
          <button type="button" onClick={clearAll}
            className="text-xs hover:opacity-80" style={{ color: MUTED }}>Limpiar todo</button>
        </div>

        {/* Fila 1 */}
        <div className="grid grid-cols-2 md:grid-cols-5 gap-4 mb-4">
          <Field label="Manifiesto">
            <input className={inputCls} style={{ borderColor: BDR }}
              type="number" placeholder="Número..."
              value={filters.manifiesto}
              onChange={e => set('manifiesto', e.target.value)} />
          </Field>
          <FilterAutocomplete label="Placa" items={catalogos.vehiculos} labelKey="nombre" idKey="id"
            value={filters.placa || null} onChange={v => set('placa', v ?? '')} />
          <Field label="Fecha desde">
            <input className={inputCls} style={{ borderColor: BDR }}
              type="date" value={filters.fecha_desde}
              onChange={e => set('fecha_desde', e.target.value)} />
          </Field>
          <Field label="Fecha hasta">
            <input className={inputCls} style={{ borderColor: BDR }}
              type="date" value={filters.fecha_hasta}
              onChange={e => set('fecha_hasta', e.target.value)} />
          </Field>
          <Field label="Factura Electrónica">
            <div className="flex gap-1">
              {['', 'true', 'false'].map(v => (
                <button key={v} type="button"
                  onClick={() => set('tiene_fe', v)}
                  className="flex-1 text-xs font-semibold px-2 py-1.5 rounded-lg transition-all"
                  style={{
                    background: filters.tiene_fe === v ? BTN_GRAD : '#F1F5F9',
                    color:      filters.tiene_fe === v ? '#fff' : MUTED,
                    boxShadow:  filters.tiene_fe === v ? BTN_SHADOW : 'none',
                  }}>
                  {v === '' ? 'Todas' : v === 'true' ? 'Con FE' : 'Sin FE'}
                </button>
              ))}
            </div>
          </Field>
        </div>

        {/* Fila 2 */}
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4 mb-4">
          <FilterSelect label="Mes" value={filters.mes} onChange={v => set('mes', v)} options={MESES_OPTS} />
          <FilterSelect label="Año" value={filters.año} onChange={v => set('año', v)} options={AÑOS_OPTS} />
          <FilterSelect label="Compromiso de pago" value={filters.compromiso_pago} onChange={v => set('compromiso_pago', v)} options={catalogos.compromisos_pago} />
          <FilterSelect label="Estado interno" value={filters.estado_interno} onChange={v => set('estado_interno', v)} options={ESTADO_INTERNO_OPTS} />
          <FilterAutocomplete label="Conductor" items={catalogos.conductores}
            value={filters.conductor} onChange={v => set('conductor', v)} />
          <FilterAutocomplete label="Cédula" items={catalogos.conductores} labelKey="cedula" idKey="cedula" searchKey="cedula"
            value={filters.cedula_conductor || null} onChange={v => set('cedula_conductor', v ?? '')} />
        </div>

        {/* Fila 3 */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-5">
          <FilterAutocomplete label="Cliente" items={catalogos.clientes}
            value={filters.cliente} onChange={v => set('cliente', v)} />
          <FilterAutocomplete label="Origen" items={catalogos.lugares}
            value={filters.origen} onChange={v => set('origen', v)} />
          <FilterAutocomplete label="Destino" items={catalogos.lugares}
            value={filters.destino} onChange={v => set('destino', v)} />
          <FilterAutocomplete label="Responsable" items={catalogos.responsables}
            value={filters.nombre_responsable || null} onChange={v => set('nombre_responsable', v ?? '')} />
        </div>

        <button type="submit"
          className="flex items-center gap-2 px-5 py-2 rounded-xl text-sm font-semibold transition-opacity hover:opacity-90"
          style={{ background: BTN_GRAD, color: '#fff', boxShadow: BTN_SHADOW }}>
          <Search size={14} /> Consultar
        </button>
      </form>

      {/* Export bar — solo gerencia por ahora */}
      {isGerencia && rows.length > 0 && (
        <div className="flex justify-end gap-2">
          <button type="button" onClick={exportCSV}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-semibold transition-opacity hover:opacity-80"
            style={{ background: '#F1F5F9', color: MUTED, border: `1px solid ${BDR}` }}>
            <Download size={12} /> CSV
          </button>
          <button type="button" onClick={exportExcel}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-semibold transition-opacity hover:opacity-80"
            style={{ background: '#F1F5F9', color: MUTED, border: `1px solid ${BDR}` }}>
            <Download size={12} /> Excel
          </button>
        </div>
      )}

      {/* Results table */}
      <div className="rounded-2xl overflow-hidden" style={{ border: `1px solid ${BDR}` }}>
        {loading ? (
          <div className="flex items-center justify-center h-32">
            <span className="text-sm animate-pulse" style={{ color: MUTED }}>Cargando...</span>
          </div>
        ) : rows.length === 0 ? (
          <div className="flex items-center justify-center h-32">
            <span className="text-sm" style={{ color: MUTED }}>Sin resultados para los filtros seleccionados.</span>
          </div>
        ) : (
          <div style={{ maxHeight: '68vh', overflow: 'auto' }}>
            <table className="text-sm border-collapse" style={{ minWidth: '3620px' }}>
              <thead style={{ position: 'sticky', top: 0, zIndex: 15 }}>
                <tr style={{ background: '#F1F5F9' }}>
                  <Th sticky width="105px">Manifiesto</Th>
                  <Th width="90px">Remesas</Th>
                  <Th width="125px">Fecha Despacho</Th>
                  <Th width="160px">Origen</Th>
                  <Th width="140px">Dpto. Origen</Th>
                  <Th width="160px">Destino</Th>
                  <Th width="140px">Dpto. Destino</Th>
                  <Th width="140px">Cliente</Th>
                  <Th right width="110px">Valor Remesa</Th>
                  <Th right width="110px">Flete Neto</Th>
                  <Th right width="95px">Anticipo</Th>
                  <Th width="80px">Placa</Th>
                  <Th width="105px">Remolque</Th>
                  <Th width="240px">Conductor</Th>
                  <Th width="100px">Celular</Th>
                  <Th width="95px">Cédula</Th>
                  <Th width="210px">Propietario</Th>
                  <Th width="160px">Agencia Despachadora</Th>
                  <Th width="140px">Responsable</Th>
                  <Th width="125px">Fecha Cumplido</Th>
                  <Th width="115px">Días Cumplido</Th>
                  <Th width="160px">Compromiso Pago</Th>
                  <Th width="120px">Días x Vencer</Th>
                  <Th width="280px">Novedades</Th>
                  <Th width="240px">Novedad Conductor</Th>
                  <Th width="240px">Novedad Empresa</Th>
                  <Th width="100px">Reajuste</Th>
                  <Th width="105px">Descuento</Th>
                  <Th width="170px">Consignación Terceros</Th>
                  <Th width="160px">Estado Interno</Th>
                  <Th width="200px">Responsable Estado Interno</Th>
                  <Th width="100px">Fecha Pago</Th>
                  <Th right width="110px">Valor Pagado</Th>
                  <Th width="180px">Entidad Financiera</Th>
                  <Th width="180px">Responsable Pago</Th>
                  <Th width="90px">Factura No</Th>
                  <Th width="110px">Fecha Emisión</Th>
                  <Th width="260px">Legalización FE / DS</Th>
                  {canVerValorFactura && <Th right width="110px">Valor Factura</Th>}
                  <Th width="85px">Días Fact.</Th>
                  <Th width="50px"></Th>
                </tr>
              </thead>
              <tbody>
                {rows.map((r, i) => {
                  const plazoVencido = r.dias_cumplido > 20 && !r.fecha_pago
                  const sidebarColor = SIDEBAR_COLOR[r.estado_interno]
                  const rowBg = sidebarColor
                    ? sidebarColor + '66'
                    : i % 2 === 0 ? BG : '#F8FAFC'
                  const stickyBg = sidebarColor ? blendOnWhite(sidebarColor, 0.4) : rowBg
                  return (
                    <tr key={r.manifiesto} style={{ background: rowBg }}>
                      <Td mono highlight={GOLD} sticky bg={stickyBg} width="105px">{r.manifiesto}</Td>
                      <Td muted width="90px">{r.remesas || '—'}</Td>
                      <Td width="125px">{fmtDate(r.fecha_despacho)}</Td>
                      <Td width="160px">{r.origen ?? '—'}</Td>
                      <Td muted width="140px">{r.departamento_origen ?? '—'}</Td>
                      <Td width="160px">{r.destino ?? '—'}</Td>
                      <Td muted width="140px">{r.departamento_destino ?? '—'}</Td>
                      <Td width="140px">{r.cliente ?? '—'}</Td>
                      <Td mono width="110px">{money(r.valor_remesa)}</Td>
                      <Td mono width="110px">{money(r.saldo ?? r.flete_conductor)}</Td>
                      <Td mono muted width="95px">{money(r.anticipo)}</Td>
                      <Td mono muted width="80px">{r.placa ?? '—'}</Td>
                      <Td muted width="105px">{r.tipo_vehiculo ?? '—'}</Td>
                      <Td width="240px">{r.conductor ?? '—'}</Td>
                      <Td muted width="100px">{r.celular ?? '—'}</Td>
                      <Td muted width="95px">{r.cedula_conductor ?? '—'}</Td>
                      <Td muted width="210px">{r.propietario ?? '—'}</Td>
                      <Td muted width="160px">{r.agencia_despachadora ?? '—'}</Td>
                      <Td muted width="140px">{r.nombre_responsable ?? '—'}</Td>
                      <Td width="125px">{fmtDate(r.fecha_cumplido)}</Td>
                      <td className="px-3 py-2 text-xs whitespace-nowrap" style={{ width: '115px', minWidth: '115px', borderRight: '1px solid #CBD5E1' }}>
                        {r.dias_cumplido != null ? (
                          <div className="flex items-center justify-start gap-1.5">
                            <span style={{ color: plazoVencido ? RED : TICK }}>{r.dias_cumplido}</span>
                            <PlazoBadge diasCumplidos={r.dias_cumplido} fechaPago={r.fecha_pago} />
                          </div>
                        ) : <span style={{ color: MUTED }}>—</span>}
                      </td>
                      <td className="px-3 py-2 whitespace-nowrap" style={{ width: '160px', minWidth: '160px', borderRight: '1px solid #CBD5E1' }}>
                        <EstadoBadge value={r.compromiso_pago} colorFn={estadoPagoColor} />
                      </td>
                      <td className="px-3 py-2 whitespace-nowrap" style={{ width: '120px', minWidth: '120px', borderRight: '1px solid #CBD5E1' }}>
                        <VencimientoBadge fechaEstimada={r.fecha_estimada_pago} />
                      </td>
                      <td className="px-3 py-2 text-xs" style={{ width: '280px', minWidth: '280px', maxWidth: '280px', borderRight: '1px solid #CBD5E1' }}>
                        <div title={r.novedades || undefined} style={{ color: MUTED, display: '-webkit-box', WebkitLineClamp: 2, WebkitBoxOrient: 'vertical', overflow: 'hidden' }}>
                          {r.novedades || '—'}
                        </div>
                      </td>
                      <td className="px-3 py-2 text-xs" style={{ width: '240px', minWidth: '240px', maxWidth: '240px', borderRight: '1px solid #CBD5E1' }}>
                        <div title={r.novedad_conductor || undefined} style={{ color: MUTED, display: '-webkit-box', WebkitLineClamp: 2, WebkitBoxOrient: 'vertical', overflow: 'hidden' }}>
                          {r.novedad_conductor || '—'}
                        </div>
                      </td>
                      <td className="px-3 py-2 text-xs" style={{ width: '240px', minWidth: '240px', maxWidth: '240px', borderRight: '1px solid #CBD5E1' }}>
                        <div title={r.novedad_empresa || undefined} style={{ color: MUTED, display: '-webkit-box', WebkitLineClamp: 2, WebkitBoxOrient: 'vertical', overflow: 'hidden' }}>
                          {r.novedad_empresa || '—'}
                        </div>
                      </td>
                      <Td mono width="100px">{r.ajuste_positivo_flete != null ? money(r.ajuste_positivo_flete) : '—'}</Td>
                      <Td mono width="105px">{r.ajuste_negativo_flete != null ? money(r.ajuste_negativo_flete) : '—'}</Td>
                      <Td mono width="170px">{r.consignacion_a_terceros != null ? money(r.consignacion_a_terceros) : '—'}</Td>
                      <td className="px-3 py-2 whitespace-nowrap" style={{ width: '160px', minWidth: '160px', borderRight: '1px solid #CBD5E1' }}>
                        <EstadoBadge value={r.estado_interno} colorFn={estadoInternoColor} />
                      </td>
                      <Td muted width="200px">{r.responsable_estado_interno ?? '—'}</Td>
                      <Td width="100px">{fmtDate(r.fecha_pago)}</Td>
                      <Td mono highlight={GREEN} width="110px">{money(r.valor_pagado)}</Td>
                      <Td muted width="180px">{r.entidad_financiera ?? '—'}</Td>
                      <Td muted width="180px">{r.responsable ?? '—'}</Td>
                      <Td mono muted width="90px">{r.factura_no ?? (
                        <span className="text-[10px] px-1.5 py-0.5 rounded font-semibold"
                          style={{ background: GOLD + '22', color: GOLD, border: `1px solid ${GOLD}44` }}>
                          Sin fact.
                        </span>
                      )}</Td>
                      <Td muted width="110px">{fmtDate(r.fecha_factura)}</Td>
                      <Td muted width="260px">{r.factura_electronica ?? '—'}</Td>
                      {canVerValorFactura && (
                        <Td mono width="110px">{r.valor_factura != null ? money(r.valor_factura) : '—'}</Td>
                      )}
                      <Td mono muted width="85px">{r.dias_para_facturar ?? '—'}</Td>
                      <td className="px-3 py-2 sticky right-0" style={{ width: '50px', background: stickyBg, boxShadow: '-2px 0 4px rgba(0,0,0,0.06)' }}>
                        {openEnCarga && (
                          <button type="button"
                            onClick={() => openEnCarga(r.manifiesto)}
                            className="flex items-center gap-1 text-xs px-2 py-1 rounded-lg transition-opacity hover:opacity-80"
                            style={{ background: BLUE + '22', color: BLUE, border: `1px solid ${BLUE}44` }}>
                            <ExternalLink size={10} /> Ver
                          </button>
                        )}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        )}

        {/* Pagination */}
        {!loading && rows.length > 0 && (
          <div className="flex items-center justify-between px-4 py-3"
            style={{ borderTop: `1px solid ${BDR}`, background: '#F1F5F9' }}>
            <span className="text-xs" style={{ color: MUTED }}>
              Página {page + 1}{hasMore ? '+' : ''} · {rows.length} resultados
            </span>
            <div className="flex gap-2">
              <button type="button" onClick={handlePrev} disabled={page === 0}
                className="flex items-center gap-1 px-3 py-1.5 text-xs rounded-lg transition-opacity disabled:opacity-30"
                style={{ background: BG, border: `1px solid ${BDR}`, color: TICK }}>
                <ChevronLeft size={12} /> Anterior
              </button>
              <button type="button" onClick={handleNext} disabled={!hasMore}
                className="flex items-center gap-1 px-3 py-1.5 text-xs rounded-lg transition-opacity disabled:opacity-30"
                style={{ background: BG, border: `1px solid ${BDR}`, color: TICK }}>
                Siguiente <ChevronRight size={12} />
              </button>
            </div>
          </div>
        )}
      </div>
      </>}
    </div>
  )
}
