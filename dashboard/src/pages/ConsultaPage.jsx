import { useState, useRef, useCallback } from 'react'
import { Search, ChevronDown, Check, ChevronLeft, ChevronRight, ExternalLink } from 'lucide-react'
import { useCatalogos } from '../hooks/useCatalogos'
import { useConsulta }  from '../hooks/useConsulta'

// ── Theme ─────────────────────────────────────────────────────────────────────
const BG    = '#1B2B3B'
const BDR   = '#2A3F52'
const TICK  = '#F0F4F8'
const BLUE  = '#1E6FBF'
const GOLD  = '#C9A84C'
const MUTED = '#8FA3B1'
const RED   = '#E05252'
const GREEN = '#4CAF7D'

// ── Estado colours ────────────────────────────────────────────────────────────
const ESTADO_PAGO_COLOR = {
  'PAGADO': GREEN, 'ANULADO': RED, 'PRIORITARIO': GOLD,
}
const ESTADO_INTERNO_COLOR = {
  'CUMPLIDO': GREEN, 'ANULADO': RED,
  'NOVEDAD PENDIENTE': GOLD, 'PENDIENTE FACTURA ELECTRONICA': GOLD,
}
function estadoPagoColor(v)    { return ESTADO_PAGO_COLOR[v]    ?? MUTED }
function estadoInternoColor(v) { return ESTADO_INTERNO_COLOR[v] ?? MUTED }

// ── ENUMs ─────────────────────────────────────────────────────────────────────
const ESTADO_PAGO_OPTS    = ['PAGO A 15 DIAS','PAGO A 20 DIAS','PAGO A 30 DIAS','PAGO A 5-8 DIAS',
  'CONTRAENTREGA','PRONTO PAGO','PAGO NORMAL','PAGO INMEDIATO','URBANO','PAGADO','ANULADO','PRIORITARIO','RNDC','OTROS']
const ESTADO_INTERNO_OPTS = ['CUMPLIDO','NO SE HA CUMPLIDO','PENDIENTE FACTURA ELECTRONICA',
  'FACTURA RECIBIDA','NOVEDAD PENDIENTE','ANULADO']

// ── Helpers ───────────────────────────────────────────────────────────────────
const fmt = (n) => n == null ? '—' : Number(n).toLocaleString('es-CO', { minimumFractionDigits: 0 })
const fmtDate = (d) => {
  if (!d) return '—'
  const [y, m, day] = d.split('-')
  return `${day}/${m}/${y}`
}

// ── Primitives ────────────────────────────────────────────────────────────────
const inputCls = `w-full rounded-md border px-3 py-2 text-sm focus:outline-none focus:ring-1
  focus:ring-[#1E6FBF] transition-colors bg-transparent text-[#F0F4F8] placeholder:text-[#8FA3B1]`

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
        <button
          type="button"
          onClick={() => setOpen(v => !v)}
          onBlur={() => setTimeout(() => setOpen(false), 150)}
          className="w-full flex items-center justify-between px-3 py-2 text-sm rounded-md border focus:outline-none focus:ring-1 focus:ring-[#1E6FBF] transition-colors"
          style={{ borderColor: BDR, background: BG, color: value ? TICK : MUTED }}>
          <span className="truncate">{value || placeholder}</span>
          <ChevronDown size={13} style={{
            color: MUTED, flexShrink: 0,
            transform: open ? 'rotate(180deg)' : 'none',
            transition: 'transform 0.2s',
          }} />
        </button>
        {open && (
          <div className="absolute z-50 w-full mt-1 rounded-xl shadow-xl overflow-hidden max-h-56 overflow-y-auto"
            style={{ background: '#0f1e2b', border: `1px solid ${BDR}` }}>
            <button type="button" onMouseDown={() => { onChange(''); setOpen(false) }}
              className="w-full text-left px-3 py-2 text-sm hover:bg-white/5"
              style={{ color: MUTED }}>{placeholder}</button>
            {options.map(o => (
              <button key={o} type="button" onMouseDown={() => { onChange(o); setOpen(false) }}
                className="w-full text-left px-3 py-2 text-sm hover:bg-white/5 flex items-center justify-between"
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

function FilterAutocomplete({ label, items, labelKey = 'nombre', idKey = 'id', value, onChange, placeholder = 'Todos' }) {
  const [query, setQuery] = useState('')
  const [open,  setOpen]  = useState(false)
  const inputRef = useRef(null)

  const selected = items.find(i => i[idKey] === value)
  const display  = selected ? selected[labelKey] : ''

  const filtered = query.length < 1
    ? items.slice(0, 60)
    : items.filter(i => i[labelKey].toLowerCase().includes(query.toLowerCase())).slice(0, 60)

  const pick = (item) => {
    onChange(item ? item[idKey] : null)
    setQuery('')
    setOpen(false)
  }

  return (
    <Field label={label}>
      <div className="relative">
        <div className="relative flex items-center">
          <input
            ref={inputRef}
            className={inputCls}
            style={{ borderColor: BDR, paddingRight: display ? '2rem' : '0.75rem' }}
            placeholder={display || placeholder}
            value={open ? query : (display || '')}
            onFocus={() => { setQuery(''); setOpen(true) }}
            onChange={e => setQuery(e.target.value)}
            onBlur={() => setTimeout(() => setOpen(false), 150)}
          />
          {display && !open && (
            <button type="button" onMouseDown={() => pick(null)}
              className="absolute right-2 text-xs"
              style={{ color: MUTED }}>✕</button>
          )}
        </div>
        {open && (
          <div className="absolute z-50 w-full mt-1 rounded-xl shadow-xl overflow-hidden max-h-56 overflow-y-auto"
            style={{ background: '#0f1e2b', border: `1px solid ${BDR}` }}>
            <button type="button" onMouseDown={() => pick(null)}
              className="w-full text-left px-3 py-2 text-sm hover:bg-white/5"
              style={{ color: MUTED }}>{placeholder}</button>
            {filtered.map(item => (
              <button key={item[idKey]} type="button" onMouseDown={() => pick(item)}
                className="w-full text-left px-3 py-2 text-sm hover:bg-white/5 flex items-center justify-between"
                style={{ color: TICK }}>
                <span>{item[labelKey]}</span>
                {item[idKey] === value && <Check size={11} style={{ color: BLUE, flexShrink: 0 }} />}
              </button>
            ))}
          </div>
        )}
      </div>
    </Field>
  )
}

function KpiBox({ label, value, color }) {
  return (
    <div className="rounded-xl p-4 flex flex-col gap-1" style={{ background: '#0f1e2b', border: `1px solid ${BDR}` }}>
      <span className="text-[10px] font-bold uppercase tracking-wider" style={{ color: MUTED }}>{label}</span>
      <span className="text-lg font-bold tabular-nums" style={{ color: color ?? TICK }}>{value}</span>
    </div>
  )
}

function EstadoBadge({ value, colorFn }) {
  if (!value) return <span style={{ color: MUTED }}>—</span>
  return (
    <span className="text-[10px] font-bold px-2 py-0.5 rounded-full"
      style={{ background: colorFn(value) + '22', color: colorFn(value), border: `1px solid ${colorFn(value)}55` }}>
      {value}
    </span>
  )
}

// ── Main Component ────────────────────────────────────────────────────────────
export default function ConsultaPage({ openEnCarga }) {
  const { catalogos } = useCatalogos()
  const { rows, totals, loading, page, hasMore, buscar, nextPage, prevPage } = useConsulta()

  const [filters, setFilters] = useState({
    manifiesto: '', fecha_desde: '', fecha_hasta: '',
    conductor_id: null, cliente_id: null, origen_id: null, destino_id: null,
    estado_pago: '', estado_interno: '',
  })
  const [searched, setSearched] = useState(false)

  const set = (k, v) => setFilters(f => ({ ...f, [k]: v }))

  const handleSearch = (e) => {
    e?.preventDefault()
    setSearched(true)
    buscar(filters, 0)
  }

  const handleNext = () => { buscar(filters, page + 1) }
  const handlePrev = () => { buscar(filters, page - 1) }

  const clearAll = () => {
    const empty = {
      manifiesto: '', fecha_desde: '', fecha_hasta: '',
      conductor_id: null, cliente_id: null, origen_id: null, destino_id: null,
      estado_pago: '', estado_interno: '',
    }
    setFilters(empty)
  }

  return (
    <div className="flex flex-col gap-6 pb-8">

      {/* Filter panel */}
      <form onSubmit={handleSearch}
        className="rounded-2xl p-5" style={{ background: BG, border: `1px solid ${BDR}` }}>
        <div className="flex items-center justify-between mb-4">
          <span className="text-sm font-semibold" style={{ color: TICK }}>Filtros de consulta</span>
          <button type="button" onClick={clearAll}
            className="text-xs hover:opacity-80" style={{ color: MUTED }}>Limpiar todo</button>
        </div>

        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-4">
          <Field label="Manifiesto">
            <input className={inputCls} style={{ borderColor: BDR }}
              type="number" placeholder="Número..."
              value={filters.manifiesto}
              onChange={e => set('manifiesto', e.target.value)} />
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

          <FilterSelect
            label="Estado pago"
            value={filters.estado_pago}
            onChange={v => set('estado_pago', v)}
            options={ESTADO_PAGO_OPTS}
          />
        </div>

        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-5">
          <FilterAutocomplete
            label="Conductor"
            items={catalogos.conductores}
            value={filters.conductor_id}
            onChange={v => set('conductor_id', v)}
          />
          <FilterAutocomplete
            label="Cliente"
            items={catalogos.clientes}
            value={filters.cliente_id}
            onChange={v => set('cliente_id', v)}
          />
          <FilterAutocomplete
            label="Origen"
            items={catalogos.lugares}
            value={filters.origen_id}
            onChange={v => set('origen_id', v)}
          />
          <FilterAutocomplete
            label="Destino"
            items={catalogos.lugares}
            value={filters.destino_id}
            onChange={v => set('destino_id', v)}
          />
        </div>

        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-5">
          <FilterSelect
            label="Estado interno"
            value={filters.estado_interno}
            onChange={v => set('estado_interno', v)}
            options={ESTADO_INTERNO_OPTS}
          />
        </div>

        <button type="submit"
          className="flex items-center gap-2 px-5 py-2 rounded-xl text-sm font-semibold transition-opacity hover:opacity-90"
          style={{ background: BLUE, color: '#fff' }}>
          <Search size={14} />
          Consultar
        </button>
      </form>

      {/* KPI bar */}
      {searched && totals && (
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
          <KpiBox label="Manifiestos"    value={fmt(totals.total_manifiestos)} />
          <KpiBox label="Suma remesas"   value={`$${fmt(totals.suma_remesas)}`} />
          <KpiBox label="Suma fletes"    value={`$${fmt(totals.suma_fletes)}`} />
          <KpiBox label="Suma anticipos" value={`$${fmt(totals.suma_anticipos)}`} />
          <KpiBox label="Suma pagado"    value={`$${fmt(totals.suma_pagado)}`} color={GREEN} />
          <KpiBox label="Pendiente"      value={`$${fmt(totals.pendiente_pagar)}`}
            color={Number(totals.pendiente_pagar) > 0 ? GOLD : GREEN} />
        </div>
      )}

      {/* Results table */}
      {searched && (
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
            <div className="overflow-x-auto">
              <table className="w-full text-sm border-collapse">
                <thead>
                  <tr style={{ background: '#0D1B2A' }}>
                    {['Manifiesto','Fecha','Conductor','Vehículo','Cliente','Origen','Destino',
                      'Remesa','Flete','Anticipo','Pagado','Est. Pago','Est. Interno','Factura',''].map(h => (
                      <th key={h} className="px-3 py-2.5 text-left text-[10px] font-bold uppercase tracking-wider whitespace-nowrap"
                        style={{ color: MUTED, borderBottom: `1px solid ${BDR}` }}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {rows.map((r, i) => (
                    <tr key={r.manifiesto}
                      style={{ background: i % 2 === 0 ? BG : '#162333' }}>
                      <td className="px-3 py-2 font-mono font-semibold whitespace-nowrap"
                        style={{ color: GOLD }}>{r.manifiesto}</td>
                      <td className="px-3 py-2 whitespace-nowrap" style={{ color: TICK }}>{fmtDate(r.fecha_despacho)}</td>
                      <td className="px-3 py-2" style={{ color: TICK }}>{r.conductor_nombre ?? '—'}</td>
                      <td className="px-3 py-2 whitespace-nowrap font-mono text-xs" style={{ color: MUTED }}>
                        {r.placa ?? '—'}
                        {r.placa_remolque && <span className="ml-1 opacity-60">/ {r.placa_remolque}</span>}
                      </td>
                      <td className="px-3 py-2" style={{ color: TICK }}>{r.cliente_nombre ?? '—'}</td>
                      <td className="px-3 py-2 whitespace-nowrap" style={{ color: TICK }}>{r.origen_nombre ?? '—'}</td>
                      <td className="px-3 py-2 whitespace-nowrap" style={{ color: TICK }}>{r.destino_nombre ?? '—'}</td>
                      <td className="px-3 py-2 text-right font-mono tabular-nums whitespace-nowrap"
                        style={{ color: TICK }}>${fmt(r.valor_remesa)}</td>
                      <td className="px-3 py-2 text-right font-mono tabular-nums whitespace-nowrap"
                        style={{ color: TICK }}>${fmt(r.flete_conductor)}</td>
                      <td className="px-3 py-2 text-right font-mono tabular-nums whitespace-nowrap"
                        style={{ color: MUTED }}>${fmt(r.anticipo)}</td>
                      <td className="px-3 py-2 text-right font-mono tabular-nums whitespace-nowrap"
                        style={{ color: GREEN }}>${fmt(r.valor_pagado)}</td>
                      <td className="px-3 py-2 whitespace-nowrap">
                        <EstadoBadge value={r.estado_pago} colorFn={estadoPagoColor} />
                      </td>
                      <td className="px-3 py-2 whitespace-nowrap">
                        <EstadoBadge value={r.estado_interno} colorFn={estadoInternoColor} />
                      </td>
                      <td className="px-3 py-2 font-mono text-xs" style={{ color: MUTED }}>
                        {r.factura_no ?? '—'}
                      </td>
                      <td className="px-3 py-2">
                        {openEnCarga && (
                          <button type="button"
                            onClick={() => openEnCarga(r.manifiesto)}
                            className="flex items-center gap-1 text-xs px-2 py-1 rounded-lg transition-opacity hover:opacity-80"
                            style={{ background: BLUE + '22', color: BLUE, border: `1px solid ${BLUE}44` }}>
                            <ExternalLink size={10} />
                            Ver
                          </button>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {/* Pagination */}
          {!loading && rows.length > 0 && (
            <div className="flex items-center justify-between px-4 py-3"
              style={{ borderTop: `1px solid ${BDR}`, background: '#0D1B2A' }}>
              <span className="text-xs" style={{ color: MUTED }}>
                Página {page + 1}{hasMore ? '+' : ''}
                {' · '}{rows.length} resultados
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
      )}
    </div>
  )
}
