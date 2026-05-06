import { useState, useRef } from 'react'
import { Search, ChevronDown, Check, ChevronLeft, ChevronRight, ExternalLink, AlertTriangle } from 'lucide-react'
import { useCatalogos } from '../hooks/useCatalogos'
import { useConsulta }  from '../hooks/useConsulta'

// ── Theme ─────────────────────────────────────────────────────────────────────
const BG    = '#FFFFFF'
const BDR   = '#E2E8F0'
const TICK  = '#0F172A'
const BLUE  = '#1E6FBF'
const GOLD  = '#C9A84C'
const MUTED = '#64748B'
const RED   = '#DC2626'
const GREEN = '#16A34A'

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
const ESTADO_PAGO_OPTS    = ['PAGO A 15 DIAS','PAGO A 20 DIAS','PAGO A 30 DIAS','PAGO A 5-8 DIAS',
  'CONTRAENTREGA','PRONTO PAGO','PAGO NORMAL','PAGO INMEDIATO','URBANO','PAGADO','ANULADO','PRIORITARIO','RNDC','OTROS']
const ESTADO_INTERNO_OPTS = ['CUMPLIDO','NO SE HA CUMPLIDO','PENDIENTE FACTURA ELECTRONICA',
  'FACTURA RECIBIDA','NOVEDAD PENDIENTE','ANULADO']

// Mezcla un color hex con blanco para simular transparencia (evita fondo transparente en sticky)
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

function FilterAutocomplete({ label, items, labelKey = 'nombre', idKey = 'id', value, onChange, placeholder = 'Todos' }) {
  const [query, setQuery] = useState('')
  const [open,  setOpen]  = useState(false)
  const inputRef = useRef(null)

  const selected = items.find(i => i[idKey] === value)
  const display  = selected ? selected[labelKey] : ''
  const filtered = query.length < 1
    ? items.slice(0, 60)
    : items.filter(i => i[labelKey].toLowerCase().includes(query.toLowerCase())).slice(0, 60)

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

// ── Celda de tabla ────────────────────────────────────────────────────────────
function Td({ children, right, mono, muted, nowrap = true, highlight, sticky, bg, style: extraStyle }) {
  return (
    <td className={`px-3 py-2 text-xs${nowrap ? ' whitespace-nowrap' : ''}${right ? ' text-right' : ''}${mono ? ' font-mono tabular-nums' : ''}${sticky ? ' sticky left-0 z-10' : ''}`}
      style={{
        color: highlight ?? (muted ? MUTED : TICK),
        borderRight: `1px solid #CBD5E1`,
        ...(bg ? { background: bg } : {}),
        ...(sticky ? { boxShadow: '2px 0 4px rgba(0,0,0,0.06)' } : {}),
        ...extraStyle,
      }}>
      {children}
    </td>
  )
}

// ── Encabezado de columna ─────────────────────────────────────────────────────
function Th({ children, right, sticky }) {
  return (
    <th className={`px-3 py-2.5 text-[10px] font-bold uppercase tracking-wider whitespace-nowrap${right ? ' text-right' : ' text-left'}${sticky ? ' sticky left-0' : ''}`}
      style={{
        color: MUTED,
        background: '#F1F5F9',
        borderBottom: `1px solid #CBD5E1`,
        borderRight: `1px solid #CBD5E1`,
        zIndex: sticky ? 25 : undefined,
        ...(sticky ? { boxShadow: '2px 0 4px rgba(0,0,0,0.06)' } : {}),
      }}>
      {children}
    </th>
  )
}

// ── Main Component ────────────────────────────────────────────────────────────
export default function ConsultaPage({ openEnCarga }) {
  const { catalogos } = useCatalogos()
  const { rows, loading, page, hasMore, buscar } = useConsulta()

  const [filters, setFilters] = useState({
    manifiesto: '', fecha_desde: '', fecha_hasta: '',
    conductor: null, cliente: null, origen: null, destino: null,
    compromiso_pago: '', estado_interno: '', placa: '', mes: '', año: '',
  })
  const [searched, setSearched] = useState(false)

  const set = (k, v) => setFilters(f => ({ ...f, [k]: v }))

  const handleSearch = e => {
    e?.preventDefault()
    setSearched(true)
    buscar(filters, 0)
  }

  const handleNext = () => { buscar(filters, page + 1) }
  const handlePrev = () => { buscar(filters, page - 1) }

  const clearAll = () => {
    setFilters({
      manifiesto: '', fecha_desde: '', fecha_hasta: '',
      conductor: null, cliente: null, origen: null, destino: null,
      compromiso_pago: '', estado_interno: '', placa: '', mes: '', año: '',
    })
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

        {/* Fila 1 */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-4">
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
        </div>

        {/* Fila 2 */}
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-4 mb-4">
          <FilterSelect label="Mes" value={filters.mes} onChange={v => set('mes', v)} options={MESES_OPTS} />
          <FilterSelect label="Año" value={filters.año} onChange={v => set('año', v)} options={AÑOS_OPTS} />
          <FilterSelect label="Compromiso de pago" value={filters.compromiso_pago} onChange={v => set('compromiso_pago', v)} options={ESTADO_PAGO_OPTS} />
          <FilterSelect label="Estado interno" value={filters.estado_interno} onChange={v => set('estado_interno', v)} options={ESTADO_INTERNO_OPTS} />
          <FilterAutocomplete label="Conductor" items={catalogos.conductores}
            value={filters.conductor} onChange={v => set('conductor', v)} />
        </div>

        {/* Fila 3 */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-5">
          <FilterAutocomplete label="Cliente" items={catalogos.clientes}
            value={filters.cliente} onChange={v => set('cliente', v)} />
          <FilterAutocomplete label="Origen" items={catalogos.lugares}
            value={filters.origen} onChange={v => set('origen', v)} />
          <FilterAutocomplete label="Destino" items={catalogos.lugares}
            value={filters.destino} onChange={v => set('destino', v)} />
        </div>

        <button type="submit"
          className="flex items-center gap-2 px-5 py-2 rounded-xl text-sm font-semibold transition-opacity hover:opacity-90"
          style={{ background: BLUE, color: '#fff' }}>
          <Search size={14} /> Consultar
        </button>
      </form>

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
            <div style={{ maxHeight: '68vh', overflow: 'auto' }}>
              <table className="text-sm border-collapse" style={{ minWidth: '2900px' }}>
                <thead style={{ position: 'sticky', top: 0, zIndex: 15 }}>
                  <tr style={{ background: '#F1F5F9' }}>
                    <Th sticky>Manifiesto</Th>
                    <Th>Remesas</Th>
                    <Th>F. Despacho</Th>
                    <Th>Origen</Th>
                    <Th>Dpto. Origen</Th>
                    <Th>Destino</Th>
                    <Th>Dpto. Destino</Th>
                    <Th>Cliente</Th>
                    <Th right>Valor Remesa</Th>
                    <Th right>Flete Neto</Th>
                    <Th right>Anticipo</Th>
                    <Th>Placa</Th>
                    <Th>Tipo Veh.</Th>
                    <Th>Conductor</Th>
                    <Th>Celular</Th>
                    <Th>Cédula</Th>
                    <Th>Propietario</Th>
                    <Th>Agencia</Th>
                    <Th>Responsable</Th>
                    <Th>F. Cumplido</Th>
                    <Th right>Días</Th>
                    <Th>Compromiso Pago</Th>
                    <Th>Novedades</Th>
                    <Th>Novedad Conductor</Th>
                    <Th>Novedad Empresa</Th>
                    <Th right>Aj. Positivo Flete</Th>
                    <Th right>Aj. Negativo Flete</Th>
                    <Th>Estado Interno</Th>
                    <Th>Resp. Estado Int.</Th>
                    <Th>F. Pago</Th>
                    <Th right>Valor Pagado</Th>
                    <Th>Entidad</Th>
                    <Th>Responsable Pago</Th>
                    <Th>Factura No</Th>
                    <Th>Fecha Emisión Factura</Th>
                    <Th>Mes</Th>
                    <Th>Factura Electrónica</Th>
                    <Th right>Días Fact.</Th>
                    <Th></Th>
                  </tr>
                </thead>
                <tbody>
                  {rows.map((r, i) => {
                    const plazoVencido = r.dias_cumplido > 20 && !r.fecha_pago
                    const sidebarColor = SIDEBAR_COLOR[r.estado_interno]
                    const rowBg = sidebarColor
                      ? sidebarColor + '66'
                      : i % 2 === 0 ? BG : '#F8FAFC'
                    // Fondo opaco para la celda sticky (sin transparencia que deje ver texto debajo)
                    const stickyBg = sidebarColor ? blendOnWhite(sidebarColor, 0.4) : rowBg
                    return (
                      <tr key={r.manifiesto} style={{ background: rowBg }}>
                        <Td mono highlight={GOLD} sticky bg={stickyBg}>{r.manifiesto}</Td>
                        <Td muted>{r.remesas || '—'}</Td>
                        <Td>{fmtDate(r.fecha_despacho)}</Td>
                        <Td>{r.origen ?? '—'}</Td>
                        <Td muted>{r.departamento_origen ?? '—'}</Td>
                        <Td>{r.destino ?? '—'}</Td>
                        <Td muted>{r.departamento_destino ?? '—'}</Td>
                        <Td>{r.cliente ?? '—'}</Td>
                        <Td right mono>{money(r.valor_remesa)}</Td>
                        <Td right mono>{money(r.flete_neto_conductor ?? r.flete_conductor)}</Td>
                        <Td right mono muted>{money(r.anticipo)}</Td>
                        <Td mono muted>{r.placa ?? '—'}</Td>
                        <Td muted>{r.tipo_vehiculo ?? '—'}</Td>
                        <Td>{r.conductor ?? '—'}</Td>
                        <Td muted>{r.celular ?? '—'}</Td>
                        <Td muted>{r.cedula_conductor ?? '—'}</Td>
                        <Td muted>{r.propietario ?? '—'}</Td>
                        <Td muted>{r.agencia_despachadora ?? '—'}</Td>
                        <Td muted>{r.nombre_responsable ?? '—'}</Td>
                        <Td>{fmtDate(r.fecha_cumplido)}</Td>
                        <td className="px-3 py-2 text-xs text-right whitespace-nowrap">
                          {r.dias_cumplido != null ? (
                            <div className="flex items-center justify-end gap-1.5">
                              <span style={{ color: plazoVencido ? RED : TICK }}>{r.dias_cumplido}</span>
                              <PlazoBadge diasCumplidos={r.dias_cumplido} fechaPago={r.fecha_pago} />
                            </div>
                          ) : <span style={{ color: MUTED }}>—</span>}
                        </td>
                        <td className="px-3 py-2 whitespace-nowrap">
                          <EstadoBadge value={r.compromiso_pago} colorFn={estadoPagoColor} />
                        </td>
                        <td className="px-3 py-2 text-xs max-w-45"
                          style={{ color: MUTED, whiteSpace: 'normal', wordBreak: 'break-word' }}>
                          {r.novedades || '—'}
                        </td>
                        <td className="px-3 py-2 text-xs max-w-45"
                          style={{ color: MUTED, whiteSpace: 'normal', wordBreak: 'break-word' }}>
                          {r.novedad_conductor || '—'}
                        </td>
                        <td className="px-3 py-2 text-xs max-w-45"
                          style={{ color: MUTED, whiteSpace: 'normal', wordBreak: 'break-word' }}>
                          {r.novedad_empresa || '—'}
                        </td>
                        <Td right mono>{r.ajuste_positivo_flete != null ? money(r.ajuste_positivo_flete) : '—'}</Td>
                        <Td right mono>{r.ajuste_negativo_flete != null ? money(r.ajuste_negativo_flete) : '—'}</Td>
                        <td className="px-3 py-2 whitespace-nowrap">
                          <EstadoBadge value={r.estado_interno} colorFn={estadoInternoColor} />
                        </td>
                        <Td muted>{r.responsable_estado_interno ?? '—'}</Td>
                        <Td>{fmtDate(r.fecha_pago)}</Td>
                        <Td right mono highlight={GREEN}>{money(r.valor_pagado)}</Td>
                        <Td muted>{r.entidad_financiera ?? '—'}</Td>
                        <Td muted>{r.responsable ?? '—'}</Td>
                        <Td mono muted>{r.factura_no ?? (
                          <span className="text-[10px] px-1.5 py-0.5 rounded font-semibold"
                            style={{ background: GOLD + '22', color: GOLD, border: `1px solid ${GOLD}44` }}>
                            Sin factura
                          </span>
                        )}</Td>
                        <Td muted>{fmtDate(r.fecha_factura)}</Td>
                        <Td muted>{r.mes ?? '—'}</Td>
                        <Td muted>{r.factura_electronica ?? '—'}</Td>
                        <Td right mono muted>{r.dias_para_facturar ?? '—'}</Td>
                        <td className="px-3 py-2 sticky right-0"
                          style={{ background: stickyBg, boxShadow: '-2px 0 4px rgba(0,0,0,0.06)' }}>
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
      )}
    </div>
  )
}
