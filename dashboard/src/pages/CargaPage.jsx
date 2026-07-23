import { useState, useEffect, useRef } from 'react'
import * as XLSX from 'xlsx'
import { Search, ArrowLeft, Save, CheckCircle, AlertCircle,
         User, MapPin, DollarSign, FileText, ClipboardList,
         ChevronDown, Check, Calendar, Pencil, Trash2, X,
         ChevronUp, RotateCcw, Lock, Upload, AlertTriangle } from 'lucide-react'
import { supabase } from '../lib/supabase'
import { useCatalogos } from '../hooks/useCatalogos'
import { useManifiesto } from '../hooks/useManifiesto'
import { normalizeVal } from '../lib/normalize'
import { buildPayload } from '../lib/excel-upload'

// ── ENUMs ────────────────────────────────────────────────────────────────────
const COMPROMISO_PAGO_OPTS   = ['PAGO A 15 DIAS','CONTRAENTREGA','PRONTO PAGO','PAGO INMEDIATO']
const ENTIDAD_FIN_OPTS       = ['TRANSF BANCOLOMBIA','TRANSF BANCO DE BOGOTA','TRANSF DAVIVIENDA','CHEQUE BANCOLOMBIA','CHEQUE BANCO DE BOGOTA','CHEQUE DAVIVIENDA']
const ESTADO_INTERNO_OPTS    = ['CUMPLIDO','NO SE HA CUMPLIDO','PENDIENTE FACTURA ELECTRONICA','FACTURA RECIBIDA','NOVEDAD PENDIENTE','ANULADO']

// ── Theme ────────────────────────────────────────────────────────────────────
const BG   = '#FFFFFF'
const BDR  = '#E2E8F0'
const TICK = '#0F172A'
const BLUE = '#1E6FBF'
const GOLD = '#C9A84C'
const MUTED = '#64748B'
const BTN_GRAD = 'linear-gradient(135deg, #1E6FBF 0%, #6366F1 100%)'
const BTN_SHADOW = '0 2px 8px 0 rgba(30,111,191,0.22)'

// ── Primitives ───────────────────────────────────────────────────────────────
const inputCls = `w-full rounded-md border px-3 py-2 text-sm focus:outline-none focus:ring-1
  focus:ring-[#1E6FBF] transition-colors bg-transparent text-[#0F172A] placeholder:text-[#64748B]`

function Field({ label, col = 1, children }) {
  return (
    <div style={{ gridColumn: `span ${col}` }}>
      <label className="block text-[10px] font-bold uppercase tracking-wider mb-1.5"
        style={{ color: MUTED }}>{label}</label>
      {children}
    </div>
  )
}

function Input({ label, col, ...props }) {
  return (
    <Field label={label} col={col}>
      <input className={inputCls} style={{ borderColor: BDR }} {...props} />
    </Field>
  )
}

function MoneyInput({ label, col, value, onChange }) {
  const n = value !== '' && value != null ? Number(value) : null
  const display = n != null && !isNaN(n) ? `$ ${n.toLocaleString('es-CO')}` : ''
  return (
    <Field label={label} col={col}>
      <input
        className={inputCls} style={{ borderColor: BDR }}
        type="text" inputMode="numeric"
        value={display}
        placeholder="$ 0"
        onChange={e => onChange({ target: { value: e.target.value.replace(/[^0-9]/g, '') } })}
      />
    </Field>
  )
}

function PhoneInput({ label, col, value, onChange }) {
  const raw = (value ?? '').replace(/[^0-9]/g, '').slice(0, 10)
  const display = raw ? `${raw.slice(0, 3)} ${raw.slice(3, 6)} ${raw.slice(6)}` : ''
  const invalid = raw.length > 0 && raw.length !== 10
  const border = invalid ? '#EF4444' : BDR
  return (
    <Field label={label} col={col}>
      <input
        className={inputCls} style={{ borderColor: border }}
        type="text" inputMode="numeric"
        value={display}
        placeholder="300 123 4567"
        onChange={e => {
          const digits = e.target.value.replace(/[^0-9]/g, '').slice(0, 10)
          onChange({ target: { value: digits } })
        }}
      />
      {invalid && (
        <p className="text-[10px] mt-0.5" style={{ color: '#EF4444' }}>
          Debe tener exactamente 10 dígitos
        </p>
      )}
    </Field>
  )
}

function Select({ label, col, value, onChange, options, placeholder = 'Seleccionar...' }) {
  const [open, setOpen] = useState(false)
  return (
    <Field label={label} col={col}>
      <div className="relative">
        <button
          type="button"
          onClick={() => setOpen(v => !v)}
          onBlur={() => setTimeout(() => setOpen(false), 150)}
          className="w-full flex items-center justify-between px-3 py-2 text-sm rounded-xl border focus:outline-none focus:ring-1 focus:ring-[#1E6FBF] transition-colors"
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
            style={{ background: '#F8FAFC', border: `1px solid ${BDR}` }}>
            <button type="button" onMouseDown={() => { onChange(''); setOpen(false) }}
              className="w-full text-left px-3 py-2 text-sm transition-colors hover:bg-black/5"
              style={{ color: MUTED }}>{placeholder}</button>
            {options.map(o => (
              <button key={o} type="button" onMouseDown={() => { onChange(o); setOpen(false) }}
                className="w-full text-left px-3 py-2 text-sm transition-colors hover:bg-black/5 flex items-center justify-between"
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

function DateInput({ label, col, value, onChange, disabled }) {
  return (
    <Field label={label} col={col}>
      <div className="relative">
        <input type="date" value={value} onChange={onChange}
          disabled={disabled}
          className={inputCls} style={{ borderColor: BDR, opacity: disabled ? 0.5 : 1 }} />
        <Calendar size={13} className="absolute right-3 top-1/2 -translate-y-1/2 pointer-events-none"
          style={{ color: BLUE }} />
      </div>
    </Field>
  )
}

function Autocomplete({ label, col, displayValue, onSelect, onCreate, options, placeholder }) {
  const [query, setQuery] = useState(displayValue || '')
  const [open,  setOpen]  = useState(false)
  const [saving, setSaving] = useState(false)
  const containerRef = useRef(null)

  // eslint-disable-next-line react-hooks/set-state-in-effect
  useEffect(() => { setQuery(displayValue || '') }, [displayValue])

  const filtered = options
    .filter(o => o.label.toLowerCase().includes(query.toLowerCase()))
    .slice(0, 10)

  const exactMatch = options.some(o => o.label.toLowerCase() === query.trim().toLowerCase())
  const showCreate = onCreate && query.trim().length >= 2 && !exactMatch

  const handleCreate = async () => {
    setSaving(true)
    try {
      const created = await onCreate(query.trim())
      if (created) onSelect({ id: created.id ?? created.nombre ?? created.placa, label: query.trim() })
      setOpen(false)
    } finally { setSaving(false) }
  }

  const handleBlur = () => setTimeout(() => {
    if (!containerRef.current?.contains(document.activeElement)) setOpen(false)
  }, 150)

  return (
    <Field label={label} col={col}>
      <div className="relative" ref={containerRef}>
        <input value={query} autoComplete="off" placeholder={placeholder}
          className={inputCls} style={{ borderColor: BDR }}
          onChange={e => { setQuery(e.target.value); setOpen(true) }}
          onFocus={() => setOpen(true)}
          onBlur={handleBlur}
        />
        {open && (filtered.length > 0 || showCreate) && (
          <div className="absolute z-50 w-full mt-1 rounded-xl shadow-xl overflow-hidden"
            style={{ background: '#F8FAFC', border: `1px solid ${BDR}` }}>
            <div className="max-h-52 overflow-y-auto">
              {filtered.map(o => (
                <button key={o.id} type="button" onMouseDown={() => { onSelect(o); setQuery(o.label); setOpen(false) }}
                  className="w-full text-left px-3 py-2 text-sm transition-colors hover:bg-black/5 flex items-center gap-2"
                  style={{ color: TICK }}>
                  <span>{o.label}</span>
                  {o.sub && <span className="text-xs opacity-40">{o.sub}</span>}
                </button>
              ))}
              {showCreate && (
                <button type="button" disabled={saving} onMouseDown={handleCreate}
                  className="w-full text-left px-3 py-2 text-sm border-t transition-colors hover:bg-black/5 flex items-center gap-2"
                  style={{ color: BLUE, borderColor: BDR }}>
                  <span>{`Usar "${query.trim()}"`}</span>
                </button>
              )}
            </div>
          </div>
        )}
      </div>
    </Field>
  )
}

function PanelManifiestosFE({ manifiestos, manifiestoActual, loading }) {
  if (loading) {
    return (
      <div className="rounded-xl p-5 flex flex-col gap-4" style={{ background: BG, border: `1px solid ${BDR}` }}>
        <p className="text-sm" style={{ color: MUTED }}>Cargando manifiestos relacionados...</p>
      </div>
    )
  }
  if (!manifiestos || manifiestos.length === 0) return null

  const totalValor = manifiestos.reduce((s, m) => s + (Number(m.valor_factura) || 0), 0)
  const totalSaldo = manifiestos.reduce((s, m) => s + (Number(m.saldo) || 0), 0)

  return (
    <div className="rounded-xl p-5 flex flex-col gap-4" style={{ background: BG, border: `1px solid ${BDR}` }}>
      <div className="flex items-center gap-2 pb-3 border-b" style={{ borderColor: BDR }}>
        <FileText size={13} color={BLUE} />
        <p className="text-xs font-bold uppercase tracking-widest" style={{ color: TICK }}>
          Manifiestos con esta FE
        </p>
      </div>

      <div className="grid grid-cols-[80px_1fr_140px_140px] gap-2 text-[10px] font-bold uppercase tracking-wider px-3"
        style={{ color: MUTED }}>
        <span>Manifiesto</span>
        <span>Cliente</span>
        <span className="text-right">Valor factura</span>
        <span className="text-right">Saldo</span>
      </div>

      <div className="flex flex-col gap-1">
        {manifiestos.map(m => {
          const esActual = m.manifiesto === manifiestoActual
          return (
            <div key={m.manifiesto}
              className="grid grid-cols-[80px_1fr_140px_140px] gap-2 items-center px-3 py-2 rounded-lg text-sm"
              style={{
                background: esActual ? BLUE + '12' : 'transparent',
                border: esActual ? `1px solid ${BLUE}44` : `1px solid transparent`,
              }}>
              <span className="font-mono font-bold" style={{ color: GOLD }}>{m.manifiesto}</span>
              <span className="truncate" style={{ color: TICK }}>{m.cliente ?? '—'}</span>
              <span className="text-right font-mono" style={{ color: TICK }}>
                {m.valor_factura != null ? `$${Number(m.valor_factura).toLocaleString('es-CO')}` : '—'}
              </span>
              <span className="text-right font-mono" style={{ color: TICK }}>
                {m.saldo != null ? `$${Number(m.saldo).toLocaleString('es-CO')}` : '—'}
              </span>
            </div>
          )
        })}
      </div>

      <div className="border-t pt-3" style={{ borderColor: BDR }}>
        <div className="grid grid-cols-[80px_1fr_140px_140px] gap-2 items-center px-3 text-sm font-bold">
          <span style={{ color: MUTED }}>TOTALES</span>
          <span className="text-xs" style={{ color: MUTED }}>
            {manifiestos.length} manifiesto{manifiestos.length !== 1 ? 's' : ''}
          </span>
          <span className="text-right font-mono" style={{ color: TICK }}>
            ${totalValor.toLocaleString('es-CO')}
          </span>
          <span className="text-right font-mono" style={{ color: TICK }}>
            ${totalSaldo.toLocaleString('es-CO')}
          </span>
        </div>
      </div>
    </div>
  )
}

function SectionCard({ icon: Icon, title, children, cols = 3 }) { // eslint-disable-line no-unused-vars
  return (
    <div className="rounded-xl p-5 flex flex-col gap-4" style={{ background: BG, border: `1px solid ${BDR}` }}>
      <div className="flex items-center gap-2 pb-3 border-b" style={{ borderColor: BDR }}>
        <Icon size={13} color={BLUE} />
        <p className="text-xs font-bold uppercase tracking-widest" style={{ color: TICK }}>{title}</p>
      </div>
      <div className="grid gap-4" style={{ gridTemplateColumns: `repeat(${cols}, minmax(0, 1fr))` }}>
        {children}
      </div>
    </div>
  )
}

function Toast({ msg, onClose }) {
  useEffect(() => {
    const t = setTimeout(onClose, 4000)
    return () => clearTimeout(t)
  }, [onClose])
  const ok = msg.type === 'success'
  return (
    <div className="fixed bottom-6 right-6 z-50 flex items-center gap-3 px-4 py-3 rounded-xl shadow-2xl text-sm font-medium"
      style={{ background: ok ? '#F0FDF4' : '#FEF2F2', border: `1px solid ${ok ? '#86EFAC' : '#FECACA'}`, color: ok ? '#166534' : '#DC2626' }}>
      {ok ? <CheckCircle size={15} /> : <AlertCircle size={15} />}
      {msg.text}
    </div>
  )
}

// ── Form initial state ───────────────────────────────────────────────────────
const SEGUIMIENTO_INIT = {
  fecha_cumplido: '', compromiso_pago: 'PAGO A 15 DIAS', novedades: '',
  estado_interno: '', responsable_estado_interno: '',
  novedad_conductor: '', novedad_empresa: '',
  ajuste_positivo_flete: '', ajuste_negativo_flete: '',
  consignacion_a_terceros: '',
}

const TESORERIA_INIT = {
  fecha_pago: '', valor_pagado: '', entidad_financiera: '', responsable: '',
}

const FACT_INIT = {
  factura_no: '', fecha_factura: '', factura_electronica: '', mes_facturacion: '',
  valor_factura: '',
}

// ── Excel Upload Panel ───────────────────────────────────────────────────────
// Campos comparables entre payload y DB para detectar cambios
const DB_FIELDS = [
  'fecha_despacho','origen','destino','cliente','conductor','cedula_conductor','celular',
  'placa','placa_remolque','propietario','agencia_despachadora','nombre_responsable',
  'valor_remesa','flete_conductor','anticipo','remesas',
]
const FIELD_LABELS = {
  fecha_despacho:'Fecha despacho', origen:'Origen', destino:'Destino',
  cliente:'Cliente', conductor:'Conductor', cedula_conductor:'Cédula', celular:'Celular',
  placa:'Placa', placa_remolque:'Remolque', propietario:'Propietario',
  agencia_despachadora:'Agencia Despachadora', nombre_responsable:'Responsable',
  valor_remesa:'Valor remesa', flete_conductor:'Flete', anticipo:'Anticipo',
  remesas:'Remesas',
}
const UPLOAD_BATCH = 20

// Markup compartido entre preview y revisión: lista de filas Excel sin manifiesto válido.
function ErrorRows({ invalid, title }) {
  return (
    <div className="rounded-lg overflow-hidden" style={{ border: `1px solid #FECACA` }}>
      <div className="px-3 py-1.5 text-[10px] font-bold uppercase tracking-wider"
        style={{ background: '#FEF2F2', color: '#DC2626' }}>
        {title}
      </div>
      <div style={{ maxHeight: 180, overflowY: 'auto' }}>
        {invalid.map((e, i) => (
          <div key={i} className="grid grid-cols-[48px_140px_160px_1fr] px-3 py-1.5 text-xs border-t"
            style={{ borderColor: '#FECACA', color: TICK }}>
            <span style={{ color: MUTED }}>Fila {e.fila}</span>
            <span style={{ color: '#DC2626', fontWeight: 600 }}>{e.campo ?? '—'}</span>
            <span style={{ color: MUTED, fontFamily: 'monospace' }}>{e.valor ?? '—'}</span>
            <span>{e.error}</span>
          </div>
        ))}
      </div>
    </div>
  )
}

function ExcelUploadPanel({ onDone }) {
  const RED = '#DC2626'

  const [dragging,      setDragging]      = useState(false)
  const [preview,       setPreview]       = useState(null)
  const [revision,      setRevision]      = useState(null)  // { nuevos, sinCambios, conCambios, invalid }
  const [seleccionados, setSeleccionados] = useState(new Set())
  const [expandidos,    setExpandidos]    = useState(new Set())
  const [result,        setResult]        = useState(null)
  const [busy,          setBusy]          = useState(false)
  const [progress,      setProgress]      = useState(0)
  const inputRef = useRef(null)

  // Paso 1: parsear archivo → mostrar preview
  const parseFile = async (file) => {
    if (!file) return
    setResult(null); setPreview(null); setRevision(null)
    try {
      const buf  = await file.arrayBuffer()
      const wb   = XLSX.read(buf, { type: 'array', cellDates: false })
      const ws   = wb.Sheets[wb.SheetNames[0]]
      const rows = XLSX.utils.sheet_to_json(ws, { defval: null })

      if (rows.length === 0) {
        setResult({ ok: 0, actualizados: 0, omitidos: 0, errores: [{ fila: '-', msg: 'El archivo está vacío o no tiene datos.' }] })
        return
      }

      const payloads    = rows.map((r, i) => buildPayload(r, i, file.name))
      const valid       = payloads.filter(p => p.payload)
      const invalid     = payloads.filter(p => p.error)
      const previewRows = valid.slice(0, 5).map(p => ({
        manifiesto: p.payload.p_manifiesto,
        fecha:      p.payload.p_fecha_despacho ?? '—',
        conductor:  p.payload.p_conductor      ?? '—',
        origen:     p.payload.p_origen         ?? '—',
        destino:    p.payload.p_destino        ?? '—',
        cliente:    p.payload.p_cliente        ?? '—',
      }))
      setPreview({ fileName: file.name, valid, invalid, previewRows })
    } catch (err) {
      setResult({ ok: 0, actualizados: 0, omitidos: 0, errores: [{ fila: '-', msg: err.message }] })
    }
  }

  // Paso 2: consultar DB y categorizar cambios
  const verificarCambios = async () => {
    if (!preview) return
    setBusy(true)
    try {
      const { valid, invalid } = preview
      const numeros  = valid.map(p => p.payload.p_manifiesto)
      const dbSelect = DB_FIELDS.join(',') + ',manifiesto'
      // Chunk para no exceder max-rows=1000 de PostgREST
      const CHUNK = 900
      let allRows = []
      for (let i = 0; i < numeros.length; i += CHUNK) {
        const chunk = numeros.slice(i, i + CHUNK)
        const { data, error } = await supabase
          .from('manifiestos_flat').select(dbSelect).in('manifiesto', chunk)
        if (error) throw error
        allRows = allRows.concat(data ?? [])
      }

      const dbMap      = new Map(allRows.map(r => [r.manifiesto, r]))
      const nuevos     = []
      const sinCambios = []
      const conCambios = []

      for (const p of valid) {
        const num   = p.payload.p_manifiesto
        const dbRow = dbMap.get(num)
        if (!dbRow) { nuevos.push(p); continue }
        const diffs = DB_FIELDS
          .map(f => ({ field: f, valDB: normalizeVal(dbRow[f], f), valNew: normalizeVal(p.payload[`p_${f}`], f) }))
          .filter(d => d.valDB !== d.valNew)
        if (diffs.length === 0) sinCambios.push(p)
        else conCambios.push({ ...p, diffs })
      }

      setRevision({ nuevos, sinCambios, conCambios, invalid })
      setSeleccionados(new Set(conCambios.map(p => p.payload.p_manifiesto)))
      setExpandidos(new Set())
      setBusy(false)
    } catch (err) {
      setResult({ ok: 0, actualizados: 0, omitidos: 0, errores: [{ fila: '-', msg: err.message }] })
      setBusy(false)
    }
  }

  // Paso 3: ejecutar la carga real
  const ejecutarCarga = async (nuevos, aceptados, nSinCambios, invalid) => {
    setBusy(true); setProgress(0); setRevision(null); setPreview(null)
    const errores  = (invalid || []).map(p => ({ fila: p.fila, msg: p.error }))
    let ok = 0; let actualizados = 0
    const toUpload = [
      ...nuevos.map(p => ({ ...p, _isNuevo: true })),
      ...aceptados.map(p => ({ ...p, _isNuevo: false })),
    ]
    for (let i = 0; i < toUpload.length; i += UPLOAD_BATCH) {
      const chunk = toUpload.slice(i, i + UPLOAD_BATCH)
      const res   = await Promise.all(chunk.map(p => supabase.rpc('guardar_digitador_batch', p.payload)))
      res.forEach((r, j) => {
        if (r.error) errores.push({ fila: chunk[j].fila, msg: r.error.message })
        else if (chunk[j]._isNuevo) ok++
        else actualizados++
      })
      setProgress(Math.round(((i + chunk.length) / Math.max(toUpload.length, 1)) * 100))
    }
    setResult({ ok, actualizados, omitidos: nSinCambios, errores })
    setBusy(false)
    if (ok > 0 || actualizados > 0) onDone?.()
  }

  const confirmarRevision = () => {
    if (!revision) return
    const aceptados = revision.conCambios.filter(p => seleccionados.has(p.payload.p_manifiesto))
    const rechazados = revision.conCambios.length - aceptados.length
    ejecutarCarga(revision.nuevos, aceptados, revision.sinCambios.length + rechazados, revision.invalid)
  }

  const cancelar = () => { setPreview(null); setResult(null); setRevision(null) }

  const toggleSeleccionado = (num) => setSeleccionados(prev => {
    const next = new Set(prev); next.has(num) ? next.delete(num) : next.add(num); return next
  })
  const toggleExpandido = (num) => setExpandidos(prev => {
    const next = new Set(prev); next.has(num) ? next.delete(num) : next.add(num); return next
  })

  const onDrop = (e) => {
    e.preventDefault(); setDragging(false)
    const file = e.dataTransfer.files[0]
    if (file) parseFile(file)
  }

  const totalImportar = revision ? revision.nuevos.length + seleccionados.size : 0

  return (
    <div className="flex flex-col gap-4">

      {/* Drop zone */}
      {!preview && !result && !revision && (
        <div
          onDragOver={e => { e.preventDefault(); setDragging(true) }}
          onDragLeave={() => setDragging(false)}
          onDrop={onDrop}
          onClick={() => inputRef.current?.click()}
          className="flex flex-col items-center justify-center gap-4 rounded-xl border-2 border-dashed py-20 cursor-pointer transition-colors"
          style={{ borderColor: dragging ? BLUE : BDR, background: dragging ? BLUE + '08' : '#FAFBFC' }}>
          <input ref={inputRef} type="file" accept=".xlsx,.xls" className="hidden"
            onChange={e => { if (e.target.files[0]) parseFile(e.target.files[0]) }} />
          <Upload size={36} style={{ color: dragging ? BLUE : MUTED }} />
          <p className="text-base font-medium" style={{ color: TICK }}>Arrastre el archivo Excel aquí o haga clic para seleccionarlo</p>
          <p className="text-sm" style={{ color: MUTED }}>.xlsx · .xls · Una hoja por archivo</p>
        </div>
      )}

      {/* Panel de preview */}
      {preview && !busy && !revision && (
        <div className="flex flex-col gap-4 rounded-xl p-4" style={{ background: BG, border: `1px solid ${BDR}` }}>
          <div>
            <p className="text-sm font-semibold" style={{ color: TICK }}>{preview.fileName}</p>
            <p className="text-xs mt-0.5" style={{ color: MUTED }}>
              {preview.valid.length} registros válidos
              {preview.invalid.length > 0 && <span style={{ color: RED }}> · {preview.invalid.length} con error</span>}
            </p>
          </div>

          <div className="rounded-lg overflow-hidden" style={{ border: `1px solid ${BDR}` }}>
            <div className="grid grid-cols-[80px_90px_1fr_1fr_1fr_1fr] px-3 py-1.5 text-[10px] font-bold uppercase tracking-wider"
              style={{ background: '#F1F5F9', color: MUTED, borderBottom: `1px solid ${BDR}` }}>
              <span>Manifiesto</span><span>Fecha</span><span>Conductor</span>
              <span>Origen</span><span>Destino</span><span>Cliente</span>
            </div>
            {preview.previewRows.map((r, i) => (
              <div key={i} className="grid grid-cols-[80px_90px_1fr_1fr_1fr_1fr] px-3 py-1.5 text-xs border-t"
                style={{ borderColor: BDR, color: TICK, background: i % 2 === 0 ? BG : '#F8FAFC' }}>
                <span className="font-mono" style={{ color: GOLD }}>{r.manifiesto}</span>
                <span style={{ color: MUTED }}>{r.fecha}</span>
                <span className="truncate">{r.conductor}</span>
                <span className="truncate" style={{ color: MUTED }}>{r.origen}</span>
                <span className="truncate" style={{ color: MUTED }}>{r.destino}</span>
                <span className="truncate">{r.cliente}</span>
              </div>
            ))}
            {preview.valid.length > 5 && (
              <div className="px-3 py-1.5 text-xs border-t" style={{ borderColor: BDR, color: MUTED, background: '#F8FAFC' }}>
                ...y {preview.valid.length - 5} registros más
              </div>
            )}
          </div>

          {preview.invalid.length > 0 && (
            <ErrorRows invalid={preview.invalid} title="Filas con error (no se importarán)" />
          )}

          <div className="flex gap-3">
            <button type="button" onClick={verificarCambios}
              className="flex items-center gap-2 px-5 py-2 rounded-xl text-sm font-semibold transition-opacity hover:opacity-90"
              style={{ background: BTN_GRAD, color: '#fff', boxShadow: BTN_SHADOW }}>
              <Upload size={14} /> Verificar e importar
            </button>
            <button type="button" onClick={cancelar}
              className="px-4 py-2 rounded-xl text-sm font-semibold transition-opacity hover:opacity-80"
              style={{ background: BDR, color: TICK }}>
              Cancelar
            </button>
          </div>
        </div>
      )}

      {/* Panel de revisión — manifiestos con cambios */}
      {revision && !busy && (
        <div className="flex flex-col gap-4 rounded-xl p-4" style={{ background: BG, border: `1px solid ${BDR}` }}>

          {/* Resumen del archivo */}
          <div className="flex flex-col gap-3 pb-3" style={{ borderBottom: `1px solid ${BDR}` }}>
            <p className="text-xs font-bold uppercase tracking-wider" style={{ color: MUTED }}>
              Resumen del archivo
            </p>

            {/* Pill del total detectado — neutral */}
            <div className="self-start inline-flex items-baseline gap-2 rounded-full px-4 py-2"
                 style={{ background: '#F1F5F9', border: `1px solid ${BDR}` }}>
              <span className="text-2xl font-bold leading-none" style={{ color: TICK }}>
                {revision.nuevos.length + revision.conCambios.length + revision.sinCambios.length + revision.invalid.length}
              </span>
              <span className="text-xs font-medium" style={{ color: MUTED }}>
                manifiestos detectados en el Excel
              </span>
            </div>

            {/* Pills por categoría */}
            <div className="flex flex-wrap gap-2">
              {/* Nuevos — azul llamativo */}
              <div className="inline-flex items-center gap-2 rounded-full pl-3 pr-4 py-1.5"
                   style={{ background: '#DBEAFE', border: '1px solid #93C5FD' }}>
                <span className="inline-flex items-center justify-center text-sm font-bold rounded-full px-2 py-0.5"
                      style={{ background: '#2563EB', color: '#FFFFFF', minWidth: 24 }}>
                  {revision.nuevos.length}
                </span>
                <span className="text-xs font-semibold uppercase tracking-wider" style={{ color: '#1E3A8A' }}>
                  Nuevos
                </span>
              </div>

              {/* Requieren revisión — amarillo llamativo */}
              <div className="inline-flex items-center gap-2 rounded-full pl-3 pr-4 py-1.5"
                   style={{ background: '#FEF3C7', border: '1px solid #FCD34D' }}>
                <span className="inline-flex items-center justify-center text-sm font-bold rounded-full px-2 py-0.5"
                      style={{ background: '#D97706', color: '#FFFFFF', minWidth: 24 }}>
                  {revision.conCambios.length}
                </span>
                <span className="text-xs font-semibold uppercase tracking-wider" style={{ color: '#78350F' }}>
                  Requieren revisión
                </span>
              </div>

              {/* Sin cambios — gris sutil */}
              <div className="inline-flex items-center gap-2 rounded-full pl-3 pr-4 py-1.5"
                   style={{ background: '#F1F5F9', border: `1px solid ${BDR}` }}>
                <span className="inline-flex items-center justify-center text-sm font-bold rounded-full px-2 py-0.5"
                      style={{ background: '#94A3B8', color: '#FFFFFF', minWidth: 24 }}>
                  {revision.sinCambios.length}
                </span>
                <span className="text-xs font-semibold uppercase tracking-wider" style={{ color: MUTED }}>
                  Sin cambios
                </span>
              </div>

              {/* Con error — rojo sutil */}
              {revision.invalid.length > 0 && (
                <div className="inline-flex items-center gap-2 rounded-full pl-3 pr-4 py-1.5"
                     style={{ background: '#FEE2E2', border: '1px solid #FCA5A5' }}>
                  <span className="inline-flex items-center justify-center text-sm font-bold rounded-full px-2 py-0.5"
                        style={{ background: '#DC2626', color: '#FFFFFF', minWidth: 24 }}>
                    {revision.invalid.length}
                  </span>
                  <span className="text-xs font-semibold uppercase tracking-wider" style={{ color: '#7F1D1D' }}>
                    Con error
                  </span>
                </div>
              )}
            </div>
          </div>

          {/* Aviso de los registros a revisar */}
          <div className="flex items-start gap-3 rounded-lg px-4 py-3"
               style={{ background: BLUE + '0D', border: `1px solid ${BLUE}33` }}>
            <AlertTriangle size={18} style={{ color: BLUE, flexShrink: 0, marginTop: 1 }} />
            <div className="flex flex-col gap-1">
              <p className="text-sm font-semibold" style={{ color: TICK }}>
                {revision.conCambios.length} manifiesto{revision.conCambios.length !== 1 ? 's' : ''} requiere{revision.conCambios.length !== 1 ? 'n' : ''} revisión
              </p>
              <p className="text-xs leading-relaxed" style={{ color: MUTED }}>
                Estos registros ya existen en el sistema con información distinta a la del Excel.
                Seleccione cuáles desea actualizar con los nuevos valores. Los no seleccionados conservarán
                su información actual.
              </p>
            </div>
          </div>

          {revision.conCambios.length > 0 && (
            <div className="flex gap-4">
              <button type="button"
                onClick={() => setSeleccionados(new Set(revision.conCambios.map(p => p.payload.p_manifiesto)))}
                className="text-xs font-semibold hover:opacity-70" style={{ color: BLUE }}>
                Seleccionar todos
              </button>
              <button type="button"
                onClick={() => setSeleccionados(new Set())}
                className="text-xs font-semibold hover:opacity-70" style={{ color: MUTED }}>
                Deseleccionar todos
              </button>
            </div>
          )}

          {/* Lista de manifiestos con cambios */}
          <div className="flex flex-col gap-2">
            {revision.conCambios.map((p) => {
              const num      = p.payload.p_manifiesto
              const checked  = seleccionados.has(num)
              const expanded = expandidos.has(num)
              return (
                <div key={num} className="rounded-lg overflow-hidden" style={{ border: `1px solid ${checked ? BLUE + '55' : BDR}` }}>
                  <div className="flex items-center gap-3 px-3 py-2.5"
                    style={{ background: checked ? BLUE + '08' : '#F8FAFC' }}>
                    <input type="checkbox" checked={checked} onChange={() => toggleSeleccionado(num)}
                      style={{ accentColor: BLUE, width: 14, height: 14, flexShrink: 0 }} />
                    <span className="text-sm font-bold font-mono" style={{ color: GOLD }}>{num}</span>
                    <span className="text-xs" style={{ color: MUTED }}>
                      {p.diffs.length} campo{p.diffs.length !== 1 ? 's' : ''} cambiado{p.diffs.length !== 1 ? 's' : ''}
                    </span>
                    <div className="flex-1" />
                    <button type="button" onClick={() => toggleExpandido(num)}
                      className="flex items-center gap-1 text-xs hover:opacity-70" style={{ color: MUTED }}>
                      {expanded ? <ChevronUp size={12} /> : <ChevronDown size={12} />}
                      {expanded ? 'Ocultar' : 'Ver cambios'}
                    </button>
                  </div>
                  {expanded && (
                    <div className="border-t" style={{ borderColor: BDR }}>
                      <div className="grid grid-cols-[140px_1fr_1fr] px-3 py-1.5 text-[10px] font-bold uppercase tracking-wider"
                        style={{ background: '#F1F5F9', color: MUTED }}>
                        <span>Campo</span><span>Valor actual (DB)</span><span>Valor nuevo (Excel)</span>
                      </div>
                      {p.diffs.map(({ field, valDB, valNew }) => (
                        <div key={field} className="grid grid-cols-[140px_1fr_1fr] px-3 py-1.5 text-xs border-t"
                          style={{ borderColor: BDR }}>
                          <span className="font-medium" style={{ color: MUTED }}>{FIELD_LABELS[field]}</span>
                          <span style={{ color: RED }}>{valDB ?? '—'}</span>
                          <span style={{ color: '#166534' }}>{valNew ?? '—'}</span>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              )
            })}
          </div>

          {revision.invalid.length > 0 && (
            <ErrorRows invalid={revision.invalid}
              title={`Filas con error — no se importarán (${revision.invalid.length})`} />
          )}

          <div className="flex gap-3 pt-1">
            <button type="button" onClick={confirmarRevision}
              disabled={totalImportar === 0}
              className="flex items-center gap-2 px-5 py-2 rounded-xl text-sm font-semibold transition-opacity hover:opacity-90 disabled:opacity-40 disabled:cursor-not-allowed"
              style={{ background: BTN_GRAD, color: '#fff', boxShadow: BTN_SHADOW }}>
              <Upload size={14} /> Importar ({totalImportar} registro{totalImportar !== 1 ? 's' : ''})
            </button>
            <button type="button" onClick={cancelar}
              className="px-4 py-2 rounded-xl text-sm font-semibold transition-opacity hover:opacity-80"
              style={{ background: BDR, color: TICK }}>
              Cancelar
            </button>
          </div>
        </div>
      )}

      {/* Progreso durante la carga */}
      {busy && (
        <div className="flex flex-col gap-3 rounded-xl p-4" style={{ background: BG, border: `1px solid ${BDR}` }}>
          <p className="text-sm font-medium" style={{ color: BLUE }}>Importando registros... {progress}%</p>
          <div className="rounded-full overflow-hidden h-2" style={{ background: BDR }}>
            <div className="h-2 rounded-full transition-all duration-300" style={{ width: `${progress}%`, background: BLUE }} />
          </div>
        </div>
      )}

      {/* Resultado final */}
      {result && (
        <div className="flex flex-col gap-3 rounded-xl p-4" style={{ background: BG, border: `1px solid ${BDR}` }}>
          <div className="flex gap-4 flex-wrap">
            {result.ok > 0 && (
              <span className="flex items-center gap-1.5 text-sm font-semibold" style={{ color: '#166534' }}>
                <CheckCircle size={14} /> {result.ok} nuevos importados
              </span>
            )}
            {result.actualizados > 0 && (
              <span className="flex items-center gap-1.5 text-sm font-semibold" style={{ color: BLUE }}>
                <CheckCircle size={14} /> {result.actualizados} actualizados
              </span>
            )}
            {result.omitidos > 0 && (
              <span className="flex items-center gap-1.5 text-sm font-semibold" style={{ color: MUTED }}>
                <AlertTriangle size={14} /> {result.omitidos} omitidos
              </span>
            )}
            {result.errores.length > 0 && (
              <span className="flex items-center gap-1.5 text-sm font-semibold" style={{ color: RED }}>
                <AlertCircle size={14} /> {result.errores.length} errores
              </span>
            )}
          </div>
          {result.errores.length > 0 && (
            <div className="rounded-lg overflow-hidden" style={{ border: `1px solid #FECACA` }}>
              <div className="grid grid-cols-[60px_1fr] px-3 py-1.5 text-[10px] font-bold uppercase tracking-wider"
                style={{ background: '#FEF2F2', color: RED }}>
                <span>Fila</span><span>Error</span>
              </div>
              {result.errores.slice(0, 10).map((e, i) => (
                <div key={i} className="grid grid-cols-[60px_1fr] px-3 py-1.5 text-xs border-t"
                  style={{ borderColor: '#FECACA', color: TICK }}>
                  <span style={{ color: MUTED }}>{e.fila}</span>
                  <span>{e.msg}</span>
                </div>
              ))}
              {result.errores.length > 10 && (
                <p className="px-3 py-1.5 text-xs border-t" style={{ borderColor: '#FECACA', color: MUTED }}>
                  ...y {result.errores.length - 10} errores más
                </p>
              )}
            </div>
          )}
          <button type="button" onClick={cancelar}
            className="self-start text-xs hover:opacity-80" style={{ color: MUTED }}>
            Importar otro archivo
          </button>
        </div>
      )}
    </div>
  )
}

// ── Main Page ────────────────────────────────────────────────────────────────
export default function CargaPage({ target, clearTarget, user }) {
  const rol = user?.app_metadata?.role || ''
  const canEditDespacho      = ['digitador', 'gerencia'].includes(rol)
  // logistico hereda a digitador y tesoreria (per USUARIOS DRIVE: ambos tienen "CUMPLE,")
  const canEditLogistico     = ['logistico', 'digitador', 'tesoreria', 'gerencia'].includes(rol)
  // tesorería solo edita R-W del Drive en Cumplimiento; campos extra (novedad_conductor,
  // novedad_empresa, ajustes al flete, consignación) son exclusivos de logístico.
  const canEditCumplimientoExtra = ['logistico', 'digitador', 'gerencia'].includes(rol)
  // financiero solo puede editar estado_interno dentro de cumplimiento, nada más de esa tab
  const canEditEstadoInterno = canEditLogistico || ['financiero', 'administrativo'].includes(rol)
  const canEditTesoreria     = ['tesoreria', 'contadora', 'gerencia'].includes(rol)
  const canEditFinanciero    = ['financiero', 'contadora', 'gerencia'].includes(rol)
  const canUploadExcel       = ['digitador', 'gerencia'].includes(rol)

  const [query,  setQuery]  = useState('')
  const [view,   setView]   = useState('inicio')
  const [ficha,  setFicha]  = useState(null)
  const fichaFromExcel = !!ficha?.archivo_origen
  const [tab,    setTab]    = useState('despacho')
  const [formSeg,   setFS]  = useState(SEGUIMIENTO_INIT)
  const [formTes,   setFT]  = useState(TESORERIA_INIT)
  const [formFact,  setFF]  = useState(FACT_INIT)
  const [formEdit,  setFE]  = useState({})
  const [editMode,  setEditMode]    = useState(false)
  const [confirmDel, setConfirmDel]     = useState(false)
  const [deleteText, setDeleteText]     = useState('')
  const [busy,      setBusy]      = useState(false)
  const [msg,       setMsg]       = useState(null)
  const [manifiestosFE, setManifiestosFE] = useState(null)
  const [loadingFE, setLoadingFE] = useState(false)

  const { catalogos } = useCatalogos()
  const { search, update, remove, updateLogistico, updateEstadoInterno, updateTesoreria, updateFacturacion, getManifiestosPorFE } = useManifiesto()

  // ── Catalog options ─────────────────────────────────────────────────────────
  const optConductores  = catalogos.conductores.map(c => ({ id: c.nombre, label: c.nombre, sub: c.cedula }))
  const optClientes     = catalogos.clientes.map(c => ({ id: c.nombre, label: c.nombre }))
  const optLugares      = catalogos.lugares.map(l => ({ id: l.nombre, label: l.nombre }))
  const optResponsables = catalogos.responsables.map(r => ({ id: r.nombre, label: r.nombre }))
  const optVehiculos    = catalogos.vehiculos.map(v => ({ id: v.placa, label: v.placa }))
  const optRemolques    = catalogos.remolques.map(r => ({ id: r.placa, label: r.placa }))
  const optAgencias     = catalogos.agencias.map(a => ({ id: a.nombre, label: a.nombre }))
  const optPropietarios = catalogos.propietarios.map(p => ({ id: p.nombre, label: p.nombre }))
  const optFacturasElectronicas = catalogos.facturas_electronicas.map(f => ({ id: f.nombre, label: f.nombre }))
  const optFacturasNo = catalogos.facturas_no.map(f => ({ id: f.nombre, label: f.nombre }))

  const newText = (nombre) => ({ id: nombre, nombre, label: nombre, placa: nombre })

  // ── Load ficha ──────────────────────────────────────────────────────────────
  const userName = user?.user_metadata?.nombre || user?.app_metadata?.nombre || user?.email || ''

  const buildEditForm = (data) => ({
    fecha_despacho:       data.fecha_despacho       || '',
    conductor:            data.conductor            || '',
    cedula_conductor:     data.cedula_conductor     || '',
    celular:              data.celular              || '',
    placa:                data.placa                || '',
    placa_remolque:         data.placa_remolque         || '',
    propietario:          data.propietario          || '',
    cliente:              data.cliente              || '',
    origen:               data.origen               || '',
    destino:              data.destino              || '',
    agencia_despachadora: data.agencia_despachadora || '',
    nombre_responsable:   data.nombre_responsable   || '',
    valor_remesa:         data.valor_remesa         ?? '',
    flete_conductor:      data.flete_conductor      ?? '',
    anticipo:             data.anticipo             ?? '',
    remesas:              data.remesas              || '',
  })

  const loadFicha = (data) => {
    setFicha(data)
    setTab('despacho')
    setEditMode(false)
    setConfirmDel(false)
    setFE(buildEditForm(data))
    setFS({
      fecha_cumplido:              data.fecha_cumplido              || '',
      compromiso_pago:             data.compromiso_pago             || 'PAGO A 15 DIAS',
      novedades:                   data.novedades                   ?? '',
      estado_interno:              data.estado_interno              || '',
      responsable_estado_interno:  data.responsable_estado_interno  || '',
      novedad_conductor:           data.novedad_conductor           ?? '',
      novedad_empresa:             data.novedad_empresa             ?? '',
      ajuste_positivo_flete:       data.ajuste_positivo_flete       ?? '',
      ajuste_negativo_flete:       data.ajuste_negativo_flete       ?? '',
      consignacion_a_terceros:     data.consignacion_a_terceros     ?? '',
    })
    setFT({
      fecha_pago:         data.fecha_pago         || '',
      valor_pagado:       data.valor_pagado        ?? '',
      entidad_financiera: data.entidad_financiera  || '',
      responsable:        data.responsable         || '',
    })
    setFF({
      factura_no:          data.factura_no          || '',
      fecha_factura:       data.fecha_factura        || '',
      factura_electronica: data.factura_electronica  || '',
      mes_facturacion:     data.mes_facturacion      ?? '',
      valor_factura:       data.valor_factura        ?? '',
    })
  }

  const toast = (type, text) => setMsg({ type, text })

  const revertAll = () => {
    if (!ficha) return
    setFE(buildEditForm(ficha))
    setFS({
      fecha_cumplido:              ficha.fecha_cumplido              || '',
      compromiso_pago:             ficha.compromiso_pago             || 'PAGO A 15 DIAS',
      novedades:                   ficha.novedades                   ?? '',
      estado_interno:              ficha.estado_interno              || '',
      responsable_estado_interno:  ficha.responsable_estado_interno  || '',
      novedad_conductor:           ficha.novedad_conductor           ?? '',
      novedad_empresa:             ficha.novedad_empresa             ?? '',
      ajuste_positivo_flete:       ficha.ajuste_positivo_flete       ?? '',
      ajuste_negativo_flete:       ficha.ajuste_negativo_flete       ?? '',
      consignacion_a_terceros:     ficha.consignacion_a_terceros     ?? '',
    })
    setFT({
      fecha_pago:         ficha.fecha_pago         || '',
      valor_pagado:       ficha.valor_pagado        ?? '',
      entidad_financiera: ficha.entidad_financiera  || '',
      responsable:        ficha.responsable         || '',
    })
    setFF({
      factura_no:          ficha.factura_no          || '',
      fecha_factura:       ficha.fecha_factura        || '',
      factura_electronica: ficha.factura_electronica  || '',
      mes_facturacion:     ficha.mes_facturacion      ?? '',
      valor_factura:       ficha.valor_factura        ?? '',
    })
    toast('success', 'Campos restaurados a los valores guardados.')
  }

  useEffect(() => {
    if (!target) return
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setQuery(String(target))
    search(target).then(data => {
      if (data) { loadFicha(data); setView('ficha') }
      else toast('error', `Manifiesto ${target} no existe en la base de datos.`)
    }).catch(err => toast('error', `Error al cargar manifiesto ${target}: ${err.message ?? 'desconocido'}`))
    clearTarget?.()
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [target])

  useEffect(() => {
    const fe = formFact.factura_electronica?.trim()
    if (!fe) { setManifiestosFE(null); return } // eslint-disable-line react-hooks/set-state-in-effect
    setLoadingFE(true)
    getManifiestosPorFE(fe)
      .then(data => setManifiestosFE(data))
      .catch(() => setManifiestosFE([]))
      .finally(() => setLoadingFE(false))
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [formFact.factura_electronica])

  // ── Conductor select helpers ─────────────────────────────────────────────
  const fillConductor = (nombreCond, setter) => {
    const cond = catalogos.conductores.find(c => c.nombre === nombreCond)
    setter(p => ({
      ...p,
      conductor:        nombreCond,
      cedula_conductor: cond?.cedula  ?? p.cedula_conductor,
      celular:          cond?.celular ?? p.celular,
    }))
  }

  // ── Handlers ──────────────────────────────────────────────────────────────
  const handleSearch = async (e) => {
    e.preventDefault()
    const raw = query.trim()
    if (!raw) {
      toast('error', 'Ingresá un número de manifiesto para buscar.')
      return
    }
    const num = Number(raw)
    if (!Number.isInteger(num) || num <= 0) {
      toast('error', `"${raw}" no es un número de manifiesto válido. Usá solo dígitos.`)
      return
    }
    setBusy(true)
    try {
      const data = await search(num)
      if (data) {
        loadFicha(data)
        setView('ficha')
      } else {
        toast('error', `Manifiesto ${num} no existe en la base de datos. Verificá el número o cargalo desde Excel.`)
      }
    } catch (err) {
      toast('error', `No se pudo buscar el manifiesto ${num}: ${err.message ?? 'error desconocido'}`)
    } finally { setBusy(false) }
  }

  const handleUpdate = async (e) => {
    e.preventDefault()
    if (!formEdit.conductor || !formEdit.cliente ||
        !formEdit.origen    || !formEdit.destino || !formEdit.fecha_despacho) {
      toast('error', 'Completá los campos obligatorios (*)'); return
    }
    const cel = (formEdit.celular ?? '').replace(/[^0-9]/g, '')
    if (cel.length > 0 && cel.length !== 10) {
      toast('error', 'El celular debe tener exactamente 10 dígitos'); return
    }
    setBusy(true)
    try {
      await update(ficha.manifiesto, formEdit)
      toast('success', 'Manifiesto actualizado correctamente.')
      const data = await search(ficha.manifiesto)
      loadFicha(data)
    } catch (err) {
      toast('error', err.message ?? 'Error al actualizar')
    } finally { setBusy(false) }
  }

  const handleDelete = async () => {
    setBusy(true)
    try {
      const num = ficha.manifiesto
      await remove(num)
      toast('success', `Manifiesto ${num} eliminado.`)
      volver()
    } catch (err) {
      toast('error', err.message ?? 'Error al eliminar')
    } finally { setBusy(false) }
  }

  const handleSaveSeg = async (e) => {
    e.preventDefault()
    if (formSeg.fecha_cumplido && !ficha.factura_electronica) {
      toast('error', 'No se puede marcar cumplido sin factura electrónica. Completá la legalización primero.')
      setBusy(false); return
    }
    setBusy(true)
    try {
      await updateLogistico(ficha.manifiesto, { ...formSeg, responsable_estado_interno: userName })
      toast('success', 'Cumplimiento actualizado correctamente.')
      const data = await search(ficha.manifiesto)
      loadFicha(data)
    } catch (err) { toast('error', err.message ?? 'Error al guardar') }
    finally { setBusy(false) }
  }

  const handleSaveEstadoInterno = async (e) => {
    e.preventDefault()
    if (!formSeg.estado_interno) { toast('error', 'Seleccioná un estado interno.'); return }
    setBusy(true)
    try {
      // RPC dedicada: solo actualiza estado_interno + responsable.
      // financiero/administrativo NO tienen acceso a guardar_logistico.
      await updateEstadoInterno(ficha.manifiesto, {
        estado_interno:             formSeg.estado_interno,
        responsable_estado_interno: userName,
      })
      toast('success', 'Estado interno actualizado correctamente.')
      const data = await search(ficha.manifiesto)
      loadFicha(data)
    } catch (err) { toast('error', err.message ?? 'Error al guardar') }
    finally { setBusy(false) }
  }

  const handleSaveTes = async (e) => {
    e.preventDefault()
    setBusy(true)
    try {
      await updateTesoreria(ficha.manifiesto, { ...formTes, responsable: userName })
      toast('success', 'Tesorería actualizada correctamente.')
      const data = await search(ficha.manifiesto)
      loadFicha(data)
    } catch (err) { toast('error', err.message ?? 'Error al guardar') }
    finally { setBusy(false) }
  }

  const handleSaveFact = async (e) => {
    e.preventDefault()
    // Los tres campos son siempre obligatorios juntos
    const tieneFact  = !!formFact.factura_no.trim()
    const tieneFecha = !!formFact.fecha_factura
    const tieneValor = formFact.valor_factura !== '' && formFact.valor_factura != null
    if (!tieneFact || !tieneFecha || !tieneValor) {
      toast('error', 'N° factura, fecha y valor de factura son obligatorios.')
      return
    }
    setBusy(true)
    try {
      await updateFacturacion(ficha.manifiesto, formFact)
      toast('success', 'Facturación actualizada correctamente.')
      const data = await search(ficha.manifiesto)
      loadFicha(data)
      if (data?.factura_electronica) {
        getManifiestosPorFE(data.factura_electronica)
          .then(d => setManifiestosFE(d))
          .catch(() => {})
      }
    } catch (err) { toast('error', err.message ?? 'Error al guardar') }
    finally { setBusy(false) }
  }

  const volver = () => { setView('inicio'); setFicha(null); setQuery(''); setEditMode(false); setConfirmDel(false); setDeleteText('') }
  const fs = (key) => (val) => setFS(p => ({ ...p, [key]: val }))
  const ft = (key) => (val) => setFT(p => ({ ...p, [key]: val }))
  const ff = (key) => (val) => setFF(p => ({ ...p, [key]: val }))
  const fe = (key) => (val) => setFE(p => ({ ...p, [key]: val }))

  // ── Render ─────────────────────────────────────────────────────────────────
  return (
    <div className="flex flex-col gap-6 max-w-7xl mx-auto">

      {/* Search bar */}
      <form onSubmit={handleSearch} className="flex gap-3">
        <div className="relative flex-1">
          <Search size={18} className="absolute left-4 top-1/2 -translate-y-1/2 pointer-events-none" style={{ color: MUTED }} />
          <input value={query} onChange={e => setQuery(e.target.value)}
            placeholder="Buscar por número de manifiesto..."
            className={inputCls + ' pl-11 text-base'} style={{ borderColor: BDR, paddingTop: '12px', paddingBottom: '12px' }}
          />
        </div>
        <button type="submit" disabled={busy || !query.trim()}
          className="px-6 text-base rounded-lg font-semibold transition-opacity disabled:opacity-50"
          style={{ background: BTN_GRAD, color: '#FFFFFF', boxShadow: BTN_SHADOW, paddingTop: '12px', paddingBottom: '12px' }}>
          Buscar
        </button>
      </form>

      {/* ── INICIO ─────────────────────────────────────────────────────────── */}
      {view === 'inicio' && (
        <div className="flex flex-col gap-6">
          {/* Upload panel para digitadores */}
          {canUploadExcel && (
            <div className="flex flex-col gap-4">
              <div className="flex items-center gap-2 pl-1">
                <Upload size={16} color={BLUE} />
                <p className="text-sm font-bold uppercase tracking-widest" style={{ color: TICK }}>Importar desde Excel</p>
              </div>
              <ExcelUploadPanel onDone={() => {}} />
            </div>
          )}

          {/* Placeholder cuando no hay upload */}
          {!canUploadExcel && (
            <div className="flex flex-col items-center justify-center gap-3 py-24 text-center">
              <Search size={32} style={{ color: MUTED }} />
              <p className="text-sm font-medium" style={{ color: TICK }}>Buscá un manifiesto para ver o editar su información</p>
              <p className="text-xs" style={{ color: MUTED }}>Ingresá el número de manifiesto en la barra de búsqueda</p>
            </div>
          )}
        </div>
      )}

      {/* ── FICHA ──────────────────────────────────────────────────────────── */}
      {view === 'ficha' && ficha && (
        <div className="flex flex-col gap-4">
          {/* Header */}
          <div className="flex items-center gap-3 flex-wrap">
            <button type="button" onClick={volver}
              className="flex items-center gap-1.5 text-xs transition-opacity hover:opacity-70"
              style={{ color: MUTED }}>
              <ArrowLeft size={13} /> Volver
            </button>
            <div className="h-4 w-px" style={{ background: BDR }} />
            <p className="text-sm font-bold" style={{ color: TICK }}>Manifiesto {ficha.manifiesto}</p>
            <span className="text-xs px-2 py-0.5 rounded-full font-medium"
              style={{ background: GOLD + '18', color: '#78400A', border: `1px solid ${GOLD}55` }}>
              {ficha.mes} {ficha.año}
            </span>
            <div className="flex-1" />
            {!confirmDel && (
              <>
                {tab === 'despacho' && canEditDespacho && !fichaFromExcel && (
                  <button type="button" onClick={() => setEditMode(v => !v)}
                    className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-semibold transition-colors"
                    style={editMode
                      ? { background: '#F1F5F9', color: MUTED, border: `1px solid ${BDR}` }
                      : { background: BLUE + '15', color: BLUE, border: `1px solid ${BLUE}` }}>
                    {editMode ? <><X size={12} /> Cancelar</> : <><Pencil size={12} /> Editar</>}
                  </button>
                )}
                {rol === 'gerencia' && (
                  <button type="button" onClick={() => setConfirmDel(true)}
                    className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-semibold transition-colors"
                    style={{ background: '#FEF2F2', color: '#DC2626', border: '1px solid #FECACA' }}>
                    <Trash2 size={12} /> Eliminar
                  </button>
                )}
              </>
            )}
            {confirmDel && (
              <div className="flex items-center gap-2 px-3 py-2 rounded-lg flex-wrap"
                style={{ background: '#FEF2F2', border: '1px solid #FECACA' }}>
                <span className="text-xs font-medium" style={{ color: '#DC2626' }}>
                  Escribe <strong>{ficha.manifiesto}</strong> para confirmar la eliminación:
                </span>
                <input
                  autoFocus
                  type="number"
                  value={deleteText}
                  onChange={e => setDeleteText(e.target.value)}
                  placeholder={String(ficha.manifiesto)}
                  className="w-28 rounded-md border px-2 py-0.5 text-xs focus:outline-none focus:ring-1 focus:ring-red-400"
                  style={{ borderColor: '#FECACA', color: '#DC2626', background: '#fff' }}
                />
                <button type="button"
                  disabled={busy || String(deleteText).trim() !== String(ficha.manifiesto)}
                  onClick={handleDelete}
                  className="px-2.5 py-0.5 rounded font-bold text-xs transition-opacity disabled:opacity-30"
                  style={{ background: '#ef4444', color: '#fff' }}>
                  {busy ? '...' : 'Eliminar'}
                </button>
                <button type="button"
                  onClick={() => { setConfirmDel(false); setDeleteText('') }}
                  className="px-2 py-0.5 rounded text-xs font-semibold hover:opacity-70"
                  style={{ color: MUTED }}>
                  Cancelar
                </button>
              </div>
            )}
          </div>

          {/* Resumen rápido */}
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
            {[
              { l: 'Conductor', v: ficha.conductor ?? '—' },
              { l: 'Ruta',      v: ficha.origen && ficha.destino ? `${ficha.origen} → ${ficha.destino}` : '—' },
              { l: 'Cliente',   v: ficha.cliente ?? '—' },
              { l: 'Agencia Despachadora', v: ficha.agencia_despachadora ?? '—' },
            ].map(({ l, v }) => (
              <div key={l} className="rounded-lg px-4 py-3" style={{ background: BG, border: `1px solid ${BDR}` }}>
                <p className="text-[10px] font-bold uppercase tracking-wider mb-1" style={{ color: MUTED }}>{l}</p>
                <p className="text-sm font-semibold truncate" style={{ color: TICK }}>{v}</p>
              </div>
            ))}
          </div>

          {/* Tabs */}
          <div className="flex gap-1 rounded-lg p-1" style={{ background: BG, border: `1px solid ${BDR}`, width: 'fit-content' }}>
            {[
              { id: 'despacho',    label: 'Despacho' },
              { id: 'cumplimiento', label: 'Cumplimiento' },
              { id: 'tesoreria',   label: 'Tesorería' },
              { id: 'facturacion', label: 'Facturación y Legalización' },
            ].map(t => (
              <button key={t.id} onClick={() => { setTab(t.id); setEditMode(false) }}
                className="px-4 py-1.5 rounded-md text-xs font-semibold transition-all"
                style={tab === t.id ? { background: BTN_GRAD, color: '#FFFFFF', boxShadow: BTN_SHADOW } : { color: MUTED, background: 'transparent' }}>
                {t.label}
              </button>
            ))}
          </div>

          {/* Tab: Despacho — readonly */}
          {tab === 'despacho' && !editMode && (() => {
            return (
              <div className="flex flex-col gap-4">
                {fichaFromExcel && (
                  <div className="flex items-center gap-2 rounded-lg px-3 py-2.5 text-xs"
                    style={{ background: '#FFF7ED', border: '1px solid #FED7AA', color: '#92400E' }}>
                    <Lock size={12} style={{ flexShrink: 0 }} />
                    Datos cargados desde Excel ({ficha.archivo_origen}). La única forma de modificarlos es mediante el panel de importación.
                  </div>
                )}
                <div>
                  <p className="text-[10px] font-bold uppercase tracking-widest mb-2" style={{ color: MUTED }}>Despacho</p>
                  <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-2">
                    {[
                      { l: 'Manifiesto',      v: ficha.manifiesto },
                      { l: 'Fecha despacho',  v: ficha.fecha_despacho },
                      { l: 'Mes / Año',       v: `${ficha.mes} ${ficha.año}` },
                      { l: 'Conductor',       v: ficha.conductor },
                      { l: 'Cédula',          v: ficha.cedula_conductor },
                      { l: 'Celular',         v: ficha.celular },
                      { l: 'Placa',           v: ficha.placa },
                      { l: 'Remolque',   v: ficha.placa_remolque },
                      { l: 'Propietario',     v: ficha.propietario },
                      { l: 'Cliente',         v: ficha.cliente },
                      { l: 'Origen',          v: ficha.origen },
                      { l: 'Destino',         v: ficha.destino },
                      { l: 'Agencia Despachadora', v: ficha.agencia_despachadora },
                      { l: 'Responsable',     v: ficha.nombre_responsable },
                      { l: 'Valor remesa',        v: ficha.valor_remesa         != null ? `$${Number(ficha.valor_remesa).toLocaleString('es-CO')}` : null },
                      { l: 'Flete conductor',     v: ficha.flete_conductor      != null ? `$${Number(ficha.flete_conductor).toLocaleString('es-CO')}` : null },
                      { l: 'Saldo',v: ficha.saldo != null ? `$${Number(ficha.saldo).toLocaleString('es-CO')}` : null },
                      { l: 'Anticipo',            v: ficha.anticipo             != null ? `$${Number(ficha.anticipo).toLocaleString('es-CO')}` : null },
                      { l: 'Remesas',         v: ficha.remesas || null, col: 2 },
                    ].map(({ l, v, col }) => (
                      <div key={l} className="rounded-lg px-3 py-2.5"
                        style={{ background: BG, border: `1px solid ${BDR}`, gridColumn: col ? `span ${col}` : undefined }}>
                        <p className="text-[10px] font-bold uppercase tracking-wider mb-0.5" style={{ color: MUTED }}>{l}</p>
                        <p className="text-sm" style={{ color: TICK }}>{v ?? '—'}</p>
                      </div>
                    ))}
                  </div>
                </div>
                {[
                  {
                    title: 'Cumplimiento',
                    items: [
                      { l: 'Compromiso pago',   v: ficha.compromiso_pago },
                      { l: 'Fecha cumplido',    v: ficha.fecha_cumplido },
                      { l: 'Estado interno',    v: ficha.estado_interno },
                      { l: 'Novedades',         v: ficha.novedades, col: 2 },
                    ],
                  },
                  {
                    title: 'Tesorería',
                    items: [
                      { l: 'Fecha pago',        v: ficha.fecha_pago },
                      { l: 'Valor pagado',      v: ficha.valor_pagado != null ? `$${Number(ficha.valor_pagado).toLocaleString('es-CO')}` : null },
                      { l: 'Entidad financiera',v: ficha.entidad_financiera },
                    ],
                  },
                  {
                    title: 'Facturación',
                    items: [
                      { l: 'N° Factura',                  v: ficha.factura_no },
                      { l: 'Fecha de emisión de factura', v: ficha.fecha_factura },
                    ],
                  },
                ].map(({ title, items }) => (
                  <div key={title}>
                    <p className="text-[10px] font-bold uppercase tracking-widest mb-2" style={{ color: MUTED }}>{title}</p>
                    <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-2">
                      {items.map(({ l, v, col }) => (
                        <div key={l} className="rounded-lg px-3 py-2.5"
                          style={{ background: BG, border: `1px solid ${BDR}`, gridColumn: col ? `span ${col}` : undefined }}>
                          <p className="text-[10px] font-bold uppercase tracking-wider mb-0.5" style={{ color: MUTED }}>{l}</p>
                          <p className="text-sm font-semibold" style={{ color: v ? TICK : MUTED }}>{v ?? '—'}</p>
                        </div>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            )
          })()}

          {/* Tab: Despacho — edit mode */}
          {tab === 'despacho' && editMode && (
            <form onSubmit={handleUpdate} className="flex flex-col gap-4">
              <SectionCard icon={FileText} title="Identificación" cols={3}>
                <Input label="N° Manifiesto" value={ficha.manifiesto} readOnly
                  style={{ borderColor: BDR, opacity: 0.5, cursor: 'not-allowed' }} />
                <DateInput label="Fecha despacho *"
                  value={formEdit.fecha_despacho} onChange={e => fe('fecha_despacho')(e.target.value)} />
                <Autocomplete label="Agencia Despachadora" displayValue={formEdit.agencia_despachadora}
                  placeholder="Agencia despachadora" options={optAgencias} onCreate={newText}
                  onSelect={o => setFE(p => ({ ...p, agencia_despachadora: o.label }))} />
              </SectionCard>
              <SectionCard icon={User} title="Personal" cols={3}>
                <Autocomplete label="Conductor *" displayValue={formEdit.conductor}
                  placeholder="Nombre del conductor" options={optConductores} onCreate={newText}
                  onSelect={o => fillConductor(o.label, setFE)} />
                <Input label="Cédula conductor" placeholder="Número de cédula"
                  value={formEdit.cedula_conductor ?? ''} onChange={e => fe('cedula_conductor')(e.target.value)} />
                <PhoneInput label="Celular conductor"
                  value={formEdit.celular ?? ''} onChange={e => fe('celular')(e.target.value)} />
                <Autocomplete label="Cliente *" displayValue={formEdit.cliente}
                  placeholder="Nombre del cliente" options={optClientes} onCreate={newText}
                  onSelect={o => setFE(p => ({ ...p, cliente: o.label }))} />
                <Autocomplete label="Responsable despacho" displayValue={formEdit.nombre_responsable}
                  placeholder="Responsable" options={optResponsables} onCreate={newText}
                  onSelect={o => setFE(p => ({ ...p, nombre_responsable: o.label }))} />
              </SectionCard>
              <SectionCard icon={MapPin} title="Ruta" cols={3}>
                <Autocomplete label="Origen *" displayValue={formEdit.origen}
                  placeholder="Ciudad de origen" options={optLugares} onCreate={newText}
                  onSelect={o => setFE(p => ({ ...p, origen: o.label }))} />
                <Autocomplete label="Destino *" displayValue={formEdit.destino}
                  placeholder="Ciudad de destino" options={optLugares} onCreate={newText}
                  onSelect={o => setFE(p => ({ ...p, destino: o.label }))} />
                <Autocomplete label="Placa vehículo" displayValue={formEdit.placa}
                  placeholder="Placa del vehículo" options={optVehiculos} onCreate={newText}
                  onSelect={o => setFE(p => ({ ...p, placa: o.label }))} />
                <Autocomplete label="Placa remolque" displayValue={formEdit.placa_remolque}
                  placeholder="Placa del remolque" options={optRemolques} onCreate={newText}
                  onSelect={o => setFE(p => ({ ...p, placa_remolque: o.label }))} />
                <Autocomplete label="Propietario vehículo" displayValue={formEdit.propietario ?? ''}
                  placeholder="Nombre del propietario" options={optPropietarios} onCreate={newText}
                  onSelect={o => setFE(p => ({ ...p, propietario: o.label }))} />
              </SectionCard>
              <SectionCard icon={DollarSign} title="Financiero" cols={3}>
                <MoneyInput label="Valor remesa"
                  value={formEdit.valor_remesa} onChange={e => fe('valor_remesa')(e.target.value)} />
                <MoneyInput label="Flete conductor"
                  value={formEdit.flete_conductor} onChange={e => fe('flete_conductor')(e.target.value)} />
                <MoneyInput label="Anticipo"
                  value={formEdit.anticipo} onChange={e => fe('anticipo')(e.target.value)} />
              </SectionCard>
              <SectionCard icon={ClipboardList} title="Remesas" cols={1}>
                <Field label="Códigos de remesa (separados por ;)" col={1}>
                  <input className={inputCls} style={{ borderColor: BDR }}
                    placeholder="27854; 27855; 27856"
                    value={formEdit.remesas} onChange={e => fe('remesas')(e.target.value)} />
                </Field>
              </SectionCard>
              <div className="flex justify-end gap-2">
                <button type="button" onClick={revertAll}
                  className="flex items-center gap-1.5 px-4 py-2 rounded-lg text-sm font-semibold transition-opacity hover:opacity-80"
                  style={{ background: GOLD + '18', color: '#78400A', border: `1px solid ${GOLD}44` }}>
                  <RotateCcw size={13} /> Restablecer
                </button>
                <button type="button" onClick={() => setEditMode(false)}
                  className="px-4 py-2 rounded-lg text-sm font-semibold"
                  style={{ background: '#F1F5F9', color: MUTED, border: `1px solid ${BDR}` }}>
                  Cancelar
                </button>
                <button type="submit" disabled={busy}
                  className="flex items-center gap-2 px-6 py-2.5 rounded-lg text-sm font-semibold disabled:opacity-50"
                  style={{ background: BTN_GRAD, color: '#FFFFFF', boxShadow: BTN_SHADOW }}>
                  <Save size={14} /> {busy ? 'Guardando...' : 'Guardar cambios'}
                </button>
              </div>
            </form>
          )}

          {/* Tab: Cumplimiento — lectura pura para roles sin ningún permiso sobre esta tab */}
          {tab === 'cumplimiento' && !canEditEstadoInterno && (
            <div className="flex flex-col gap-4">
              <div>
                <p className="text-[10px] font-bold uppercase tracking-widest mb-2" style={{ color: MUTED }}>Cumplimiento operativo</p>
                <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-2">
                  {[
                    { l: 'Fecha cumplido',          v: ficha.fecha_cumplido },
                    { l: 'Compromiso pago',          v: ficha.compromiso_pago },
                    { l: 'Estado interno',           v: ficha.estado_interno },
                    { l: 'Responsable estado int.',  v: ficha.responsable_estado_interno },
                    { l: 'Novedades',                v: ficha.novedades, col: 4 },
                    { l: 'Novedad del conductor',    v: ficha.novedad_conductor, col: 2 },
                    { l: 'Novedad de la empresa',    v: ficha.novedad_empresa, col: 2 },
                    { l: 'Reajuste',                 v: ficha.ajuste_positivo_flete != null ? `$ ${Number(ficha.ajuste_positivo_flete).toLocaleString('es-CO')}` : null },
                    { l: 'Descuento',                v: ficha.ajuste_negativo_flete != null ? `$ ${Number(ficha.ajuste_negativo_flete).toLocaleString('es-CO')}` : null },
                    { l: 'Consignación a terceros',  v: ficha.consignacion_a_terceros != null ? `$ ${Number(ficha.consignacion_a_terceros).toLocaleString('es-CO')}` : null },
                  ].map(({ l, v, col }) => (
                    <div key={l} className="rounded-lg px-3 py-2.5"
                      style={{ background: BG, border: `1px solid ${BDR}`, gridColumn: col ? `span ${col}` : undefined }}>
                      <p className="text-[10px] font-bold uppercase tracking-wider mb-0.5" style={{ color: MUTED }}>{l}</p>
                      <p className="text-sm" style={{ color: v ? TICK : MUTED }}>{v ?? '—'}</p>
                    </div>
                  ))}
                </div>
              </div>
              <p className="text-xs" style={{ color: MUTED }}>Solo el equipo operativo puede editar esta sección.</p>
            </div>
          )}

          {/* Tab: Cumplimiento — financiero: solo estado_interno editable, resto readonly */}
          {tab === 'cumplimiento' && canEditEstadoInterno && !canEditLogistico && (
            <div className="flex flex-col gap-4">
              <div>
                <p className="text-[10px] font-bold uppercase tracking-widest mb-2" style={{ color: MUTED }}>Cumplimiento operativo</p>
                <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-2">
                  {[
                    { l: 'Fecha cumplido',         v: ficha.fecha_cumplido },
                    { l: 'Compromiso pago',         v: ficha.compromiso_pago },
                    { l: 'Responsable estado int.', v: ficha.responsable_estado_interno },
                    { l: 'Novedades',               v: ficha.novedades, col: 4 },
                    { l: 'Novedad del conductor',   v: ficha.novedad_conductor, col: 2 },
                    { l: 'Novedad de la empresa',   v: ficha.novedad_empresa, col: 2 },
                    { l: 'Reajuste',                v: ficha.ajuste_positivo_flete != null ? `$ ${Number(ficha.ajuste_positivo_flete).toLocaleString('es-CO')}` : null },
                    { l: 'Descuento',               v: ficha.ajuste_negativo_flete != null ? `$ ${Number(ficha.ajuste_negativo_flete).toLocaleString('es-CO')}` : null },
                    { l: 'Consignación a terceros', v: ficha.consignacion_a_terceros != null ? `$ ${Number(ficha.consignacion_a_terceros).toLocaleString('es-CO')}` : null },
                  ].map(({ l, v, col }) => (
                    <div key={l} className="rounded-lg px-3 py-2.5"
                      style={{ background: BG, border: `1px solid ${BDR}`, gridColumn: col ? `span ${col}` : undefined }}>
                      <p className="text-[10px] font-bold uppercase tracking-wider mb-0.5" style={{ color: MUTED }}>{l}</p>
                      <p className="text-sm" style={{ color: v ? TICK : MUTED }}>{v ?? '—'}</p>
                    </div>
                  ))}
                </div>
              </div>
              <form onSubmit={handleSaveEstadoInterno} className="flex flex-col gap-4">
                <SectionCard icon={ClipboardList} title="Estado interno" cols={2}>
                  <Select label="Estado interno" value={formSeg.estado_interno}
                    onChange={fs('estado_interno')} options={ESTADO_INTERNO_OPTS} />
                  <Field label="Responsable">
                    <div className="flex items-center justify-between h-9 px-3 text-sm rounded-md border"
                      style={{ borderColor: BDR, background: '#F8FAFC', color: TICK }}>
                      <span>{userName}</span>
                      <Lock size={11} style={{ color: MUTED }} />
                    </div>
                  </Field>
                </SectionCard>
                <div className="flex justify-end gap-2">
                  <button type="button" onClick={revertAll}
                    className="flex items-center gap-1.5 px-4 py-2 rounded-lg text-sm font-semibold transition-opacity hover:opacity-80"
                    style={{ background: GOLD + '18', color: '#78400A', border: `1px solid ${GOLD}44` }}>
                    <RotateCcw size={13} /> Restablecer
                  </button>
                  <button type="submit" disabled={busy}
                    className="flex items-center gap-2 px-6 py-2.5 rounded-lg text-sm font-semibold disabled:opacity-50"
                    style={{ background: BTN_GRAD, color: '#FFFFFF', boxShadow: BTN_SHADOW }}>
                    <Save size={14} /> {busy ? 'Guardando...' : 'Guardar estado interno'}
                  </button>
                </div>
              </form>
            </div>
          )}

          {/* Tab: Cumplimiento — edición completa para logistico/digitador/tesoreria/gerencia */}
          {tab === 'cumplimiento' && canEditLogistico && (
            <form onSubmit={handleSaveSeg} className="flex flex-col gap-4">
              <SectionCard icon={ClipboardList} title="Cumplimiento operativo" cols={3}>
                <div className="relative">
                  <DateInput label="Fecha cumplido"
                    value={formSeg.fecha_cumplido} onChange={e => fs('fecha_cumplido')(e.target.value)}
                    disabled={!ficha.factura_electronica} />
                  {!ficha.factura_electronica && (
                    <span className="absolute -bottom-5 left-0 text-xs whitespace-nowrap"
                      style={{ color: '#D97706' }}>
                      Requiere factura electrónica
                    </span>
                  )}
                </div>
                <Select label="Compromiso de pago" value={formSeg.compromiso_pago}
                  onChange={fs('compromiso_pago')} options={COMPROMISO_PAGO_OPTS} />
                <Select label="Estado interno" value={formSeg.estado_interno}
                  onChange={fs('estado_interno')} options={ESTADO_INTERNO_OPTS} />
                <Field label="Responsable estado interno">
                  <div className="flex items-center justify-between h-9 px-3 text-sm rounded-md border"
                    style={{ borderColor: BDR, background: '#F8FAFC', color: TICK }}>
                    <span>{userName}</span>
                    <Lock size={11} style={{ color: MUTED }} />
                  </div>
                </Field>
              </SectionCard>
              <SectionCard icon={ClipboardList} title="Novedades" cols={1}>
                <Field label="Novedades generales" col={1}>
                  <textarea rows={3} className={inputCls} style={{ borderColor: BDR, resize: 'vertical' }}
                    placeholder="Observaciones o novedades del viaje..."
                    value={formSeg.novedades} onChange={e => fs('novedades')(e.target.value)} />
                </Field>
              </SectionCard>
              {canEditCumplimientoExtra ? (
                <SectionCard icon={ClipboardList} title="Ajuste al flete" cols={2}>
                  <Field label="Novedad del conductor" col={2}>
                    <textarea rows={2} className={inputCls} style={{ borderColor: BDR, resize: 'vertical' }}
                      placeholder="Ej. demora en descargue, costos adicionales reconocidos al conductor..."
                      value={formSeg.novedad_conductor} onChange={e => fs('novedad_conductor')(e.target.value)} />
                  </Field>
                  <Field label="Novedad de la empresa" col={2}>
                    <textarea rows={2} className={inputCls} style={{ borderColor: BDR, resize: 'vertical' }}
                      placeholder="Ej. daño de mercancía, incumplimiento del conductor..."
                      value={formSeg.novedad_empresa} onChange={e => fs('novedad_empresa')(e.target.value)} />
                  </Field>
                  <MoneyInput label="Reajuste"
                    value={formSeg.ajuste_positivo_flete}
                    onChange={e => fs('ajuste_positivo_flete')(e.target.value)} />
                  <MoneyInput label="Descuento"
                    value={formSeg.ajuste_negativo_flete}
                    onChange={e => fs('ajuste_negativo_flete')(e.target.value)} />
                  <MoneyInput label="Consignación a terceros"
                    value={formSeg.consignacion_a_terceros}
                    onChange={e => fs('consignacion_a_terceros')(e.target.value)} />
                </SectionCard>
              ) : (
                <SectionCard icon={ClipboardList} title="Ajuste al flete" cols={2}>
                  {[
                    { l: 'Novedad del conductor',   v: ficha.novedad_conductor,   col: 2 },
                    { l: 'Novedad de la empresa',   v: ficha.novedad_empresa,     col: 2 },
                    { l: 'Reajuste',                v: ficha.ajuste_positivo_flete   != null ? `$ ${Number(ficha.ajuste_positivo_flete).toLocaleString('es-CO')}` : null },
                    { l: 'Descuento',               v: ficha.ajuste_negativo_flete   != null ? `$ ${Number(ficha.ajuste_negativo_flete).toLocaleString('es-CO')}` : null },
                    { l: 'Consignación a terceros', v: ficha.consignacion_a_terceros != null ? `$ ${Number(ficha.consignacion_a_terceros).toLocaleString('es-CO')}` : null },
                  ].map(({ l, v, col }) => (
                    <div key={l} className="rounded-lg px-3 py-2.5"
                      style={{ background: BG, border: `1px solid ${BDR}`, gridColumn: col ? `span ${col}` : undefined }}>
                      <p className="text-[10px] font-bold uppercase tracking-wider mb-0.5" style={{ color: MUTED }}>{l}</p>
                      <p className="text-sm" style={{ color: v ? TICK : MUTED }}>{v ?? '—'}</p>
                    </div>
                  ))}
                </SectionCard>
              )}
              <div className="flex justify-end gap-2">
                <button type="button" onClick={revertAll}
                  className="flex items-center gap-1.5 px-4 py-2 rounded-lg text-sm font-semibold transition-opacity hover:opacity-80"
                  style={{ background: GOLD + '18', color: '#78400A', border: `1px solid ${GOLD}44` }}>
                  <RotateCcw size={13} /> Restablecer
                </button>
                <button type="submit" disabled={busy}
                  className="flex items-center gap-2 px-6 py-2.5 rounded-lg text-sm font-semibold disabled:opacity-50"
                  style={{ background: BTN_GRAD, color: '#FFFFFF', boxShadow: BTN_SHADOW }}>
                  <Save size={14} /> {busy ? 'Guardando...' : 'Guardar cumplimiento'}
                </button>
              </div>
            </form>
          )}

          {/* Tab: Tesorería — lectura para roles sin permiso */}
          {tab === 'tesoreria' && !canEditTesoreria && (
            <div className="flex flex-col gap-4">
              <div>
                <p className="text-[10px] font-bold uppercase tracking-widest mb-2" style={{ color: MUTED }}>Pago conductor</p>
                <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-2">
                  {[
                    { l: 'Fecha pago',         v: ficha.fecha_pago },
                    { l: 'Valor pagado',        v: ficha.valor_pagado != null ? `$${Number(ficha.valor_pagado).toLocaleString('es-CO')}` : null },
                    { l: 'Entidad financiera',  v: ficha.entidad_financiera },
                    { l: 'Responsable pago',    v: ficha.responsable },
                  ].map(({ l, v }) => (
                    <div key={l} className="rounded-lg px-3 py-2.5"
                      style={{ background: BG, border: `1px solid ${BDR}` }}>
                      <p className="text-[10px] font-bold uppercase tracking-wider mb-0.5" style={{ color: MUTED }}>{l}</p>
                      <p className="text-sm" style={{ color: v ? TICK : MUTED }}>{v ?? '—'}</p>
                    </div>
                  ))}
                </div>
              </div>
              <p className="text-xs" style={{ color: MUTED }}>Solo tesorería puede editar esta sección.</p>
            </div>
          )}

          {/* Tab: Tesorería — edición para tesoreria/admin */}
          {tab === 'tesoreria' && canEditTesoreria && (
            <form onSubmit={handleSaveTes} className="flex flex-col gap-4">
              <SectionCard icon={DollarSign} title="Pago conductor" cols={3}>
                <DateInput label="Fecha pago"
                  value={formTes.fecha_pago} onChange={e => ft('fecha_pago')(e.target.value)} />
                <MoneyInput label="Valor pagado"
                  value={formTes.valor_pagado} onChange={e => ft('valor_pagado')(e.target.value)} />
                <Select label="Entidad financiera" value={formTes.entidad_financiera}
                  onChange={ft('entidad_financiera')} options={ENTIDAD_FIN_OPTS} />
                <Field label="Responsable pago">
                  <div className="flex items-center justify-between h-9 px-3 text-sm rounded-md border"
                    style={{ borderColor: BDR, background: '#F8FAFC', color: TICK }}>
                    <span>{userName}</span>
                    <Lock size={11} style={{ color: MUTED }} />
                  </div>
                </Field>
              </SectionCard>
              <div className="flex justify-end gap-2">
                <button type="button" onClick={revertAll}
                  className="flex items-center gap-1.5 px-4 py-2 rounded-lg text-sm font-semibold transition-opacity hover:opacity-80"
                  style={{ background: GOLD + '18', color: '#78400A', border: `1px solid ${GOLD}44` }}>
                  <RotateCcw size={13} /> Restablecer
                </button>
                <button type="submit" disabled={busy}
                  className="flex items-center gap-2 px-6 py-2.5 rounded-lg text-sm font-semibold disabled:opacity-50"
                  style={{ background: BTN_GRAD, color: '#FFFFFF', boxShadow: BTN_SHADOW }}>
                  <Save size={14} /> {busy ? 'Guardando...' : 'Guardar tesorería'}
                </button>
              </div>
            </form>
          )}

          {/* Tab: Facturación — lectura para roles sin permiso */}
          {tab === 'facturacion' && !canEditFinanciero && (
            <div className="flex flex-col gap-4">
              <div>
                <p className="text-[10px] font-bold uppercase tracking-widest mb-2" style={{ color: MUTED }}>Facturación</p>
                <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-2">
                  {[
                    { l: 'N° Factura',                    v: ficha.factura_no },
                    { l: 'Fecha de emisión de factura',   v: ficha.fecha_factura },
                    { l: 'Legalización FE / DS',          v: ficha.factura_electronica, col: 2 },
                  ].map(({ l, v, col }) => (
                    <div key={l} className="rounded-lg px-3 py-2.5"
                      style={{ background: BG, border: `1px solid ${BDR}`, gridColumn: col ? `span ${col}` : undefined }}>
                      <p className="text-[10px] font-bold uppercase tracking-wider mb-0.5" style={{ color: MUTED }}>{l}</p>
                      <p className="text-sm" style={{ color: v ? TICK : MUTED }}>{v ?? '—'}</p>
                    </div>
                  ))}
                </div>
              </div>

              {ficha.factura_electronica && manifiestosFE && manifiestosFE.length > 0 && (
                <PanelManifiestosFE
                  manifiestos={manifiestosFE}
                  manifiestoActual={ficha.manifiesto}
                  loading={loadingFE}
                />
              )}

              <p className="text-xs" style={{ color: MUTED }}>Solo el equipo financiero puede editar esta sección.</p>
            </div>
          )}

          {/* Tab: Facturación — edición para financiero/admin */}
          {tab === 'facturacion' && canEditFinanciero && (
            <form onSubmit={handleSaveFact} className="flex flex-col gap-4">
              <SectionCard icon={FileText} title="Facturación" cols={2}>
                <Autocomplete label="N° Factura" displayValue={formFact.factura_no}
                  placeholder="FE-0001" options={optFacturasNo}
                  onSelect={o => setFF(p => ({ ...p, factura_no: o.label }))} />
                <DateInput label="Fecha de emisión de factura"
                  value={formFact.fecha_factura}
                  onChange={e => {
                    const v = e.target.value
                    const mes = v ? new Date(v + 'T12:00:00').getMonth() + 1 : ''
                    setFF(p => ({ ...p, fecha_factura: v, mes_facturacion: mes !== '' ? mes : '' }))
                  }} />
                <MoneyInput label="Valor de la factura"
                  value={formFact.valor_factura}
                  onChange={e => ff('valor_factura')(e.target.value)} />
              </SectionCard>
              <SectionCard icon={ClipboardList} title="Legalización FE / DS" cols={1}>
                <Autocomplete label="N° Legalización / Propietario vehículo (prefijo: FE, DS, FWP...)"
                  displayValue={formFact.factura_electronica}
                  placeholder="FE-MC-00001 / Nombre propietario"
                  options={optFacturasElectronicas}
                  onSelect={o => setFF(p => ({ ...p, factura_electronica: o.label }))} />
              </SectionCard>

              {formFact.factura_electronica?.trim() && (
                <PanelManifiestosFE
                  manifiestos={manifiestosFE}
                  manifiestoActual={ficha?.manifiesto}
                  loading={loadingFE}
                />
              )}

              <div className="flex justify-end gap-2">
                <button type="button" onClick={revertAll}
                  className="flex items-center gap-1.5 px-4 py-2 rounded-lg text-sm font-semibold transition-opacity hover:opacity-80"
                  style={{ background: GOLD + '18', color: '#78400A', border: `1px solid ${GOLD}44` }}>
                  <RotateCcw size={13} /> Restablecer
                </button>
                <button type="submit" disabled={busy}
                  className="flex items-center gap-2 px-6 py-2.5 rounded-lg text-sm font-semibold disabled:opacity-50"
                  style={{ background: BTN_GRAD, color: '#FFFFFF', boxShadow: BTN_SHADOW }}>
                  <Save size={14} /> {busy ? 'Guardando...' : 'Guardar facturación'}
                </button>
              </div>
            </form>
          )}
        </div>
      )}

      {msg && <Toast msg={msg} onClose={() => setMsg(null)} />}
    </div>
  )
}
