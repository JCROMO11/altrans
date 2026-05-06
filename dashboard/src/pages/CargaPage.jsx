import { useState, useEffect, useRef, useCallback } from 'react'
import { Search, Plus, ArrowLeft, Save, CheckCircle, AlertCircle,
         User, MapPin, DollarSign, FileText, ClipboardList,
         ChevronDown, Check, Calendar, Pencil, Trash2, X, Clock,
         ChevronUp, RotateCcw, Lock } from 'lucide-react'
import { supabase } from '../lib/supabase'
import { useCatalogos } from '../hooks/useCatalogos'
import { useManifiesto } from '../hooks/useManifiesto'

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

function DateInput({ label, col, value, onChange }) {
  return (
    <Field label={label} col={col}>
      <div className="relative">
        <input type="date" value={value} onChange={onChange}
          className={inputCls} style={{ borderColor: BDR }} />
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
                  <Plus size={12} />
                  {`Usar "${query.trim()}"`}
                </button>
              )}
            </div>
          </div>
        )}
      </div>
    </Field>
  )
}

function SectionCard({ icon: Icon, title, children, cols = 3 }) {
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
const NUEVO_INIT = {
  manifiesto: '', fecha_despacho: '',
  conductor: '', cedula_conductor: '', celular: '',
  placa: '', tipo_vehiculo: '', propietario: '',
  cliente: '', origen: '', destino: '',
  agencia_despachadora: '', nombre_responsable: '',
  valor_remesa: '', flete_conductor: '', anticipo: '',
  remesas: '',
}

const SEGUIMIENTO_INIT = {
  fecha_cumplido: '', compromiso_pago: 'PAGO A 15 DIAS', novedades: '',
  estado_interno: '', responsable_estado_interno: '',
  novedad_conductor: '', novedad_empresa: '',
  ajuste_positivo_flete: '', ajuste_negativo_flete: '',
}

const TESORERIA_INIT = {
  fecha_pago: '', valor_pagado: '', entidad_financiera: '', responsable: '',
}

const FACT_INIT = {
  factura_no: '', fecha_factura: '', factura_electronica: '', mes_facturacion: '',
}

// ── Main Page ────────────────────────────────────────────────────────────────
export default function CargaPage({ target, clearTarget, user }) {
  const rol = user?.app_metadata?.role || ''
  const canEditDespacho   = ['digitador', 'admin'].includes(rol)
  const canEditOperativo  = ['operativo',  'admin'].includes(rol)
  const canEditTesoreria  = ['tesoreria',  'admin'].includes(rol)
  const canEditFinanciero = ['financiero', 'admin'].includes(rol)

  const [query,  setQuery]  = useState('')
  const [view,   setView]   = useState('inicio')
  const [ficha,  setFicha]  = useState(null)
  const [tab,    setTab]    = useState('despacho')
  const [formNuevo, setFN]  = useState(NUEVO_INIT)
  const [formSeg,   setFS]  = useState(SEGUIMIENTO_INIT)
  const [formTes,   setFT]  = useState(TESORERIA_INIT)
  const [formFact,  setFF]  = useState(FACT_INIT)
  const [formEdit,  setFE]  = useState({})
  const [editMode,  setEditMode]    = useState(false)
  const [confirmDel, setConfirmDel]     = useState(false)
  const [deleteText, setDeleteText]     = useState('')
  const [recientes,  setRecientes]  = useState([])
  const [recentesOpen, setRecentesOpen] = useState(false)
  const [sessionIds, setSessionIds] = useState(new Set())
  const [busy,   setBusy]   = useState(false)
  const [msg,    setMsg]    = useState(null)

  const { catalogos } = useCatalogos()
  const { search, create, update, remove, updateSeguimiento, updateTesoreria, updateFacturacion } = useManifiesto()

  // ── Catalog options ─────────────────────────────────────────────────────────
  const optConductores = catalogos.conductores.map(c => ({ id: c.nombre, label: c.nombre, sub: c.cedula }))
  const optClientes    = catalogos.clientes.map(c => ({ id: c.nombre, label: c.nombre }))
  const optLugares     = catalogos.lugares.map(l => ({ id: l.nombre, label: l.nombre }))
  const optResponsables = catalogos.responsables.map(r => ({ id: r.nombre, label: r.nombre }))
  const optVehiculos   = catalogos.vehiculos.map(v => ({ id: v.placa, label: v.placa }))
  const optRemolques   = catalogos.remolques.map(r => ({ id: r.placa, label: r.placa }))
  const optAgencias    = catalogos.agencias.map(a => ({ id: a.nombre, label: a.nombre }))
  const optPropietarios = catalogos.propietarios.map(p => ({ id: p.nombre, label: p.nombre }))

  const newText = (nombre) => ({ id: nombre, nombre, label: nombre, placa: nombre })

  // ── Load ficha ──────────────────────────────────────────────────────────────
  const userName = user?.app_metadata?.nombre || user?.email || ''

  const loadFicha = (data) => {
    setFicha(data)
    setTab('despacho')
    setEditMode(false)
    setConfirmDel(false)
    setFE({
      fecha_despacho:       data.fecha_despacho          || '',
      conductor:            data.conductor               || '',
      cedula_conductor:     data.cedula_conductor        || '',
      celular:              data.celular                 || '',
      placa:                data.placa                   || '',
      tipo_vehiculo:        data.tipo_vehiculo           || '',
      propietario:          data.propietario             || '',
      cliente:              data.cliente                 || '',
      origen:               data.origen                 || '',
      destino:              data.destino                || '',
      agencia_despachadora: data.agencia_despachadora   || '',
      nombre_responsable:   data.nombre_responsable      || '',
      valor_remesa:         data.valor_remesa            ?? '',
      flete_conductor:      data.flete_conductor         ?? '',
      anticipo:             data.anticipo               ?? '',
      remesas:              data.remesas                || '',
    })
    setFS({
      fecha_cumplido:              data.fecha_cumplido              || '',
      compromiso_pago:             data.compromiso_pago             || 'PAGO A 15 DIAS',
      novedades:                   data.novedades                   || '',
      estado_interno:              data.estado_interno              || '',
      responsable_estado_interno:  userName,
      novedad_conductor:           data.novedad_conductor           || '',
      novedad_empresa:             data.novedad_empresa             || '',
      ajuste_positivo_flete:       data.ajuste_positivo_flete       ?? '',
      ajuste_negativo_flete:       data.ajuste_negativo_flete       ?? '',
    })
    setFT({
      fecha_pago:         data.fecha_pago         || '',
      valor_pagado:       data.valor_pagado        ?? '',
      entidad_financiera: data.entidad_financiera  || '',
      responsable:        userName, // siempre el usuario actual al editar
    })
    setFF({
      factura_no:          data.factura_no          || '',
      fecha_factura:       data.fecha_factura        || '',
      factura_electronica: data.factura_electronica  || '',
      mes_facturacion:     data.mes_facturacion      ?? '',
    })
  }

  const revertEdit = () => {
    if (!ficha) return
    setFE({
      fecha_despacho:       ficha.fecha_despacho          || '',
      conductor:            ficha.conductor               || '',
      cedula_conductor:     ficha.cedula_conductor        || '',
      celular:              ficha.celular                 || '',
      placa:                ficha.placa                   || '',
      tipo_vehiculo:        ficha.tipo_vehiculo           || '',
      propietario:          ficha.propietario             || '',
      cliente:              ficha.cliente                 || '',
      origen:               ficha.origen                 || '',
      destino:              ficha.destino                || '',
      agencia_despachadora: ficha.agencia_despachadora   || '',
      nombre_responsable:   ficha.nombre_responsable      || '',
      valor_remesa:         ficha.valor_remesa            ?? '',
      flete_conductor:      ficha.flete_conductor         ?? '',
      anticipo:             ficha.anticipo               ?? '',
      remesas:              ficha.remesas                || '',
    })
    toast('success', 'Campos restaurados a los valores guardados.')
  }

  const fetchRecientes = useCallback(async () => {
    const { data } = await supabase
      .from('manifiestos_flat')
      .select('manifiesto, fecha_despacho, mes, año, conductor, origen, destino')
      .order('actualizado_en', { ascending: false })
      .limit(8)
    setRecientes(data ?? [])
  }, [])

  useEffect(() => { fetchRecientes() }, [fetchRecientes])

  useEffect(() => {
    if (!target) return
    setQuery(String(target))
    search(target).then(data => {
      if (data) { loadFicha(data); setView('ficha') }
      else toast('error', `Manifiesto ${target} no encontrado`)
    }).catch(() => toast('error', 'Error al cargar manifiesto'))
    clearTarget?.()
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [target])

  const toast = (type, text) => setMsg({ type, text })

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
    if (!query.trim()) return
    setBusy(true)
    try {
      const data = await search(Number(query.trim()))
      if (data) {
        loadFicha(data)
        setView('ficha')
      } else {
        setFN({ ...NUEVO_INIT, manifiesto: query.trim(), nombre_responsable: userName })
        setView('nuevo')
        toast('error', `Manifiesto ${query.trim()} no encontrado — completá el formulario para crearlo.`)
      }
    } catch (err) {
      toast('error', err.message ?? 'Error al buscar')
    } finally { setBusy(false) }
  }

  const handleCrear = async (e) => {
    e.preventDefault()
    if (!formNuevo.manifiesto || !formNuevo.conductor || !formNuevo.cliente ||
        !formNuevo.origen     || !formNuevo.destino   || !formNuevo.fecha_despacho) {
      toast('error', 'Completá los campos obligatorios (*)'); return
    }
    setBusy(true)
    try {
      await create(formNuevo)
      const num = Number(formNuevo.manifiesto)
      setSessionIds(prev => new Set([...prev, num]))
      toast('success', `Manifiesto ${formNuevo.manifiesto} creado correctamente.`)
      setQuery(formNuevo.manifiesto)
      const data = await search(num)
      loadFicha(data)
      setView('ficha')
      fetchRecientes()
    } catch (err) {
      toast('error', err.message ?? 'Error al crear')
    } finally { setBusy(false) }
  }

  const handleUpdate = async (e) => {
    e.preventDefault()
    if (!formEdit.conductor || !formEdit.cliente ||
        !formEdit.origen    || !formEdit.destino || !formEdit.fecha_despacho) {
      toast('error', 'Completá los campos obligatorios (*)'); return
    }
    setBusy(true)
    try {
      await update(ficha.manifiesto, formEdit)
      toast('success', 'Manifiesto actualizado correctamente.')
      const data = await search(ficha.manifiesto)
      loadFicha(data)
      fetchRecientes()
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
      setSessionIds(prev => { const s = new Set(prev); s.delete(num); return s })
      fetchRecientes()
      volver()
    } catch (err) {
      toast('error', err.message ?? 'Error al eliminar')
    } finally { setBusy(false) }
  }

  const handleSaveSeg = async (e) => {
    e.preventDefault()
    setBusy(true)
    try {
      await updateSeguimiento(ficha.manifiesto, formSeg)
      toast('success', 'Seguimiento actualizado correctamente.')
      const data = await search(ficha.manifiesto)
      loadFicha(data)
    } catch (err) { toast('error', err.message ?? 'Error al guardar') }
    finally { setBusy(false) }
  }

  const handleSaveTes = async (e) => {
    e.preventDefault()
    setBusy(true)
    try {
      await updateTesoreria(ficha.manifiesto, formTes)
      toast('success', 'Tesorería actualizada correctamente.')
      const data = await search(ficha.manifiesto)
      loadFicha(data)
    } catch (err) { toast('error', err.message ?? 'Error al guardar') }
    finally { setBusy(false) }
  }

  const handleSaveFact = async (e) => {
    e.preventDefault()
    if (formFact.fecha_factura && (!formFact.factura_no || !formFact.factura_electronica)) {
      toast('error', 'Si ingresas la fecha de emisión, debes completar también el N° de factura y la factura electrónica.')
      return
    }
    setBusy(true)
    try {
      await updateFacturacion(ficha.manifiesto, formFact)
      toast('success', 'Facturación actualizada correctamente.')
      const data = await search(ficha.manifiesto)
      loadFicha(data)
    } catch (err) { toast('error', err.message ?? 'Error al guardar') }
    finally { setBusy(false) }
  }

  const volver = () => { setView('inicio'); setFicha(null); setQuery(''); setEditMode(false); setConfirmDel(false); setDeleteText('') }
  const fn = (key) => (val) => setFN(p => ({ ...p, [key]: val }))
  const fs = (key) => (val) => setFS(p => ({ ...p, [key]: val }))
  const ft = (key) => (val) => setFT(p => ({ ...p, [key]: val }))
  const ff = (key) => (val) => setFF(p => ({ ...p, [key]: val }))
  const fe = (key) => (val) => setFE(p => ({ ...p, [key]: val }))

  // ── Render ─────────────────────────────────────────────────────────────────
  return (
    <div className="flex flex-col gap-6 max-w-5xl mx-auto">

      {/* Search bar */}
      <form onSubmit={handleSearch} className="flex gap-2">
        <div className="relative flex-1">
          <Search size={15} className="absolute left-3 top-1/2 -translate-y-1/2 pointer-events-none" style={{ color: MUTED }} />
          <input value={query} onChange={e => setQuery(e.target.value)}
            placeholder="Buscar por número de manifiesto..."
            className={inputCls + ' pl-9'} style={{ borderColor: BDR }}
          />
        </div>
        <button type="submit" disabled={busy || !query.trim()}
          className="px-4 py-2 rounded-lg text-sm font-semibold transition-opacity disabled:opacity-50"
          style={{ background: BLUE, color: '#FFFFFF' }}>
          Buscar
        </button>
        {view !== 'nuevo' && canEditDespacho && (
          <button type="button" onClick={() => { setFN({ ...NUEVO_INIT, nombre_responsable: userName }); setView('nuevo') }}
            className="flex items-center gap-1.5 px-4 py-2 rounded-lg text-sm font-semibold transition-colors hover:opacity-80"
            style={{ background: '#DCFCE7', color: '#166534', border: '1px solid #86EFAC' }}>
            <Plus size={14} /> Nuevo
          </button>
        )}
      </form>

      {/* ── INICIO ─────────────────────────────────────────────────────────── */}
      {view === 'inicio' && (
        <div className="flex flex-col items-center justify-center gap-3 py-24 text-center">
          <Search size={32} style={{ color: MUTED }} />
          <p className="text-sm font-medium" style={{ color: TICK }}>Buscá un manifiesto o creá uno nuevo</p>
          <p className="text-xs" style={{ color: MUTED }}>Ingresá el número de manifiesto para ver o editar su información</p>
        </div>
      )}

      {/* ── NUEVO ──────────────────────────────────────────────────────────── */}
      {view === 'nuevo' && (
        <form onSubmit={handleCrear} className="flex flex-col gap-4">
          <button type="button" onClick={volver}
            className="flex items-center gap-1.5 text-xs w-fit transition-opacity hover:opacity-70"
            style={{ color: MUTED }}>
            <ArrowLeft size={13} /> Volver
          </button>

          <SectionCard icon={FileText} title="Identificación" cols={3}>
            <Input label="N° Manifiesto *" type="number" placeholder="12345"
              value={formNuevo.manifiesto} onChange={e => fn('manifiesto')(e.target.value)} />
            <DateInput label="Fecha despacho *"
              value={formNuevo.fecha_despacho} onChange={e => fn('fecha_despacho')(e.target.value)} />
            <Autocomplete label="Agencia" displayValue={formNuevo.agencia_despachadora}
              placeholder="Agencia despachadora" options={optAgencias} onCreate={newText}
              onSelect={o => setFN(p => ({ ...p, agencia_despachadora: o.label }))} />
          </SectionCard>

          <SectionCard icon={User} title="Personal" cols={3}>
            <Autocomplete label="Conductor *" displayValue={formNuevo.conductor}
              placeholder="Nombre del conductor" options={optConductores} onCreate={newText}
              onSelect={o => fillConductor(o.label, setFN)} />
            <Input label="Cédula conductor" placeholder="Número de cédula"
              value={formNuevo.cedula_conductor} onChange={e => fn('cedula_conductor')(e.target.value)} />
            <Input label="Celular conductor" placeholder="Número de celular"
              value={formNuevo.celular} onChange={e => fn('celular')(e.target.value)} />
            <Autocomplete label="Cliente *" displayValue={formNuevo.cliente}
              placeholder="Nombre del cliente" options={optClientes} onCreate={newText}
              onSelect={o => setFN(p => ({ ...p, cliente: o.label }))} />
            <Autocomplete label="Responsable despacho" displayValue={formNuevo.nombre_responsable}
              placeholder="Nombre del responsable" options={optResponsables} onCreate={newText}
              onSelect={o => setFN(p => ({ ...p, nombre_responsable: o.label }))} />
          </SectionCard>

          <SectionCard icon={MapPin} title="Ruta" cols={3}>
            <Autocomplete label="Origen *" displayValue={formNuevo.origen}
              placeholder="Ciudad de origen" options={optLugares} onCreate={newText}
              onSelect={o => setFN(p => ({ ...p, origen: o.label }))} />
            <Autocomplete label="Destino *" displayValue={formNuevo.destino}
              placeholder="Ciudad de destino" options={optLugares} onCreate={newText}
              onSelect={o => setFN(p => ({ ...p, destino: o.label }))} />
            <Autocomplete label="Placa vehículo" displayValue={formNuevo.placa}
              placeholder="Placa del vehículo" options={optVehiculos} onCreate={newText}
              onSelect={o => setFN(p => ({ ...p, placa: o.label }))} />
            <Autocomplete label="Placa remolque" displayValue={formNuevo.tipo_vehiculo}
              placeholder="Placa del remolque" options={optRemolques} onCreate={newText}
              onSelect={o => setFN(p => ({ ...p, tipo_vehiculo: o.label }))} />
            <Autocomplete label="Propietario vehículo" displayValue={formNuevo.propietario}
              placeholder="Nombre del propietario" options={optPropietarios} onCreate={newText}
              onSelect={o => setFN(p => ({ ...p, propietario: o.label }))} />
          </SectionCard>

          <SectionCard icon={DollarSign} title="Financiero" cols={3}>
            <MoneyInput label="Valor remesa"
              value={formNuevo.valor_remesa} onChange={e => fn('valor_remesa')(e.target.value)} />
            <MoneyInput label="Flete conductor"
              value={formNuevo.flete_conductor} onChange={e => fn('flete_conductor')(e.target.value)} />
            <MoneyInput label="Anticipo"
              value={formNuevo.anticipo} onChange={e => fn('anticipo')(e.target.value)} />
          </SectionCard>

          <SectionCard icon={ClipboardList} title="Remesas" cols={1}>
            <Field label="Códigos de remesa (separados por ;)" col={1}>
              <input className={inputCls} style={{ borderColor: BDR }}
                placeholder="27854; 27855; 27856"
                value={formNuevo.remesas} onChange={e => fn('remesas')(e.target.value)} />
            </Field>
          </SectionCard>

          <div className="flex justify-end">
            <button type="submit" disabled={busy}
              className="flex items-center gap-2 px-6 py-2.5 rounded-lg text-sm font-semibold disabled:opacity-50 transition-opacity"
              style={{ background: BLUE, color: '#FFFFFF' }}>
              <Save size={14} />
              {busy ? 'Guardando...' : 'Crear manifiesto'}
            </button>
          </div>
        </form>
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
                {tab === 'despacho' && canEditDespacho && (
                  <button type="button" onClick={() => setEditMode(v => !v)}
                    className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-semibold transition-colors"
                    style={editMode
                      ? { background: '#F1F5F9', color: MUTED, border: `1px solid ${BDR}` }
                      : { background: BLUE + '15', color: BLUE, border: `1px solid ${BLUE}` }}>
                    {editMode ? <><X size={12} /> Cancelar</> : <><Pencil size={12} /> Editar</>}
                  </button>
                )}
                {rol === 'admin' && (
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
              { l: 'Agencia',   v: ficha.agencia_despachadora ?? '—' },
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
              { id: 'seguimiento', label: 'Seguimiento' },
              { id: 'tesoreria',   label: 'Tesorería' },
              { id: 'facturacion', label: 'Facturación' },
            ].map(t => (
              <button key={t.id} onClick={() => { setTab(t.id); setEditMode(false) }}
                className="px-4 py-1.5 rounded-md text-xs font-semibold transition-all"
                style={tab === t.id ? { background: BLUE, color: '#FFFFFF' } : { color: MUTED }}>
                {t.label}
              </button>
            ))}
          </div>

          {/* Tab: Despacho — readonly */}
          {tab === 'despacho' && !editMode && (() => {
            const estadoColor = {
              'PAGADO': '#22c55e', 'ANULADO': '#ef4444',
              'PRIORITARIO': '#f97316', 'RNDC': '#a855f7',
            }[ficha.compromiso_pago] ?? TICK
            return (
              <div className="flex flex-col gap-4">
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
                      { l: 'Tipo vehículo',   v: ficha.tipo_vehiculo },
                      { l: 'Propietario',     v: ficha.propietario },
                      { l: 'Cliente',         v: ficha.cliente },
                      { l: 'Origen',          v: ficha.origen },
                      { l: 'Destino',         v: ficha.destino },
                      { l: 'Agencia',         v: ficha.agencia_despachadora },
                      { l: 'Responsable',     v: ficha.nombre_responsable },
                      { l: 'Valor remesa',        v: ficha.valor_remesa         != null ? `$${Number(ficha.valor_remesa).toLocaleString('es-CO')}` : null },
                      { l: 'Flete conductor',     v: ficha.flete_conductor      != null ? `$${Number(ficha.flete_conductor).toLocaleString('es-CO')}` : null },
                      { l: 'Flete neto conductor',v: ficha.flete_neto_conductor != null ? `$${Number(ficha.flete_neto_conductor).toLocaleString('es-CO')}` : null },
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
                    title: 'Seguimiento',
                    items: [
                      { l: 'Compromiso pago',   v: ficha.compromiso_pago, color: estadoColor },
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
                      { l: 'N° Factura',                   v: ficha.factura_no },
                      { l: 'Fecha de emisión de factura',  v: ficha.fecha_factura },
                      { l: 'Mes facturación',   v: ficha.mes_facturacion },
                    ],
                  },
                ].map(({ title, items }) => (
                  <div key={title}>
                    <p className="text-[10px] font-bold uppercase tracking-widest mb-2" style={{ color: MUTED }}>{title}</p>
                    <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-2">
                      {items.map(({ l, v, col, color }) => (
                        <div key={l} className="rounded-lg px-3 py-2.5"
                          style={{ background: BG, border: `1px solid ${BDR}`, gridColumn: col ? `span ${col}` : undefined }}>
                          <p className="text-[10px] font-bold uppercase tracking-wider mb-0.5" style={{ color: MUTED }}>{l}</p>
                          <p className="text-sm font-semibold" style={{ color: color ?? (v ? TICK : MUTED) }}>{v ?? '—'}</p>
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
                <Autocomplete label="Agencia" displayValue={formEdit.agencia_despachadora}
                  placeholder="Agencia despachadora" options={optAgencias} onCreate={newText}
                  onSelect={o => setFE(p => ({ ...p, agencia_despachadora: o.label }))} />
              </SectionCard>
              <SectionCard icon={User} title="Personal" cols={3}>
                <Autocomplete label="Conductor *" displayValue={formEdit.conductor}
                  placeholder="Nombre del conductor" options={optConductores} onCreate={newText}
                  onSelect={o => fillConductor(o.label, setFE)} />
                <Input label="Cédula conductor" placeholder="Número de cédula"
                  value={formEdit.cedula_conductor ?? ''} onChange={e => fe('cedula_conductor')(e.target.value)} />
                <Input label="Celular conductor" placeholder="Número de celular"
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
                <Autocomplete label="Placa remolque" displayValue={formEdit.tipo_vehiculo}
                  placeholder="Placa del remolque" options={optRemolques} onCreate={newText}
                  onSelect={o => setFE(p => ({ ...p, tipo_vehiculo: o.label }))} />
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
                <button type="button" onClick={revertEdit}
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
                  style={{ background: BLUE, color: '#FFFFFF' }}>
                  <Save size={14} /> {busy ? 'Guardando...' : 'Guardar cambios'}
                </button>
              </div>
            </form>
          )}

          {/* Tab: Seguimiento — lectura para roles sin permiso */}
          {tab === 'seguimiento' && !canEditOperativo && (
            <div className="flex flex-col gap-4">
              <div>
                <p className="text-[10px] font-bold uppercase tracking-widest mb-2" style={{ color: MUTED }}>Seguimiento operativo</p>
                <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-2">
                  {[
                    { l: 'Fecha cumplido',          v: ficha.fecha_cumplido },
                    { l: 'Compromiso pago',          v: ficha.compromiso_pago },
                    { l: 'Estado interno',           v: ficha.estado_interno },
                    { l: 'Responsable estado int.',  v: ficha.responsable_estado_interno },
                    { l: 'Novedades',                v: ficha.novedades, col: 4 },
                    { l: 'Novedad del conductor',    v: ficha.novedad_conductor, col: 2 },
                    { l: 'Novedad de la empresa',    v: ficha.novedad_empresa, col: 2 },
                    { l: 'Ajuste positivo al flete', v: ficha.ajuste_positivo_flete != null ? `$ ${Number(ficha.ajuste_positivo_flete).toLocaleString('es-CO')}` : null },
                    { l: 'Ajuste negativo al flete', v: ficha.ajuste_negativo_flete != null ? `$ ${Number(ficha.ajuste_negativo_flete).toLocaleString('es-CO')}` : null },
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

          {/* Tab: Seguimiento — edición para operativo/admin */}
          {tab === 'seguimiento' && canEditOperativo && (
            <form onSubmit={handleSaveSeg} className="flex flex-col gap-4">
              <SectionCard icon={ClipboardList} title="Seguimiento operativo" cols={3}>
                <DateInput label="Fecha cumplido"
                  value={formSeg.fecha_cumplido} onChange={e => fs('fecha_cumplido')(e.target.value)} />
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
                <MoneyInput label="Ajuste positivo al flete"
                  value={formSeg.ajuste_positivo_flete}
                  onChange={e => fs('ajuste_positivo_flete')(e.target.value)} />
                <MoneyInput label="Ajuste negativo al flete"
                  value={formSeg.ajuste_negativo_flete}
                  onChange={e => fs('ajuste_negativo_flete')(e.target.value)} />
              </SectionCard>
              <div className="flex justify-end">
                <button type="submit" disabled={busy}
                  className="flex items-center gap-2 px-6 py-2.5 rounded-lg text-sm font-semibold disabled:opacity-50"
                  style={{ background: BLUE, color: '#FFFFFF' }}>
                  <Save size={14} /> {busy ? 'Guardando...' : 'Guardar seguimiento'}
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
              <div className="flex justify-end">
                <button type="submit" disabled={busy}
                  className="flex items-center gap-2 px-6 py-2.5 rounded-lg text-sm font-semibold disabled:opacity-50"
                  style={{ background: BLUE, color: '#FFFFFF' }}>
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
                    { l: 'Mes facturación',       v: ficha.mes_facturacion },
                    { l: 'Factura electrónica',   v: ficha.factura_electronica, col: 2 },
                  ].map(({ l, v, col }) => (
                    <div key={l} className="rounded-lg px-3 py-2.5"
                      style={{ background: BG, border: `1px solid ${BDR}`, gridColumn: col ? `span ${col}` : undefined }}>
                      <p className="text-[10px] font-bold uppercase tracking-wider mb-0.5" style={{ color: MUTED }}>{l}</p>
                      <p className="text-sm" style={{ color: v ? TICK : MUTED }}>{v ?? '—'}</p>
                    </div>
                  ))}
                </div>
              </div>
              <p className="text-xs" style={{ color: MUTED }}>Solo el equipo financiero puede editar esta sección.</p>
            </div>
          )}

          {/* Tab: Facturación — edición para financiero/admin (mes_facturacion calculado, no visible) */}
          {tab === 'facturacion' && canEditFinanciero && (
            <form onSubmit={handleSaveFact} className="flex flex-col gap-4">
              <SectionCard icon={FileText} title="Facturación" cols={2}>
                <Input label="N° Factura" placeholder="FE-0001"
                  value={formFact.factura_no} onChange={e => ff('factura_no')(e.target.value)} />
                <DateInput label="Fecha de emisión de factura"
                  value={formFact.fecha_factura}
                  onChange={e => {
                    const v = e.target.value
                    const mes = v ? new Date(v + 'T12:00:00').getMonth() + 1 : ''
                    setFF(p => ({ ...p, fecha_factura: v, mes_facturacion: mes !== '' ? mes : '' }))
                  }} />
              </SectionCard>
              <SectionCard icon={ClipboardList} title="Factura electrónica" cols={1}>
                <Field label="N° Factura electrónica / Propietario vehículo" col={1}>
                  <input className={inputCls} style={{ borderColor: BDR }}
                    placeholder="FE-MC-00001 / Nombre propietario"
                    value={formFact.factura_electronica}
                    onChange={e => ff('factura_electronica')(e.target.value)} />
                </Field>
              </SectionCard>
              <div className="flex justify-end">
                <button type="submit" disabled={busy}
                  className="flex items-center gap-2 px-6 py-2.5 rounded-lg text-sm font-semibold disabled:opacity-50"
                  style={{ background: BLUE, color: '#FFFFFF' }}>
                  <Save size={14} /> {busy ? 'Guardando...' : 'Guardar facturación'}
                </button>
              </div>
            </form>
          )}
        </div>
      )}

      {/* ── RECIENTES ─────────────────────────────────────────────────────── */}
      {recientes.length > 0 && (
        <div className="flex flex-col gap-3 pt-2 border-t" style={{ borderColor: BDR }}>
          <button type="button" onClick={() => setRecentesOpen(v => !v)}
            className="flex items-center gap-2 w-fit transition-opacity hover:opacity-70">
            <Clock size={12} style={{ color: MUTED }} />
            <p className="text-[10px] font-bold uppercase tracking-widest" style={{ color: MUTED }}>
              Últimos manifiestos
            </p>
            {sessionIds.size > 0 && (
              <span className="text-[9px] px-1.5 py-0.5 rounded-full font-semibold"
                style={{ background: '#2d1f00', color: GOLD, border: `1px solid #78540a` }}>
                ★ esta sesión
              </span>
            )}
            {recentesOpen ? <ChevronUp size={12} style={{ color: MUTED }} /> : <ChevronDown size={12} style={{ color: MUTED }} />}
          </button>

          {recentesOpen && (
            <div className="rounded-xl overflow-hidden" style={{ border: `1px solid ${BDR}` }}>
              <div className="grid grid-cols-[80px_110px_1fr_1fr_100px] px-4 py-2"
                style={{ background: '#F1F5F9', borderBottom: `1px solid ${BDR}` }}>
                {['N°', 'Fecha', 'Conductor', 'Ruta', 'Período'].map(h => (
                  <span key={h} className="text-[10px] font-bold uppercase tracking-widest" style={{ color: MUTED }}>{h}</span>
                ))}
              </div>
              {recientes.map((r, i) => {
                const esMio = sessionIds.has(r.manifiesto)
                return (
                  <button key={r.manifiesto} type="button"
                    onClick={() => {
                      setQuery(String(r.manifiesto))
                      search(r.manifiesto).then(data => { if (data) { loadFicha(data); setView('ficha') } })
                    }}
                    className="grid grid-cols-[80px_110px_1fr_1fr_100px] w-full px-4 py-2.5 text-left transition-colors hover:bg-black/5"
                    style={{
                      background: esMio ? 'rgba(201,168,76,0.06)' : i % 2 === 0 ? BG : 'transparent',
                      borderTop: i > 0 ? `1px solid ${BDR}` : 'none',
                    }}>
                    <span className="text-sm font-bold" style={{ color: esMio ? GOLD : BLUE }}>
                      {r.manifiesto}{esMio && <span className="ml-1 text-[9px]">★</span>}
                    </span>
                    <span className="text-xs" style={{ color: MUTED }}>{r.fecha_despacho}</span>
                    <span className="text-xs truncate pr-2" style={{ color: TICK }}>{r.conductor ?? '—'}</span>
                    <span className="text-xs truncate pr-2" style={{ color: MUTED }}>
                      {r.origen && r.destino ? `${r.origen} → ${r.destino}` : '—'}
                    </span>
                    <span className="text-xs" style={{ color: esMio ? GOLD : MUTED }}>{r.mes} {r.año}</span>
                  </button>
                )
              })}
            </div>
          )}
        </div>
      )}

      {msg && <Toast msg={msg} onClose={() => setMsg(null)} />}
    </div>
  )
}
