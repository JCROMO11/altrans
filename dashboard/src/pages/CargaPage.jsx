import { useState, useEffect, useRef, useCallback } from 'react'
import { Search, Plus, ArrowLeft, Save, CheckCircle, AlertCircle,
         User, MapPin, DollarSign, FileText, ClipboardList,
         ChevronDown, Check, Calendar, Pencil, Trash2, X, Clock,
         ChevronUp, RotateCcw } from 'lucide-react'
import { supabase } from '../lib/supabase'
import { useCatalogos } from '../hooks/useCatalogos'
import { useManifiesto } from '../hooks/useManifiesto'

// ── ENUMs ────────────────────────────────────────────────────────────────────
const ESTADO_PAGO_OPTS     = ['PAGO A 15 DIAS','PAGO A 20 DIAS','PAGO A 30 DIAS','PAGO A 5-8 DIAS','CONTRAENTREGA','PRONTO PAGO','PAGO NORMAL','PAGO INMEDIATO','URBANO','PAGADO','ANULADO','PRIORITARIO','RNDC','OTROS']
const CONDICION_PAGO_OPTS  = ['PAGO NORMAL','CONTRAENTREGA','PRONTO PAGO','CONTINGENCIA 20-25 DH']
const ENTIDAD_FIN_OPTS     = ['TRANSF BANCOLOMBIA','TRANSF DAVIVIENDA','CHEQUE BANCOLOMBIA','CHEQUE DAVIVIENDA','TRANSF BANCO DE BOGOTA','CHEQUE BANCO DE BOGOTA','CHEQUE','TRANSF/CHEQUE','ANULADO','OTRO']
const ESTADO_INTERNO_OPTS  = ['CUMPLIDO','NO SE HA CUMPLIDO','PENDIENTE FACTURA ELECTRONICA','FACTURA RECIBIDA','NOVEDAD PENDIENTE','ANULADO']

// ── Theme ────────────────────────────────────────────────────────────────────
const BG   = '#1B2B3B'
const BDR  = '#2A3F52'
const TICK = '#F0F4F8'
const BLUE = '#1E6FBF'
const GOLD = '#C9A84C'
const MUTED = '#8FA3B1'

// ── Primitives ───────────────────────────────────────────────────────────────
const inputCls = `w-full rounded-md border px-3 py-2 text-sm focus:outline-none focus:ring-1
  focus:ring-[#1E6FBF] transition-colors bg-transparent text-[#F0F4F8] placeholder:text-[#8FA3B1]`

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
            style={{ background: '#0f1e2b', border: `1px solid ${BDR}` }}>
            <button type="button" onMouseDown={() => { onChange(''); setOpen(false) }}
              className="w-full text-left px-3 py-2 text-sm transition-colors hover:bg-white/5"
              style={{ color: MUTED }}>{placeholder}</button>
            {options.map(o => (
              <button key={o} type="button" onMouseDown={() => { onChange(o); setOpen(false) }}
                className="w-full text-left px-3 py-2 text-sm transition-colors hover:bg-white/5 flex items-center justify-between"
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

function Autocomplete({ label, col, displayValue, onSelect, onCreate, options, placeholder, extraFields }) {
  const [query, setQuery]         = useState(displayValue || '')
  const [open, setOpen]           = useState(false)
  const [saving, setSaving]       = useState(false)
  const [showExtra, setShowExtra] = useState(false)
  const [extras, setExtras]       = useState({})
  const containerRef              = useRef(null)

  useEffect(() => { setQuery(displayValue || '') }, [displayValue])

  const filtered = options
    .filter(o => o.label.toLowerCase().includes(query.toLowerCase()))
    .slice(0, 10)

  const exactMatch = options.some(o => o.label.toLowerCase() === query.trim().toLowerCase())
  const showCreate = onCreate && query.trim().length >= 2 && !exactMatch

  const handleCreate = async () => {
    if (extraFields?.length && !showExtra) { setShowExtra(true); return }
    setSaving(true)
    try {
      const created = await onCreate(query.trim(), extras)
      if (created) onSelect({ id: created.id ?? created.placa, label: query.trim() })
      setOpen(false); setShowExtra(false); setExtras({})
    } finally { setSaving(false) }
  }

  // Only close when focus leaves the whole container
  const handleBlur = () => setTimeout(() => {
    if (!containerRef.current?.contains(document.activeElement)) {
      setOpen(false); setShowExtra(false); setExtras({})
    }
  }, 150)

  return (
    <Field label={label} col={col}>
      <div className="relative" ref={containerRef}>
        <input value={query} autoComplete="off" placeholder={placeholder}
          className={inputCls} style={{ borderColor: BDR }}
          onChange={e => { setQuery(e.target.value); setOpen(true); setShowExtra(false) }}
          onFocus={() => setOpen(true)}
          onBlur={handleBlur}
        />
        {open && (filtered.length > 0 || showCreate) && (
          <div className="absolute z-50 w-full mt-1 rounded-xl shadow-xl overflow-hidden"
            style={{ background: '#0f1e2b', border: `1px solid ${BDR}` }}>
            {!showExtra && (
              <div className="max-h-52 overflow-y-auto">
                {filtered.map(o => (
                  <button key={o.id} type="button" onMouseDown={() => { onSelect(o); setQuery(o.label); setOpen(false) }}
                    className="w-full text-left px-3 py-2 text-sm transition-colors hover:bg-white/5 flex items-center gap-2"
                    style={{ color: TICK }}>
                    <span>{o.label}</span>
                    {o.sub && <span className="text-xs opacity-40">{o.sub}</span>}
                  </button>
                ))}
                {showCreate && (
                  <button type="button" disabled={saving} onMouseDown={handleCreate}
                    className="w-full text-left px-3 py-2 text-sm border-t transition-colors hover:bg-white/5 flex items-center gap-2"
                    style={{ color: BLUE, borderColor: BDR }}>
                    <Plus size={12} />
                    {`Crear "${query.trim()}"${extraFields?.length ? ' →' : ''}`}
                  </button>
                )}
              </div>
            )}
            {showExtra && (
              <div className="p-3 flex flex-col gap-2">
                <p className="text-xs font-semibold mb-1" style={{ color: TICK }}>
                  Nuevo: <span style={{ color: BLUE }}>{query.trim()}</span>
                </p>
                {extraFields.map(f => (
                  <input key={f.key} placeholder={f.label} value={extras[f.key] || ''}
                    onChange={e => setExtras(p => ({ ...p, [f.key]: e.target.value }))}
                    className={inputCls} style={{ borderColor: BDR, fontSize: '12px', padding: '6px 10px' }} />
                ))}
                <button type="button" disabled={saving} onMouseDown={handleCreate}
                  className="w-full py-1.5 rounded-lg text-xs font-semibold mt-1 disabled:opacity-50 transition-opacity"
                  style={{ background: BLUE, color: TICK }}>
                  {saving ? 'Creando...' : 'Confirmar'}
                </button>
              </div>
            )}
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
      <div className={`grid gap-4`} style={{ gridTemplateColumns: `repeat(${cols}, minmax(0, 1fr))` }}>
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
      style={{ background: ok ? '#0f2e1a' : '#2e0f0f', border: `1px solid ${ok ? '#22c55e' : '#ef4444'}`, color: ok ? '#86efac' : '#fca5a5' }}>
      {ok ? <CheckCircle size={15} /> : <AlertCircle size={15} />}
      {msg.text}
    </div>
  )
}

// ── Form initial state ───────────────────────────────────────────────────────
const NUEVO_INIT = {
  manifiesto: '', fecha_despacho: '',
  conductor_id: null, conductor_nombre: '',
  conductor_cedula: '', conductor_celular: '',
  placa: '', placa_remolque: '',
  cliente_id: null, cliente_nombre: '',
  origen_id: null, origen_nombre: '',
  destino_id: null, destino_nombre: '',
  agencia_id: null,
  responsable_id: null, responsable_nombre: '',
  valor_remesa: '', flete_conductor: '', anticipo: '',
  remesas: '',
}

const PAGOS_INIT = {
  fecha_cumplido: '', estado: '', condicion_pago: '',
  novedades: '', fecha_pago: '', valor_pagado: '',
  entidad_financiera: '', responsable_id: null, responsable_nombre: '',
}

const FACT_INIT = {
  factura_no: '', fecha_factura: '', factura_electronica: '',
  dias_para_facturar: '', mes_facturacion: '',
  estado_interno: '', responsable_id: null, responsable_nombre: '',
}

// ── Main Page ────────────────────────────────────────────────────────────────
export default function CargaPage({ target, clearTarget }) {
  const [query,  setQuery]  = useState('')
  const [view,   setView]   = useState('inicio')
  const [ficha,  setFicha]  = useState(null)
  const [tab,    setTab]    = useState('despacho')
  const [formNuevo, setFN]  = useState(NUEVO_INIT)
  const [formPagos, setFP]  = useState(PAGOS_INIT)
  const [formFact,  setFF]  = useState(FACT_INIT)
  const [formEdit,  setFE]  = useState({})
  const [editMode,  setEditMode]    = useState(false)
  const [confirmDel, setConfirmDel] = useState(false)
  const [recientes,  setRecientes]  = useState([])
  const [recentesOpen, setRecentesOpen] = useState(false)
  const [sessionIds, setSessionIds] = useState(new Set())
  const [busy,   setBusy]   = useState(false)
  const [msg,    setMsg]    = useState(null)

  // ── Deferred catalog creation ─────────────────────────────────────────────
  // New catalog entries (conductor, cliente, etc.) are NOT saved to DB until
  // the manifiesto form is submitted, preventing orphan records.
  const [pendingEntries, setPendingEntries] = useState({})
  const tempCounterRef = useRef(0)

  const stageEntry = useCallback((type, nombre, extras = {}) => {
    const tempId = `__tmp__${++tempCounterRef.current}`
    setPendingEntries(prev => ({ ...prev, [tempId]: { type, nombre, extras } }))
    return { id: tempId, nombre, placa: nombre }
  }, [])

  const clearPending = useCallback(() => {
    setPendingEntries({})
    tempCounterRef.current = 0
  }, [])

  const pendingByType = (type) =>
    Object.entries(pendingEntries)
      .filter(([, e]) => e.type === type)
      .map(([id, e]) => ({ id, label: e.nombre, sub: e.extras?.cedula }))

  const { catalogos, loading: catLoading, createConductor, updateConductor,
          createCliente, createLugar, createResponsable, createVehiculo, createRemolque } = useCatalogos()
  const { search, create, update, remove, updatePagos, updateFacturacion } = useManifiesto()

  const flushPending = useCallback(async () => {
    const idMap = {}
    for (const [tempId, entry] of Object.entries(pendingEntries)) {
      let real
      switch (entry.type) {
        case 'conductor':   real = await createConductor(entry.nombre, entry.extras); break
        case 'cliente':     real = await createCliente(entry.nombre); break
        case 'lugar':       real = await createLugar(entry.nombre); break
        case 'responsable': real = await createResponsable(entry.nombre); break
        case 'vehiculo':    real = await createVehiculo(entry.nombre); break
        case 'remolque':    real = await createRemolque(entry.nombre); break
      }
      idMap[tempId] = real?.id ?? real?.placa
    }
    setPendingEntries({})
    return idMap
  }, [pendingEntries, createConductor, createCliente, createLugar, createResponsable, createVehiculo, createRemolque])

  const isTemp = id => typeof id === 'string' && String(id).startsWith('__tmp__')
  const remapIds = (form, idMap) => {
    const remap = id => (isTemp(id) ? (idMap[id] ?? id) : id)
    return {
      ...form,
      conductor_id:   remap(form.conductor_id),
      cliente_id:     remap(form.cliente_id),
      origen_id:      remap(form.origen_id),
      destino_id:     remap(form.destino_id),
      agencia_id:     remap(form.agencia_id),
      responsable_id: remap(form.responsable_id),
    }
  }

  const fetchRecientes = useCallback(async () => {
    const { data } = await supabase
      .from('manifiestos')
      .select('manifiesto, fecha_despacho, mes, año, conductor:conductores(nombre), origen:lugares!origen_id(nombre), destino:lugares!destino_id(nombre)')
      .order('manifiesto', { ascending: false })
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

  const notAnulado = (nombre) => nombre && nombre.toUpperCase() !== 'ANULADO'

  const optConductores = [
    ...catalogos.conductores.filter(c => notAnulado(c.nombre)).map(c => ({ id: c.id, label: c.nombre, sub: c.cedula })),
    ...pendingByType('conductor'),
  ]
  const optClientes = [
    ...catalogos.clientes.filter(c => notAnulado(c.nombre)).map(c => ({ id: c.id, label: c.nombre })),
    ...pendingByType('cliente'),
  ]
  const optLugares = [
    ...catalogos.lugares.filter(l => notAnulado(l.nombre)).map(l => ({ id: l.id, label: l.nombre })),
    ...pendingByType('lugar'),
  ]
  const optResponsables = [
    ...catalogos.responsables.filter(r => notAnulado(r.nombre)).map(r => ({ id: r.id, label: r.nombre })),
    ...pendingByType('responsable'),
  ]
  const optVehiculos = [
    ...catalogos.vehiculos.filter(v => notAnulado(v.placa)).map(v => ({ id: v.placa, label: v.placa })),
    ...pendingByType('vehiculo'),
  ]
  const optRemolques = [
    ...catalogos.remolques.filter(r => notAnulado(r.placa)).map(r => ({ id: r.placa, label: r.placa })),
    ...pendingByType('remolque'),
  ]
  const optAgencias = catalogos.agencias.filter(a => notAnulado(a.nombre)).map(a => ({ id: a.id, label: a.nombre }))

  const loadFicha = (data) => {
    setFicha(data)
    setTab('despacho')
    setEditMode(false)
    setConfirmDel(false)
    const cond = catalogos.conductores.find(c => c.id === data.conductor?.id)
    setFE({
      fecha_despacho:     data.fecha_despacho || '',
      conductor_id:       data.conductor?.id  ?? null,
      conductor_nombre:   data.conductor?.nombre || '',
      conductor_cedula:   cond?.cedula  ?? data.conductor?.cedula  ?? '',
      conductor_celular:  cond?.celular ?? data.conductor?.celular ?? '',
      placa:              data.placa           || '',
      placa_remolque:     data.placa_remolque  || '',
      cliente_id:         data.cliente?.id   ?? null,
      cliente_nombre:     data.cliente?.nombre || '',
      origen_id:          data.origen?.id    ?? null,
      origen_nombre:      data.origen?.nombre || '',
      destino_id:         data.destino?.id   ?? null,
      destino_nombre:     data.destino?.nombre || '',
      agencia_id:         data.agencia_id    ?? null,
      responsable_id:     data.responsable?.id ?? null,
      responsable_nombre: data.responsable?.nombre || '',
      valor_remesa:       data.valor_remesa    ?? '',
      flete_conductor:    data.flete_conductor ?? '',
      anticipo:           data.anticipo        ?? '',
      remesas:            data.remesas?.map(r => r.codigo_remesa).join('; ') || '',
    })
    const p = data.pagos_conductor?.[0] ?? {}
    const f = data.facturacion?.[0]    ?? {}
    const resolveResp = (id) => catalogos.responsables.find(r => r.id === id)?.nombre || ''
    setFP({
      fecha_cumplido: p.fecha_cumplido || '',
      estado: p.estado || '',
      condicion_pago: p.condicion_pago || '',
      novedades: p.novedades || '',
      fecha_pago: p.fecha_pago || '',
      valor_pagado: p.valor_pagado ?? '',
      entidad_financiera: p.entidad_financiera || '',
      responsable_id: p.responsable_id || null,
      responsable_nombre: resolveResp(p.responsable_id),
    })
    setFF({
      factura_no: f.factura_no || '',
      fecha_factura: f.fecha_factura || '',
      factura_electronica: f.factura_electronica || '',
      dias_para_facturar: f.dias_para_facturar ?? '',
      mes_facturacion: f.mes_facturacion ?? '',
      estado_interno: f.estado_interno || '',
      responsable_id: f.responsable_id || null,
      responsable_nombre: resolveResp(f.responsable_id),
    })
  }

  // Revert edit fields to last saved ficha state (without exiting edit mode)
  const revertEdit = () => {
    if (!ficha) return
    const cond = catalogos.conductores.find(c => c.id === ficha.conductor?.id)
    setFE({
      fecha_despacho:     ficha.fecha_despacho || '',
      conductor_id:       ficha.conductor?.id  ?? null,
      conductor_nombre:   ficha.conductor?.nombre || '',
      conductor_cedula:   cond?.cedula  ?? ficha.conductor?.cedula  ?? '',
      conductor_celular:  cond?.celular ?? ficha.conductor?.celular ?? '',
      placa:              ficha.placa           || '',
      placa_remolque:     ficha.placa_remolque  || '',
      cliente_id:         ficha.cliente?.id   ?? null,
      cliente_nombre:     ficha.cliente?.nombre || '',
      origen_id:          ficha.origen?.id    ?? null,
      origen_nombre:      ficha.origen?.nombre || '',
      destino_id:         ficha.destino?.id   ?? null,
      destino_nombre:     ficha.destino?.nombre || '',
      agencia_id:         ficha.agencia_id    ?? null,
      responsable_id:     ficha.responsable?.id ?? null,
      responsable_nombre: ficha.responsable?.nombre || '',
      valor_remesa:       ficha.valor_remesa    ?? '',
      flete_conductor:    ficha.flete_conductor ?? '',
      anticipo:           ficha.anticipo        ?? '',
      remesas:            ficha.remesas?.map(r => r.codigo_remesa).join('; ') || '',
    })
    clearPending()
    toast('success', 'Campos restaurados a los valores guardados.')
  }

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
        setFN({ ...NUEVO_INIT, manifiesto: query.trim() })
        setView('nuevo')
        toast('error', `Manifiesto ${query.trim()} no encontrado — completá el formulario para crearlo.`)
      }
    } catch (err) {
      toast('error', err.message ?? 'Error al buscar')
    } finally { setBusy(false) }
  }

  const handleCrear = async (e) => {
    e.preventDefault()
    if (!formNuevo.manifiesto || !formNuevo.conductor_id || !formNuevo.cliente_id ||
        !formNuevo.origen_id  || !formNuevo.destino_id  || !formNuevo.fecha_despacho) {
      toast('error', 'Completá los campos obligatorios (*)'); return
    }
    setBusy(true)
    try {
      // 1. Create any staged catalog entries
      const idMap = await flushPending()
      const resolvedForm = remapIds(formNuevo, idMap)

      // 2. Update conductor cedula/celular if they changed
      const conductorId = resolvedForm.conductor_id
      if (conductorId && !isTemp(conductorId)) {
        const existing = catalogos.conductores.find(c => c.id === conductorId)
        const updates = {}
        if (formNuevo.conductor_cedula  && formNuevo.conductor_cedula  !== existing?.cedula)  updates.cedula  = formNuevo.conductor_cedula
        if (formNuevo.conductor_celular && formNuevo.conductor_celular !== existing?.celular) updates.celular = formNuevo.conductor_celular
        if (Object.keys(updates).length) await updateConductor(conductorId, updates)
      }

      // 3. Create manifiesto
      await create(resolvedForm)
      const num = Number(resolvedForm.manifiesto)
      setSessionIds(prev => new Set([...prev, num]))
      toast('success', `Manifiesto ${resolvedForm.manifiesto} creado correctamente.`)
      setQuery(resolvedForm.manifiesto)
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
    if (!formEdit.conductor_id || !formEdit.cliente_id ||
        !formEdit.origen_id   || !formEdit.destino_id || !formEdit.fecha_despacho) {
      toast('error', 'Completá los campos obligatorios (*)'); return
    }
    setBusy(true)
    try {
      const idMap = await flushPending()
      const resolvedForm = remapIds(formEdit, idMap)

      // Update conductor cedula/celular if changed
      const conductorId = resolvedForm.conductor_id
      if (conductorId && !isTemp(conductorId)) {
        const existing = catalogos.conductores.find(c => c.id === conductorId)
        const updates = {}
        if (formEdit.conductor_cedula  !== undefined && formEdit.conductor_cedula  !== existing?.cedula)  updates.cedula  = formEdit.conductor_cedula  || null
        if (formEdit.conductor_celular !== undefined && formEdit.conductor_celular !== existing?.celular) updates.celular = formEdit.conductor_celular || null
        if (Object.keys(updates).length) await updateConductor(conductorId, updates)
      }

      await update(ficha.manifiesto, resolvedForm)
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

  const handleSavePagos = async (e) => {
    e.preventDefault()
    setBusy(true)
    try {
      await updatePagos(ficha.manifiesto, formPagos)
      toast('success', 'Pagos actualizados correctamente.')
    } catch (err) { toast('error', err.message ?? 'Error al guardar') }
    finally { setBusy(false) }
  }

  const handleSaveFact = async (e) => {
    e.preventDefault()
    setBusy(true)
    try {
      await updateFacturacion(ficha.manifiesto, formFact)
      toast('success', 'Facturación actualizada correctamente.')
    } catch (err) { toast('error', err.message ?? 'Error al guardar') }
    finally { setBusy(false) }
  }

  const volver = () => {
    setView('inicio'); setFicha(null); setQuery('')
    setEditMode(false); setConfirmDel(false)
    clearPending()
  }
  const fn = (key) => (val) => setFN(p => ({ ...p, [key]: val }))
  const fp = (key) => (val) => setFP(p => ({ ...p, [key]: val }))
  const ff = (key) => (val) => setFF(p => ({ ...p, [key]: val }))
  const fe = (key) => (val) => setFE(p => ({ ...p, [key]: val }))

  // Helper to fill conductor cedula/celular from catalog when a conductor is selected
  const selectConductorNuevo = (o) => {
    const cond = catalogos.conductores.find(c => c.id === o.id)
    setFN(p => ({
      ...p,
      conductor_id: o.id, conductor_nombre: o.label,
      conductor_cedula:  cond?.cedula  ?? p.conductor_cedula,
      conductor_celular: cond?.celular ?? p.conductor_celular,
    }))
  }
  const selectConductorEdit = (o) => {
    const cond = catalogos.conductores.find(c => c.id === o.id)
    setFE(p => ({
      ...p,
      conductor_id: o.id, conductor_nombre: o.label,
      conductor_cedula:  cond?.cedula  ?? p.conductor_cedula,
      conductor_celular: cond?.celular ?? p.conductor_celular,
    }))
  }

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
          style={{ background: BLUE, color: TICK }}>
          Buscar
        </button>
        {view !== 'nuevo' && (
          <button type="button" onClick={() => { setFN(NUEVO_INIT); clearPending(); setView('nuevo') }}
            className="flex items-center gap-1.5 px-4 py-2 rounded-lg text-sm font-semibold transition-colors hover:opacity-80"
            style={{ background: '#0f2e1a', color: '#86efac', border: '1px solid #166534' }}>
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

          {Object.keys(pendingEntries).length > 0 && (
            <div className="flex items-center gap-2 px-3 py-2 rounded-lg text-xs"
              style={{ background: '#1a2a0a', border: '1px solid #3a5a1a', color: '#a3e635' }}>
              <span>⏳ {Object.keys(pendingEntries).length} entrada(s) nueva(s) se guardarán al crear el manifiesto.</span>
            </div>
          )}

          <SectionCard icon={FileText} title="Identificación" cols={3}>
            <Input label="N° Manifiesto *" type="number" placeholder="12345"
              value={formNuevo.manifiesto} onChange={e => fn('manifiesto')(e.target.value)} />
            <DateInput label="Fecha despacho *"
              value={formNuevo.fecha_despacho} onChange={e => fn('fecha_despacho')(e.target.value)} />
            <Select label="Agencia" value={formNuevo.agencia_id
                ? optAgencias.find(a => a.id === formNuevo.agencia_id)?.label : ''}
              onChange={v => {
                const ag = optAgencias.find(a => a.label === v)
                setFN(p => ({ ...p, agencia_id: ag?.id ?? null }))
              }} options={optAgencias.map(a => a.label)} />
          </SectionCard>

          <SectionCard icon={User} title="Personal" cols={3}>
            <Autocomplete label="Conductor *" displayValue={formNuevo.conductor_nombre}
              placeholder="Nombre del conductor"
              options={optConductores}
              onCreate={(nombre, extras) => stageEntry('conductor', nombre, extras)}
              onSelect={selectConductorNuevo} />
            <Input label="Cédula conductor" placeholder="Número de cédula"
              value={formNuevo.conductor_cedula} onChange={e => fn('conductor_cedula')(e.target.value)} />
            <Input label="Celular conductor" placeholder="Número de celular"
              value={formNuevo.conductor_celular} onChange={e => fn('conductor_celular')(e.target.value)} />
            <Autocomplete label="Cliente *" displayValue={formNuevo.cliente_nombre}
              placeholder="Nombre del cliente"
              options={optClientes} onCreate={(nombre) => stageEntry('cliente', nombre)}
              onSelect={o => setFN(p => ({ ...p, cliente_id: o.id, cliente_nombre: o.label }))} />
            <Autocomplete label="Responsable" displayValue={formNuevo.responsable_nombre}
              placeholder="Nombre del responsable"
              options={optResponsables} onCreate={(nombre) => stageEntry('responsable', nombre)}
              onSelect={o => setFN(p => ({ ...p, responsable_id: o.id, responsable_nombre: o.label }))} />
          </SectionCard>

          <SectionCard icon={MapPin} title="Ruta" cols={3}>
            <Autocomplete label="Origen *" displayValue={formNuevo.origen_nombre}
              placeholder="Ciudad de origen"
              options={optLugares} onCreate={(nombre) => stageEntry('lugar', nombre)}
              onSelect={o => setFN(p => ({ ...p, origen_id: o.id, origen_nombre: o.label }))} />
            <Autocomplete label="Destino *" displayValue={formNuevo.destino_nombre}
              placeholder="Ciudad de destino"
              options={optLugares} onCreate={(nombre) => stageEntry('lugar', nombre)}
              onSelect={o => setFN(p => ({ ...p, destino_id: o.id, destino_nombre: o.label }))} />
            <Autocomplete label="Placa" displayValue={formNuevo.placa}
              placeholder="Placa del vehículo"
              options={optVehiculos} onCreate={(placa) => stageEntry('vehiculo', placa)}
              onSelect={o => setFN(p => ({ ...p, placa: o.label }))} />
            <Autocomplete label="Placa remolque" displayValue={formNuevo.placa_remolque}
              placeholder="Placa del remolque"
              options={optRemolques} onCreate={(placa) => stageEntry('remolque', placa)}
              onSelect={o => setFN(p => ({ ...p, placa_remolque: o.label }))} />
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
              style={{ background: BLUE, color: TICK }}>
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
              style={{ background: '#0d1f2d', color: GOLD, border: `1px solid ${BDR}` }}>
              {ficha.mes} {ficha.año}
            </span>
            <div className="flex-1" />
            {!confirmDel && (
              <>
                <button type="button" onClick={() => { setEditMode(v => !v); setTab('despacho') }}
                  className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-semibold transition-colors"
                  style={editMode
                    ? { background: '#162232', color: MUTED, border: `1px solid ${BDR}` }
                    : { background: '#0d1f35', color: BLUE,  border: `1px solid ${BLUE}` }}>
                  {editMode ? <><X size={12} /> Cancelar</> : <><Pencil size={12} /> Editar</>}
                </button>
                <button type="button" onClick={() => setConfirmDel(true)}
                  className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-semibold transition-colors"
                  style={{ background: '#2e0f0f', color: '#fca5a5', border: '1px solid #7f1d1d' }}>
                  <Trash2 size={12} /> Eliminar
                </button>
              </>
            )}
            {confirmDel && (
              <div className="flex items-center gap-2 px-3 py-1.5 rounded-lg text-xs"
                style={{ background: '#2e0f0f', border: '1px solid #7f1d1d', color: '#fca5a5' }}>
                <span>¿Eliminar manifiesto {ficha.manifiesto}? No se puede deshacer.</span>
                <button type="button" disabled={busy} onClick={handleDelete}
                  className="px-2 py-0.5 rounded font-bold transition-opacity disabled:opacity-50"
                  style={{ background: '#ef4444', color: '#fff' }}>
                  {busy ? '...' : 'Confirmar'}
                </button>
                <button type="button" onClick={() => setConfirmDel(false)}
                  className="px-2 py-0.5 rounded font-semibold hover:opacity-70"
                  style={{ color: MUTED }}>
                  Cancelar
                </button>
              </div>
            )}
          </div>

          {/* Resumen rápido */}
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
            {[
              { l: 'Conductor', v: ficha.conductor?.nombre ?? '—' },
              { l: 'Ruta',      v: ficha.origen?.nombre && ficha.destino?.nombre ? `${ficha.origen.nombre} → ${ficha.destino.nombre}` : '—' },
              { l: 'Cliente',   v: ficha.cliente?.nombre ?? '—' },
              { l: 'Agencia',   v: ficha.agencia?.nombre ?? '—' },
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
              { id: 'pagos',       label: 'Pagos' },
              { id: 'facturacion', label: 'Facturación' },
            ].map(t => (
              <button key={t.id} onClick={() => setTab(t.id)}
                className="px-4 py-1.5 rounded-md text-xs font-semibold transition-all"
                style={tab === t.id ? { background: BLUE, color: TICK } : { color: MUTED }}>
                {t.label}
              </button>
            ))}
          </div>

          {/* Tab: Despacho — readonly */}
          {tab === 'despacho' && !editMode && (() => {
            const p = ficha.pagos_conductor?.[0] ?? {}
            const f = ficha.facturacion?.[0]    ?? {}
            const estadoColor = {
              'PAGADO': '#22c55e', 'ANULADO': '#ef4444',
              'PRIORITARIO': '#f97316', 'RNDC': '#a855f7',
            }[p.estado] ?? TICK
            return (
              <div className="flex flex-col gap-4">
                <div>
                  <p className="text-[10px] font-bold uppercase tracking-widest mb-2" style={{ color: MUTED }}>Despacho</p>
                  <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-2">
                    {[
                      { l: 'Manifiesto',      v: ficha.manifiesto },
                      { l: 'Fecha despacho',  v: ficha.fecha_despacho },
                      { l: 'Mes / Año',       v: `${ficha.mes} ${ficha.año}` },
                      { l: 'Conductor',       v: ficha.conductor?.nombre },
                      { l: 'Cédula',          v: ficha.conductor?.cedula },
                      { l: 'Celular',         v: ficha.conductor?.celular },
                      { l: 'Placa',           v: ficha.placa },
                      { l: 'Placa remolque',  v: ficha.placa_remolque },
                      { l: 'Cliente',         v: ficha.cliente?.nombre },
                      { l: 'Origen',          v: ficha.origen?.nombre },
                      { l: 'Destino',         v: ficha.destino?.nombre },
                      { l: 'Agencia',         v: ficha.agencia?.nombre },
                      { l: 'Responsable',     v: ficha.responsable?.nombre },
                      { l: 'Valor remesa',    v: ficha.valor_remesa    != null ? `$${Number(ficha.valor_remesa).toLocaleString('es-CO')}` : null },
                      { l: 'Flete conductor', v: ficha.flete_conductor != null ? `$${Number(ficha.flete_conductor).toLocaleString('es-CO')}` : null },
                      { l: 'Anticipo',        v: ficha.anticipo        != null ? `$${Number(ficha.anticipo).toLocaleString('es-CO')}` : null },
                      { l: 'Remesas',         v: ficha.remesas?.map(r => r.codigo_remesa).join('; ') || null, col: 2 },
                    ].map(({ l, v, col }) => (
                      <div key={l} className="rounded-lg px-3 py-2.5"
                        style={{ background: BG, border: `1px solid ${BDR}`, gridColumn: col ? `span ${col}` : undefined }}>
                        <p className="text-[10px] font-bold uppercase tracking-wider mb-0.5" style={{ color: MUTED }}>{l}</p>
                        <p className="text-sm" style={{ color: TICK }}>{v ?? '—'}</p>
                      </div>
                    ))}
                  </div>
                </div>
                <div>
                  <p className="text-[10px] font-bold uppercase tracking-widest mb-2" style={{ color: MUTED }}>Estado de seguimiento</p>
                  <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-2">
                    <div className="rounded-lg px-3 py-2.5" style={{ background: BG, border: `1px solid ${BDR}` }}>
                      <p className="text-[10px] font-bold uppercase tracking-wider mb-0.5" style={{ color: MUTED }}>Estado pago</p>
                      <p className="text-sm font-semibold" style={{ color: p.estado ? estadoColor : MUTED }}>
                        {p.estado ?? '—'}
                      </p>
                    </div>
                    {[
                      { l: 'Condición pago',    v: p.condicion_pago },
                      { l: 'Fecha cumplido',    v: p.fecha_cumplido },
                      { l: 'Fecha pago',        v: p.fecha_pago },
                      { l: 'Valor pagado',      v: p.valor_pagado != null ? `$${Number(p.valor_pagado).toLocaleString('es-CO')}` : null },
                      { l: 'Entidad financiera',v: p.entidad_financiera },
                      { l: 'N° Factura',        v: f.factura_no },
                      { l: 'Estado interno',    v: f.estado_interno },
                    ].map(({ l, v }) => (
                      <div key={l} className="rounded-lg px-3 py-2.5" style={{ background: BG, border: `1px solid ${BDR}` }}>
                        <p className="text-[10px] font-bold uppercase tracking-wider mb-0.5" style={{ color: MUTED }}>{l}</p>
                        <p className="text-sm" style={{ color: v ? TICK : MUTED }}>{v ?? '—'}</p>
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            )
          })()}

          {/* Tab: Despacho — edit mode */}
          {tab === 'despacho' && editMode && (
            <form onSubmit={handleUpdate} className="flex flex-col gap-4">
              {Object.keys(pendingEntries).length > 0 && (
                <div className="flex items-center gap-2 px-3 py-2 rounded-lg text-xs"
                  style={{ background: '#1a2a0a', border: '1px solid #3a5a1a', color: '#a3e635' }}>
                  <span>⏳ {Object.keys(pendingEntries).length} entrada(s) nueva(s) se guardarán al actualizar.</span>
                </div>
              )}
              <SectionCard icon={FileText} title="Identificación" cols={3}>
                <Input label="N° Manifiesto" value={ficha.manifiesto} readOnly
                  style={{ borderColor: BDR, opacity: 0.5, cursor: 'not-allowed' }} />
                <DateInput label="Fecha despacho *"
                  value={formEdit.fecha_despacho} onChange={e => fe('fecha_despacho')(e.target.value)} />
                <Select label="Agencia"
                  value={formEdit.agencia_id ? optAgencias.find(a => a.id === formEdit.agencia_id)?.label : ''}
                  onChange={v => { const ag = optAgencias.find(a => a.label === v); setFE(p => ({ ...p, agencia_id: ag?.id ?? null })) }}
                  options={optAgencias.map(a => a.label)} />
              </SectionCard>
              <SectionCard icon={User} title="Personal" cols={3}>
                <Autocomplete label="Conductor *" displayValue={formEdit.conductor_nombre}
                  placeholder="Nombre del conductor" options={optConductores}
                  onCreate={(nombre, extras) => stageEntry('conductor', nombre, extras)}
                  onSelect={selectConductorEdit} />
                <Input label="Cédula conductor" placeholder="Número de cédula"
                  value={formEdit.conductor_cedula ?? ''} onChange={e => fe('conductor_cedula')(e.target.value)} />
                <Input label="Celular conductor" placeholder="Número de celular"
                  value={formEdit.conductor_celular ?? ''} onChange={e => fe('conductor_celular')(e.target.value)} />
                <Autocomplete label="Cliente *" displayValue={formEdit.cliente_nombre}
                  placeholder="Nombre del cliente" options={optClientes} onCreate={(n) => stageEntry('cliente', n)}
                  onSelect={o => setFE(p => ({ ...p, cliente_id: o.id, cliente_nombre: o.label }))} />
                <Autocomplete label="Responsable" displayValue={formEdit.responsable_nombre}
                  placeholder="Responsable" options={optResponsables} onCreate={(n) => stageEntry('responsable', n)}
                  onSelect={o => setFE(p => ({ ...p, responsable_id: o.id, responsable_nombre: o.label }))} />
              </SectionCard>
              <SectionCard icon={MapPin} title="Ruta" cols={3}>
                <Autocomplete label="Origen *" displayValue={formEdit.origen_nombre}
                  placeholder="Ciudad de origen" options={optLugares} onCreate={(n) => stageEntry('lugar', n)}
                  onSelect={o => setFE(p => ({ ...p, origen_id: o.id, origen_nombre: o.label }))} />
                <Autocomplete label="Destino *" displayValue={formEdit.destino_nombre}
                  placeholder="Ciudad de destino" options={optLugares} onCreate={(n) => stageEntry('lugar', n)}
                  onSelect={o => setFE(p => ({ ...p, destino_id: o.id, destino_nombre: o.label }))} />
                <Autocomplete label="Placa" displayValue={formEdit.placa}
                  placeholder="Placa del vehículo" options={optVehiculos} onCreate={(n) => stageEntry('vehiculo', n)}
                  onSelect={o => setFE(p => ({ ...p, placa: o.label }))} />
                <Autocomplete label="Placa remolque" displayValue={formEdit.placa_remolque}
                  placeholder="Placa del remolque" options={optRemolques} onCreate={(n) => stageEntry('remolque', n)}
                  onSelect={o => setFE(p => ({ ...p, placa_remolque: o.label }))} />
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
                  style={{ background: '#162232', color: GOLD, border: `1px solid ${GOLD}44` }}>
                  <RotateCcw size={13} /> Restablecer
                </button>
                <button type="button" onClick={() => setEditMode(false)}
                  className="px-4 py-2 rounded-lg text-sm font-semibold"
                  style={{ background: '#162232', color: MUTED, border: `1px solid ${BDR}` }}>
                  Cancelar
                </button>
                <button type="submit" disabled={busy}
                  className="flex items-center gap-2 px-6 py-2.5 rounded-lg text-sm font-semibold disabled:opacity-50"
                  style={{ background: BLUE, color: TICK }}>
                  <Save size={14} /> {busy ? 'Guardando...' : 'Guardar cambios'}
                </button>
              </div>
            </form>
          )}

          {/* Tab: Pagos */}
          {tab === 'pagos' && (
            <form onSubmit={handleSavePagos} className="flex flex-col gap-4">
              <SectionCard icon={DollarSign} title="Estado del pago" cols={3}>
                <Select label="Estado" value={formPagos.estado} onChange={fp('estado')} options={ESTADO_PAGO_OPTS} />
                <Select label="Condición de pago" value={formPagos.condicion_pago} onChange={fp('condicion_pago')} options={CONDICION_PAGO_OPTS} />
                <Autocomplete label="Responsable" displayValue={formPagos.responsable_nombre}
                  placeholder="Responsable del pago"
                  options={optResponsables} onCreate={(n) => stageEntry('responsable', n)}
                  onSelect={o => setFP(p => ({ ...p, responsable_id: o.id, responsable_nombre: o.label }))} />
                <DateInput label="Fecha cumplido"
                  value={formPagos.fecha_cumplido} onChange={e => fp('fecha_cumplido')(e.target.value)} />
                <DateInput label="Fecha pago"
                  value={formPagos.fecha_pago} onChange={e => fp('fecha_pago')(e.target.value)} />
                <MoneyInput label="Valor pagado"
                  value={formPagos.valor_pagado} onChange={e => fp('valor_pagado')(e.target.value)} />
                <Select label="Entidad financiera" value={formPagos.entidad_financiera}
                  onChange={fp('entidad_financiera')} options={ENTIDAD_FIN_OPTS} />
              </SectionCard>
              <SectionCard icon={ClipboardList} title="Novedades" cols={1}>
                <Field label="Novedades" col={1}>
                  <textarea rows={3} className={inputCls} style={{ borderColor: BDR, resize: 'vertical' }}
                    placeholder="Observaciones o novedades del viaje..."
                    value={formPagos.novedades} onChange={e => fp('novedades')(e.target.value)} />
                </Field>
              </SectionCard>
              <div className="flex justify-end">
                <button type="submit" disabled={busy}
                  className="flex items-center gap-2 px-6 py-2.5 rounded-lg text-sm font-semibold disabled:opacity-50"
                  style={{ background: BLUE, color: TICK }}>
                  <Save size={14} /> {busy ? 'Guardando...' : 'Guardar pagos'}
                </button>
              </div>
            </form>
          )}

          {/* Tab: Facturación */}
          {tab === 'facturacion' && (
            <form onSubmit={handleSaveFact} className="flex flex-col gap-4">
              <SectionCard icon={FileText} title="Facturación" cols={3}>
                <Input label="N° Factura" placeholder="FE-0001"
                  value={formFact.factura_no} onChange={e => ff('factura_no')(e.target.value)} />
                <DateInput label="Fecha factura"
                  value={formFact.fecha_factura} onChange={e => ff('fecha_factura')(e.target.value)} />
                <Input label="Días para facturar" type="number" placeholder="0"
                  value={formFact.dias_para_facturar} onChange={e => ff('dias_para_facturar')(e.target.value)} />
                <Input label="Mes facturación (1-12)" type="number" min={1} max={12}
                  value={formFact.mes_facturacion} onChange={e => ff('mes_facturacion')(e.target.value)} />
                <Select label="Estado interno" value={formFact.estado_interno}
                  onChange={ff('estado_interno')} options={ESTADO_INTERNO_OPTS} />
                <Autocomplete label="Responsable" displayValue={formFact.responsable_nombre}
                  placeholder="Responsable de facturación"
                  options={optResponsables} onCreate={(n) => stageEntry('responsable', n)}
                  onSelect={o => setFF(p => ({ ...p, responsable_id: o.id, responsable_nombre: o.label }))} />
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
                  style={{ background: BLUE, color: TICK }}>
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
          <button
            type="button"
            onClick={() => setRecentesOpen(v => !v)}
            className="flex items-center gap-2 w-fit transition-opacity hover:opacity-70"
          >
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
            {recentesOpen
              ? <ChevronUp size={12} style={{ color: MUTED }} />
              : <ChevronDown size={12} style={{ color: MUTED }} />
            }
          </button>

          {recentesOpen && (
            <div className="rounded-xl overflow-hidden" style={{ border: `1px solid ${BDR}` }}>
              <div className="grid grid-cols-[80px_110px_1fr_1fr_100px] px-4 py-2"
                style={{ background: '#0D1B2A', borderBottom: `1px solid ${BDR}` }}>
                {['N°', 'Fecha', 'Conductor', 'Ruta', 'Período'].map(h => (
                  <span key={h} className="text-[10px] font-bold uppercase tracking-widest"
                    style={{ color: MUTED }}>{h}</span>
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
                    className="grid grid-cols-[80px_110px_1fr_1fr_100px] w-full px-4 py-2.5 text-left transition-colors hover:bg-white/5"
                    style={{
                      background: esMio ? 'rgba(201,168,76,0.06)' : i % 2 === 0 ? BG : 'transparent',
                      borderTop: i > 0 ? `1px solid ${BDR}` : 'none',
                    }}>
                    <span className="text-sm font-bold" style={{ color: esMio ? GOLD : BLUE }}>
                      {r.manifiesto}
                      {esMio && <span className="ml-1 text-[9px]">★</span>}
                    </span>
                    <span className="text-xs" style={{ color: MUTED }}>{r.fecha_despacho}</span>
                    <span className="text-xs truncate pr-2" style={{ color: TICK }}>{r.conductor?.nombre ?? '—'}</span>
                    <span className="text-xs truncate pr-2" style={{ color: MUTED }}>
                      {r.origen?.nombre && r.destino?.nombre ? `${r.origen.nombre} → ${r.destino.nombre}` : '—'}
                    </span>
                    <span className="text-xs" style={{ color: esMio ? GOLD : MUTED }}>
                      {r.mes} {r.año}
                    </span>
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
