// Helpers de parsing Excel para CargaPage.
// Extraídos a lib/ para poder testearse en aislado.

import { parseCiudad } from './geography'

// Normalización de responsables — mismas reglas que aplica el ETL Python
// (cleaning_individual.py → _PERSON_COL_FIXES.nombre_responsable). Mantener
// sincronizado para que el Excel y la DB queden idénticos sin falsos diffs.
const RESPONSABLE_FIXES = {
  'OPERATIVO3':     'OPERATIVO 3',
  'OPERAIVO 3':     'OPERATIVO 3',
  'LILIANAOBREGON': 'LILIANA OBREGON',
  'VANESA':         'VANESSA',
  ',':              null,
}

function normalizeResponsable(s) {
  if (s == null) return null
  const up = String(s).trim().toUpperCase()
  if (!up) return null
  const fixed = RESPONSABLE_FIXES[up]
  return fixed === undefined ? up : fixed
}

const MESES_ARR = ['ENERO','FEBRERO','MARZO','ABRIL','MAYO','JUNIO',
                   'JULIO','AGOSTO','SEPTIEMBRE','OCTUBRE','NOVIEMBRE','DICIEMBRE']

const trimOrNull = (v) => v != null ? String(v).trim() : null

const cleanCelular = (v) => {
  const s = trimOrNull(v)
  if (!s) return null
  const digits = s.replace(/\D/g, '')
  return digits.length === 10 ? digits : null
}

// Convierte valores numéricos del Excel. Soporta:
//   - Separadores de miles con coma o punto: "1,420,000" / "1.420.000"
//   - Decimales con coma: "1.420,50"
//   - Multi-valor separado por ";" (multi-remesa): "250,000; 250,000" → suma
export function toNum(v) {
  if (v == null) return null
  if (typeof v === 'number') return v || null
  let s = String(v).trim()
  if (!s) return null
  s = s.replace(/[^0-9.,;-]/g, '')
  if (!s) return null
  if (s.includes(';')) {
    const suma = s.split(';').reduce((acc, part) => acc + (toNum(part.trim()) ?? 0), 0)
    return suma || null
  }
  const conComa = s.includes(',')
  const conPunto = s.includes('.')
  let limpio
  if (conComa && conPunto) {
    const lastComa  = s.lastIndexOf(',')
    const lastPunto = s.lastIndexOf('.')
    if (lastComa > lastPunto) {
      limpio = s.replace(/\./g, '').replace(',', '.')
    } else {
      limpio = s.replace(/,/g, '')
    }
  } else if (conComa) {
    const partes = s.split(',')
    limpio = partes.length > 1 && partes[partes.length - 1].length === 2
      ? s.replace(/,(?=.*,)/, '').replace(',', '.')
      : s.replace(/,/g, '')
  } else if (conPunto) {
    const partes = s.split('.')
    limpio = partes.length > 1 && partes[partes.length - 1].length === 2
      ? s
      : s.replace(/\./g, '')
  } else {
    limpio = s
  }
  const n = Number(limpio)
  return isNaN(n) ? null : n || null
}

// Convierte serial Excel o string fecha a "YYYY-MM-DD".
export function parseFecha(v) {
  if (!v) return null
  if (v instanceof Date) return v.toISOString().slice(0, 10)
  if (typeof v === 'number') {
    const d = new Date(Math.round((v - 25569) * 86400 * 1000))
    return d.toISOString().slice(0, 10)
  }
  if (typeof v === 'string') return v.slice(0, 10)
  return null
}

// Construye el payload para guardar_digitador desde una fila de Excel.
// Devuelve { fila, payload } o { fila, campo, valor, error }.
export function buildPayload(r, i, fileName = null) {
  const manifiesto = Number(String(r['MANIFIESTO'] ?? '').trim())
  if (!manifiesto || isNaN(manifiesto)) {
    const raw = String(r['MANIFIESTO'] ?? '').trim()
    return {
      fila:   i + 2,
      campo:  'MANIFIESTO',
      valor:  raw || '(vacío)',
      error:  'Sin número de manifiesto válido',
    }
  }

  const fecha = parseFecha(r['FECHA EMISIÓN'])
  const mes   = fecha ? MESES_ARR[new Date(fecha + 'T12:00:00').getMonth()] : null
  const año   = fecha ? new Date(fecha + 'T12:00:00').getFullYear() : null
  const periodo = fecha && año
    ? `${año}-${String(new Date(fecha + 'T12:00:00').getMonth() + 1).padStart(2, '0')}-01`
    : null

  const { ciudad: origen,  depto: dpto_origen }  = parseCiudad(r['ORIGEN'])
  const { ciudad: destino, depto: dpto_destino } = parseCiudad(r['DESTINO'])

  return {
    fila: i + 2,
    payload: {
      p_manifiesto:           manifiesto,
      p_fecha_despacho:       fecha,
      p_mes:                  mes,
      p_año:                  año,
      p_periodo:              periodo,
      p_origen:               origen,
      p_departamento_origen:  dpto_origen,
      p_destino:              destino,
      p_departamento_destino: dpto_destino,
      p_cliente:              r['GENERADORES'] != null ? String(r['GENERADORES']).split(';')[0].trim() : null,
      p_conductor:            trimOrNull(r['CONDUCTOR']),
      p_cedula_conductor:     trimOrNull(r['DOC. CONDUCTOR']),
      p_celular:              cleanCelular(r['TEL. CONDUCTOR']),
      p_placa:                trimOrNull(r['PLACA']),
      p_placa_remolque:              trimOrNull(r['REMOLQUE']),
      p_propietario:          trimOrNull(r['POSEEDOR'] ?? r['PROPIETARIO']),
      p_agencia_despachadora: trimOrNull(r['AGENCIA']),
      p_nombre_responsable:   normalizeResponsable(r['CREADO POR']),
      p_valor_remesa:         toNum(r['VALORES REMESAS']),
      p_flete_conductor:      toNum(r['FLETE']),
      p_anticipo:             toNum(r['ANTICIPO']),
      p_reteica:              toNum(r['RETEICA']),
      p_r_fopat:              toNum(r['R. FOPAT']),
      p_remesas:              trimOrNull(r['REMESAS']),
      p_archivo_origen:       fileName,
    },
  }
}

// ── Validaciones matching DB CHECK constraints ──────────────────────────────

const PLACA_RE = /^[A-Z0-9]{4,7}$/
const PLACA_ALLOWLIST = new Set(['ANULADO', 'CONS ANULADO'])
const CEDULA_RE = /^\d{6,10}$/

export function validatePlaca(v) {
  if (!v) return null
  const s = String(v).trim().toUpperCase()
  if (!s || PLACA_ALLOWLIST.has(s) || PLACA_RE.test(s)) return null
  return `Formato de placa inválido: "${s}" (deben ser 4-7 caracteres alfanuméricos)`
}

export function validateCedula(v) {
  if (!v) return null
  const s = String(v).trim()
  if (!s) return null
  if (CEDULA_RE.test(s)) return null
  return `Cédula inválida: "${s}" (deben ser 6-10 dígitos)`
}

export function validateAño(v) {
  if (v == null) return null
  const n = Number(v)
  if (isNaN(n)) return `Año inválido: "${v}"`
  if (n >= 2023 && n <= 2026) return null
  return `Año fuera de rango: ${n} (debe ser 2023-2026)`
}

export function validateMonto(v, label) {
  if (v == null) return null
  const n = Number(v)
  if (isNaN(n)) return `${label} inválido: "${v}"`
  if (n >= 0) return null
  return `${label} no puede ser negativo: ${n}`
}

export function validatePayload(payload) {
  const errors = []
  const e = payload
  if (e.p_placa) {
    const err = validatePlaca(e.p_placa)
    if (err) errors.push({ campo: 'PLACA', valor: e.p_placa, error: err })
  }
  if (e.p_cedula_conductor) {
    const err = validateCedula(e.p_cedula_conductor)
    if (err) errors.push({ campo: 'DOC. CONDUCTOR', valor: e.p_cedula_conductor, error: err })
  }
  if (e.p_año) {
    const err = validateAño(e.p_año)
    if (err) errors.push({ campo: 'AÑO', valor: e.p_año, error: err })
  }
  if (e.p_valor_remesa != null) {
    const err = validateMonto(e.p_valor_remesa, 'VALOR REMESA')
    if (err) errors.push({ campo: 'VALORES REMESAS', valor: e.p_valor_remesa, error: err })
  }
  if (e.p_flete_conductor != null) {
    const err = validateMonto(e.p_flete_conductor, 'FLETE')
    if (err) errors.push({ campo: 'FLETE', valor: e.p_flete_conductor, error: err })
  }
  if (e.p_anticipo != null) {
    const err = validateMonto(e.p_anticipo, 'ANTICIPO')
    if (err) errors.push({ campo: 'ANTICIPO', valor: e.p_anticipo, error: err })
  }
  return errors.length ? errors : null
}
