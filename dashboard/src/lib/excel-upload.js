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
}

function normalizeResponsable(s) {
  if (s == null) return null
  const up = String(s).trim().toUpperCase()
  if (!up) return null
  return RESPONSABLE_FIXES[up] ?? up
}

const MESES_ARR = ['ENERO','FEBRERO','MARZO','ABRIL','MAYO','JUNIO',
                   'JULIO','AGOSTO','SEPTIEMBRE','OCTUBRE','NOVIEMBRE','DICIEMBRE']

// Convierte valores numéricos del Excel. Soporta:
//   - Separadores de miles con coma o punto: "1,420,000" / "1.420.000"
//   - Decimales con coma: "1.420,50"
//   - Multi-valor separado por ";" (multi-remesa): "250,000; 250,000" → suma
export function toNum(v) {
  if (v == null) return null
  if (typeof v === 'number') return v || null
  const s = String(v).trim()
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
// Devuelve { fila, payload } o { fila, error }.
export function buildPayload(r, i) {
  const manifiesto = Number(String(r['MANIFIESTO'] ?? '').trim())
  if (!manifiesto || isNaN(manifiesto)) {
    return { fila: i + 2, error: 'Sin número de manifiesto válido' }
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
      p_cliente:              r['GENERADORES']    != null ? String(r['GENERADORES']).split(';')[0].trim() : null,
      p_conductor:            r['CONDUCTOR']      != null ? String(r['CONDUCTOR']).trim()                 : null,
      p_cedula_conductor:     r['DOC. CONDUCTOR'] != null ? String(r['DOC. CONDUCTOR']).trim()            : null,
      p_celular:              r['TEL. CONDUCTOR'] != null ? String(r['TEL. CONDUCTOR']).trim()            : null,
      p_placa:                r['PLACA']          != null ? String(r['PLACA']).trim()                    : null,
      p_tipo_vehiculo:        r['REMOLQUE']       != null ? String(r['REMOLQUE']).trim()                 : null,
      p_propietario:          (r['POSEEDOR'] ?? r['PROPIETARIO']) != null
                                ? String(r['POSEEDOR'] ?? r['PROPIETARIO']).trim()
                                : null,
      p_agencia_despachadora: r['AGENCIA']        != null ? String(r['AGENCIA']).trim()                  : null,
      p_nombre_responsable:   normalizeResponsable(r['CREADO POR']),
      p_valor_remesa:         toNum(r['VALORES REMESAS']),
      p_flete_conductor:      toNum(r['FLETE']),
      p_anticipo:             toNum(r['ANTICIPO']),
      p_remesas:              r['REMESAS']        != null ? String(r['REMESAS']).trim()                  : null,
    },
  }
}
