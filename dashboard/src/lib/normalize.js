// Helpers puros para normalizar/comparar valores entre Excel/DB.
// Extraídos de CargaPage para poder testearse en aislado.

export const NUMERIC_FIELDS = new Set(['valor_remesa', 'flete_conductor', 'anticipo', 'reteica', 'r_fopat'])

// Normaliza un valor para compararlo con el equivalente en DB.
// Los NUMERIC(14,2) llegan desde PostgREST como "500000.00" mientras que el
// payload tiene Number(500000) → comparar strings sin más daría falsos diffs.
//   - Numéricos: round → entero → string. 0 se trata como null (ausencia de
//     valor) para evitar falsos diffs entre DB con 0 y Excel con celda vacía.
//   - Strings: trim simple.
export function normalizeVal(v, field) {
  if (v == null || v === '') return null
  if (NUMERIC_FIELDS.has(field)) {
    const n = Number(v)
    if (isNaN(n) || n === 0) return null
    return String(Math.round(n))
  }
  return String(v).trim()
}

export function removeAccents(s) {
  if (!s) return s
  const placeholderLower = '\x00'
  const placeholderUpper = '\x01'
  return s.replace(/ñ/g, placeholderLower).replace(/Ñ/g, placeholderUpper)
    .normalize('NFD').replace(/\p{Mn}/gu, '')
    .replace(new RegExp(placeholderLower, 'g'), 'ñ')
    .replace(new RegExp(placeholderUpper, 'g'), 'Ñ')
}
