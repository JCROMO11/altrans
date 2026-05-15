// Parsing de "CIUDAD (DEPTO)" → { ciudad, depto }.
// Extraído de useManifiesto.js para testearse en aislado y reusar.

import { removeAccents } from './normalize'

export const DEPT_ABBREV = {
  'Anti': 'Antioquia',    'Atla': 'Atlantico',      'Bogo': 'Bogota D.C.',
  'Boli': 'Bolivar',      'Boya': 'Boyaca',          'Cald': 'Caldas',
  'Casa': 'Casanare',     'Cauc': 'Cauca',            'Cesa': 'Cesar',
  'Cord': 'Cordoba',      'Cund': 'Cundinamarca',     'Huil': 'Huila',
  'La G': 'La Guajira',   'Magd': 'Magdalena',        'Meta': 'Meta',
  'Nari': 'Nariño',       'Nort': 'Norte de Santander','Quin': 'Quindio',
  'Risa': 'Risaralda',    'Sant': 'Santander',        'Toli': 'Tolima',
  'Vall': 'Valle del Cauca','Arau': 'Arauca',          'Caqu': 'Caqueta',
  'Guav': 'Guaviare',     'Putu': 'Putumayo',         'Sucr': 'Sucre',
}

export const CITY_DEPT_FALLBACK = {
  'IPIALES': 'Nariño',            'PASTO': 'Nariño',
  'RIOHACHA': 'La Guajira',       'AGUSTIN CODAZZI': 'Cesar',
  'BELLO': 'Antioquia',           'BOGOTA BOGOTA D. C.': 'Bogota D.C.',
  'CALI': 'Valle del Cauca',      'CARTAGENA': 'Bolivar',
  'ESPINAL': 'Tolima',            'GARZON': 'Huila',
  'GIRARDOTA': 'Antioquia',       'GUACHUCAL': 'Nariño',
  'IBAGUE': 'Tolima',             'LA PLATA': 'Huila',
  'MONTELIBANO': 'Cordoba',       'MOSQUERA': 'Cundinamarca',
  'PEREIRA': 'Risaralda',         'RIONEGRO': 'Antioquia',
  'PALMIRA': 'Valle del Cauca',   'TOTORO': 'Cauca',
  'BUCARAMANGA': 'Santander',     'MEDELLIN': 'Antioquia',
  'BOGOTA': 'Bogota D.C.',        'BOGOTA, D.C.': 'Bogota D.C.',
  'BOGOTA D.C.': 'Bogota D.C.',   'BOGOTA D. C.': 'Bogota D.C.',
  'BARRANQUILLA': 'Atlantico',    'MANIZALES': 'Caldas',
  'ARMENIA': 'Quindio',           'VILLAVICENCIO': 'Meta',
  'SANTIAGO DE CALI': 'Valle del Cauca',
  'CUCUTA': 'Norte de Santander', 'NEIVA': 'Huila',
  'MONTERIA': 'Cordoba',          'VALLEDUPAR': 'Cesar',
  'POPAYAN': 'Cauca',             'SINCELEJO': 'Sucre',
  'FLORENCIA': 'Caqueta',         'TUNJA': 'Boyaca',
  'QUIBDO': 'Choco',              'YOPAL': 'Casanare',
  'ARAUCA': 'Arauca',
}

export function parseCiudad(v) {
  if (!v) return { ciudad: null, depto: null }
  const s = removeAccents(String(v).trim())
  const m = s.match(/^(.*?)\s*\(([^)]+)\)\s*$/)
  if (m) {
    const ciudad = m[1].trim()
    const abrev  = m[2].trim()
    const depto  = DEPT_ABBREV[abrev] ?? removeAccents(abrev)
    return { ciudad, depto }
  }
  const key = s.toUpperCase().replace(/\s+/g, ' ')
  return { ciudad: s, depto: CITY_DEPT_FALLBACK[key] ?? null }
}
