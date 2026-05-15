import { describe, it, expect } from 'vitest'
import { toNum, parseFecha, buildPayload } from '../lib/excel-upload'

// ── toNum ────────────────────────────────────────────────────────────────────
describe('toNum', () => {
  it('null/undefined/string vacío → null', () => {
    expect(toNum(null)).toBe(null)
    expect(toNum(undefined)).toBe(null)
    expect(toNum('')).toBe(null)
    expect(toNum('   ')).toBe(null)
  })

  it('number directo (no-cero) se preserva', () => {
    expect(toNum(1234.5)).toBe(1234.5)
    expect(toNum(1000000)).toBe(1000000)
  })

  it('0 → null (regla del ETL: cero se trata como ausencia)', () => {
    expect(toNum(0)).toBe(null)
    expect(toNum('0')).toBe(null)
  })

  it('formato colombiano "1.420.000" (punto como miles)', () => {
    expect(toNum('1.420.000')).toBe(1420000)
    expect(toNum('500.000')).toBe(500000)
  })

  it('formato americano "1,420,000" (coma como miles)', () => {
    expect(toNum('1,420,000')).toBe(1420000)
  })

  it('decimal con coma "1420,50"', () => {
    expect(toNum('1420,50')).toBe(1420.50)
  })

  it('decimal con punto "1420.50"', () => {
    expect(toNum('1420.50')).toBe(1420.5)
  })

  it('mixto "1.420.000,50" (punto miles + coma decimal)', () => {
    expect(toNum('1.420.000,50')).toBe(1420000.5)
  })

  it('mixto "1,420,000.50" (coma miles + punto decimal)', () => {
    expect(toNum('1,420,000.50')).toBe(1420000.5)
  })

  it('multi-valor con ";" se suma', () => {
    expect(toNum('250000;250000')).toBe(500000)
    expect(toNum('250.000; 250.000')).toBe(500000)
    expect(toNum('100000;200000;300000')).toBe(600000)
  })

  it('valor inválido → null', () => {
    expect(toNum('abc')).toBe(null)
    expect(toNum('!!')).toBe(null)
  })
})

// ── parseFecha ───────────────────────────────────────────────────────────────
describe('parseFecha', () => {
  it('null/falsy → null', () => {
    expect(parseFecha(null)).toBe(null)
    expect(parseFecha(undefined)).toBe(null)
    expect(parseFecha('')).toBe(null)
    expect(parseFecha(0)).toBe(null)
  })

  it('Date instance → ISO yyyy-mm-dd', () => {
    expect(parseFecha(new Date('2026-05-14T10:00:00Z'))).toBe('2026-05-14')
  })

  it('serial Excel → ISO yyyy-mm-dd', () => {
    // 45000 = 2023-03-15 aprox
    const r = parseFecha(45000)
    expect(r).toMatch(/^\d{4}-\d{2}-\d{2}$/)
    // verifica que es razonablemente cercano (ajuste de zona horaria puede dar ±1)
    expect(['2023-03-14','2023-03-15','2023-03-16']).toContain(r)
  })

  it('string ISO → yyyy-mm-dd', () => {
    expect(parseFecha('2026-05-14')).toBe('2026-05-14')
    expect(parseFecha('2026-05-14T12:00:00')).toBe('2026-05-14')
  })
})

// ── buildPayload ─────────────────────────────────────────────────────────────
describe('buildPayload', () => {
  it('fila sin MANIFIESTO devuelve error', () => {
    const r = buildPayload({ ORIGEN: 'CALI' }, 0)
    expect(r.error).toBe('Sin número de manifiesto válido')
    expect(r.fila).toBe(2)
    expect(r.payload).toBeUndefined()
  })

  it('MANIFIESTO no numérico devuelve error', () => {
    const r = buildPayload({ MANIFIESTO: 'ABC' }, 5)
    expect(r.error).toBeDefined()
    expect(r.fila).toBe(7)  // i+2 (i=5 → fila 7 contando header)
  })

  it('fila válida mínima → payload con manifiesto y campos null donde falta', () => {
    const r = buildPayload({ MANIFIESTO: '12345' }, 0)
    expect(r.payload.p_manifiesto).toBe(12345)
    expect(r.payload.p_fecha_despacho).toBe(null)
    expect(r.payload.p_mes).toBe(null)
    expect(r.payload.p_año).toBe(null)
    expect(r.payload.p_origen).toBe(null)
    expect(r.payload.p_conductor).toBe(null)
  })

  it('deriva mes/año/periodo desde FECHA EMISIÓN', () => {
    const r = buildPayload({ MANIFIESTO: '1', 'FECHA EMISIÓN': '2026-05-14' }, 0)
    expect(r.payload.p_fecha_despacho).toBe('2026-05-14')
    expect(r.payload.p_mes).toBe('MAYO')
    expect(r.payload.p_año).toBe(2026)
    expect(r.payload.p_periodo).toBe('2026-05-01')
  })

  it('parsea ORIGEN/DESTINO con formato "CIUDAD (Abrev)"', () => {
    const r = buildPayload({
      MANIFIESTO: '1',
      'ORIGEN': 'BUENAVENTURA(Vall)',
      'DESTINO': 'BOGOTA(Cund)',
    }, 0)
    expect(r.payload.p_origen).toBe('BUENAVENTURA')
    expect(r.payload.p_departamento_origen).toBe('Valle del Cauca')
    expect(r.payload.p_destino).toBe('BOGOTA')
    expect(r.payload.p_departamento_destino).toBe('Cundinamarca')
  })

  it('GENERADORES con ";" → toma solo el primero', () => {
    const r = buildPayload({
      MANIFIESTO: '1',
      'GENERADORES': 'CLIENTE A; CLIENTE B; CLIENTE C',
    }, 0)
    expect(r.payload.p_cliente).toBe('CLIENTE A')
  })

  it('propietario: POSEEDOR tiene prioridad sobre PROPIETARIO (bug real)', () => {
    const r = buildPayload({
      MANIFIESTO: '1',
      'POSEEDOR':    'JUAN POSEEDOR',
      'PROPIETARIO': 'PEDRO PROPIETARIO',
    }, 0)
    expect(r.payload.p_propietario).toBe('JUAN POSEEDOR')
  })

  it('propietario: si no hay POSEEDOR usa PROPIETARIO', () => {
    const r = buildPayload({ MANIFIESTO: '1', 'PROPIETARIO': 'PEDRO' }, 0)
    expect(r.payload.p_propietario).toBe('PEDRO')
  })

  it('propietario: ambos ausentes → null', () => {
    const r = buildPayload({ MANIFIESTO: '1' }, 0)
    expect(r.payload.p_propietario).toBe(null)
  })

  it('CREADO POR se uppercasea', () => {
    const r = buildPayload({ MANIFIESTO: '1', 'CREADO POR': 'maria perez' }, 0)
    expect(r.payload.p_nombre_responsable).toBe('MARIA PEREZ')
  })

  it('normaliza responsables: OPERATIVO3 → "OPERATIVO 3" (matches ETL)', () => {
    // El ETL Python convierte OPERATIVO3 → "OPERATIVO 3" en la DB.
    // El frontend debe hacer lo mismo al subir Excel para evitar falsos diffs.
    expect(buildPayload({ MANIFIESTO: '1', 'CREADO POR': 'OPERATIVO3' }, 0)
      .payload.p_nombre_responsable).toBe('OPERATIVO 3')
    expect(buildPayload({ MANIFIESTO: '1', 'CREADO POR': 'operativo3' }, 0)
      .payload.p_nombre_responsable).toBe('OPERATIVO 3')
    expect(buildPayload({ MANIFIESTO: '1', 'CREADO POR': 'LILIANAOBREGON' }, 0)
      .payload.p_nombre_responsable).toBe('LILIANA OBREGON')
    expect(buildPayload({ MANIFIESTO: '1', 'CREADO POR': 'VANESA' }, 0)
      .payload.p_nombre_responsable).toBe('VANESSA')
  })

  it('responsable vacío/null → null', () => {
    expect(buildPayload({ MANIFIESTO: '1' }, 0).payload.p_nombre_responsable).toBe(null)
    expect(buildPayload({ MANIFIESTO: '1', 'CREADO POR': '' }, 0)
      .payload.p_nombre_responsable).toBe(null)
  })

  it('valores numéricos pasan por toNum (formato colombiano)', () => {
    const r = buildPayload({
      MANIFIESTO: '1',
      'VALORES REMESAS': '1.500.000',
      'FLETE': '800.000',
      'ANTICIPO': '200.000',
    }, 0)
    expect(r.payload.p_valor_remesa).toBe(1500000)
    expect(r.payload.p_flete_conductor).toBe(800000)
    expect(r.payload.p_anticipo).toBe(200000)
  })

  it('VALORES REMESAS multi-valor se suma', () => {
    const r = buildPayload({
      MANIFIESTO: '1',
      'VALORES REMESAS': '500.000; 500.000',
    }, 0)
    expect(r.payload.p_valor_remesa).toBe(1000000)
  })

  it('strings con espacios se trimean', () => {
    const r = buildPayload({
      MANIFIESTO: '1',
      'CONDUCTOR': '  JUAN PEREZ  ',
      'PLACA':     '  ABC123  ',
    }, 0)
    expect(r.payload.p_conductor).toBe('JUAN PEREZ')
    expect(r.payload.p_placa).toBe('ABC123')
  })

  it('FECHA EMISIÓN como serial Excel se convierte', () => {
    // 45000 cae en 2023-03-15 aproximadamente
    const r = buildPayload({ MANIFIESTO: '1', 'FECHA EMISIÓN': 45000 }, 0)
    expect(r.payload.p_fecha_despacho).toMatch(/^2023-\d{2}-\d{2}$/)
    expect(r.payload.p_año).toBe(2023)
  })

  it('fila completa realista', () => {
    const fila = {
      MANIFIESTO:        '21001',
      'FECHA EMISIÓN':   '2026-05-14',
      ORIGEN:            'CALI(Vall)',
      DESTINO:           'BOGOTA(Cund)',
      GENERADORES:       'COLPALETAS S.A.',
      CONDUCTOR:         'HENRY RAMIREZ',
      'DOC. CONDUCTOR':  '1130668182',
      'TEL. CONDUCTOR':  '3001234567',
      PLACA:             'ABC123',
      REMOLQUE:          'TRAILER',
      POSEEDOR:          'PROPIETARIO LTDA',
      AGENCIA:           'CALI',
      'CREADO POR':      'maria',
      'VALORES REMESAS': '1.500.000',
      FLETE:             '800.000',
      ANTICIPO:          '200.000',
      REMESAS:           'R-001,R-002',
    }
    const r = buildPayload(fila, 0)
    expect(r.payload).toMatchObject({
      p_manifiesto:           21001,
      p_fecha_despacho:       '2026-05-14',
      p_mes:                  'MAYO',
      p_año:                  2026,
      p_periodo:              '2026-05-01',
      p_origen:               'CALI',
      p_departamento_origen:  'Valle del Cauca',
      p_destino:              'BOGOTA',
      p_departamento_destino: 'Cundinamarca',
      p_cliente:              'COLPALETAS S.A.',
      p_conductor:            'HENRY RAMIREZ',
      p_cedula_conductor:     '1130668182',
      p_propietario:          'PROPIETARIO LTDA',
      p_agencia_despachadora: 'CALI',
      p_nombre_responsable:   'MARIA',
      p_valor_remesa:         1500000,
      p_flete_conductor:      800000,
      p_anticipo:             200000,
      p_remesas:              'R-001,R-002',
    })
  })
})
