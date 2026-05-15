import { describe, it, expect } from 'vitest'
import { normalizeVal, NUMERIC_FIELDS, removeAccents } from '../lib/normalize'

describe('normalizeVal', () => {
  it('devuelve null para valores vacíos', () => {
    expect(normalizeVal(null, 'origen')).toBe(null)
    expect(normalizeVal(undefined, 'origen')).toBe(null)
    expect(normalizeVal('', 'origen')).toBe(null)
  })

  it('trim simple para strings no-numéricos', () => {
    expect(normalizeVal('  BOGOTA  ', 'origen')).toBe('BOGOTA')
    expect(normalizeVal('CALI', 'destino')).toBe('CALI')
  })

  it('normaliza NUMERIC PostgREST "500000.00" igual a payload Number(500000)', () => {
    // Caso real del bug: PostgREST devuelve string, payload tiene number → falsos diffs
    const fromDB     = normalizeVal('500000.00', 'flete_conductor')
    const fromPayload = normalizeVal(500000, 'flete_conductor')
    expect(fromDB).toBe(fromPayload)
  })

  it('redondea decimales en campos numéricos', () => {
    expect(normalizeVal('1234.49', 'valor_remesa')).toBe('1234')
    expect(normalizeVal('1234.50', 'valor_remesa')).toBe('1235')
  })

  it('devuelve null si un numérico no se puede parsear', () => {
    expect(normalizeVal('abc', 'flete_conductor')).toBe(null)
  })

  it('0 numérico se trata como null (evita falsos diffs entre DB con 0 y Excel vacío)', () => {
    expect(normalizeVal(0, 'flete_conductor')).toBe(null)
    expect(normalizeVal('0', 'flete_conductor')).toBe(null)
    expect(normalizeVal('0.00', 'valor_remesa')).toBe(null)
    // Lado DB: PostgREST devuelve "0.00" para NUMERIC(14,2) con 0
    expect(normalizeVal('0.00', 'anticipo')).toBe(null)
    // Lado Excel: celda vacía → null
    expect(normalizeVal(null, 'anticipo')).toBe(null)
    // Ambos lados normalizados al mismo valor → no hay diff
    expect(normalizeVal(0, 'flete_conductor')).toBe(normalizeVal(null, 'flete_conductor'))
  })

  it('cubre todos los campos numéricos esperados', () => {
    expect(NUMERIC_FIELDS.has('valor_remesa')).toBe(true)
    expect(NUMERIC_FIELDS.has('flete_conductor')).toBe(true)
    expect(NUMERIC_FIELDS.has('anticipo')).toBe(true)
    // Campos NO numéricos del DB_FIELDS
    expect(NUMERIC_FIELDS.has('origen')).toBe(false)
    expect(NUMERIC_FIELDS.has('placa')).toBe(false)
    expect(NUMERIC_FIELDS.has('propietario')).toBe(false)
  })
})

describe('removeAccents', () => {
  it('quita tildes pero conserva ñ/Ñ', () => {
    expect(removeAccents('Bogotá')).toBe('Bogota')
    expect(removeAccents('Nariño')).toBe('Nariño')
    expect(removeAccents('CAÑÓN')).toBe('CAÑON')
  })

  it('maneja null/undefined sin romper', () => {
    expect(removeAccents(null)).toBe(null)
    expect(removeAccents(undefined)).toBe(undefined)
  })
})
