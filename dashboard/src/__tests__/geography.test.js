import { describe, it, expect } from 'vitest'
import { parseCiudad } from '../lib/geography'

describe('parseCiudad', () => {
  it('vacío → null/null', () => {
    expect(parseCiudad('')).toEqual({ ciudad: null, depto: null })
    expect(parseCiudad(null)).toEqual({ ciudad: null, depto: null })
  })

  it('formato "CIUDAD (ABREV)" con abreviatura conocida', () => {
    expect(parseCiudad('Bogotá (Cund)')).toEqual({ ciudad: 'Bogota', depto: 'Cundinamarca' })
    expect(parseCiudad('Cali (Vall)')).toEqual({ ciudad: 'Cali', depto: 'Valle del Cauca' })
    expect(parseCiudad('Ipiales (Nari)')).toEqual({ ciudad: 'Ipiales', depto: 'Nariño' })
  })

  it('formato "CIUDAD (ABREV)" con abreviatura desconocida → usa la abrev sin tildes', () => {
    expect(parseCiudad('Pueblo (XYZ)')).toEqual({ ciudad: 'Pueblo', depto: 'XYZ' })
  })

  it('sin paréntesis → busca en CITY_DEPT_FALLBACK', () => {
    expect(parseCiudad('BOGOTA')).toEqual({ ciudad: 'BOGOTA', depto: 'Bogota D.C.' })
    expect(parseCiudad('Medellín')).toEqual({ ciudad: 'Medellin', depto: 'Antioquia' })
    expect(parseCiudad('CALI')).toEqual({ ciudad: 'CALI', depto: 'Valle del Cauca' })
  })

  it('ciudad desconocida sin paréntesis → depto null', () => {
    expect(parseCiudad('CIUDAD INEXISTENTE')).toEqual({
      ciudad: 'CIUDAD INEXISTENTE', depto: null,
    })
  })

  it('normaliza espacios múltiples al buscar fallback', () => {
    expect(parseCiudad('BOGOTA D. C.')).toEqual({ ciudad: 'BOGOTA D. C.', depto: 'Bogota D.C.' })
  })

  it('preserva ñ', () => {
    const r = parseCiudad('Pasto (Nari)')
    expect(r.depto).toBe('Nariño')
  })
})
