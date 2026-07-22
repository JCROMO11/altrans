import { useState, useEffect } from 'react'
import { supabase } from '../lib/supabase'

export function useCatalogos() {
  const [catalogos, setCatalogos] = useState({
    conductores: [], clientes: [], lugares: [],
    responsables: [], vehiculos: [], remolques: [], agencias: [], propietarios: [],
    compromisos_pago: [], facturas_electronicas: [], facturas_no: [],
  })
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    const toList = (arr) => (arr ?? []).filter(Boolean)

    supabase.rpc('get_catalogos')
      .then(({ data: cat, error }) => {
        if (error || !cat) {
          console.error('get_catalogos error:', error)
          setLoading(false)
          return
        }
        setCatalogos({
          conductores: toList(cat.conductores).map(c => ({
            id:      c.nombre,
            nombre:  c.nombre,
            cedula:  c.cedula  ?? null,
            celular: c.celular ?? null,
          })),
          clientes:     toList(cat.clientes).map(n => ({ id: n, nombre: n })),
          lugares:      toList(cat.lugares).map(n => ({ id: n, nombre: n })),
          responsables: toList(cat.responsables).map(n => ({ id: n, nombre: n })),
          vehiculos:    toList(cat.vehiculos).map(n => ({ id: n, placa: n, nombre: n })),
          remolques:    toList(cat.remolques).map(n => ({ id: n, placa: n, nombre: n })),
          agencias:     toList(cat.agencias).map(n => ({ id: n, nombre: n })),
          propietarios: toList(cat.propietarios).map(n => ({ id: n, nombre: n })),
          compromisos_pago: toList(cat.compromisos_pago),
          facturas_electronicas: toList(cat.facturas_electronicas).map(n => ({ id: n, nombre: n })),
          facturas_no: toList(cat.facturas_no).map(n => ({ id: n, nombre: n })),
        })
        setLoading(false)
      })
      .catch(err => {
        console.error('get_catalogos error:', err)
        setLoading(false)
      })
  }, [])

  return {
    catalogos, loading,
    createConductor:   async (nombre, extras = {}) => ({ id: nombre, nombre, cedula: extras.cedula, celular: extras.celular }),
    updateConductor:   async () => {},
    createCliente:     async (nombre) => ({ id: nombre, nombre }),
    createLugar:       async (nombre) => ({ id: nombre, nombre }),
    createResponsable: async (nombre) => ({ id: nombre, nombre }),
    createVehiculo:    async (placa)  => ({ id: placa, placa }),
    createRemolque:    async (placa)  => ({ id: placa, placa }),
    createPropietario: async (nombre) => ({ id: nombre, nombre }),
  }
}
