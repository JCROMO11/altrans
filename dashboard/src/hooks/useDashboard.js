import { useState, useEffect } from 'react'
import { supabase } from '../lib/supabase'

const MESES_ORDER = ['ENERO','FEBRERO','MARZO','ABRIL','MAYO','JUNIO','JULIO','AGOSTO','SEPTIEMBRE','OCTUBRE','NOVIEMBRE','DICIEMBRE']

export function useDashboard(mes, año) {
  const [data, setData]       = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    let cancelled = false

    async function fetchData() {
      setLoading(true)
      try {
        const [kpiRes, anualRes] = await Promise.all([
          supabase.rpc('dashboard_kpis', { p_mes: mes, p_año: año }),
          supabase.rpc('tendencia_anual', { p_año: año ?? null }),
        ])
        if (cancelled) return

        const kpis       = kpiRes.data ?? {}
        const anualData  = anualRes.data ?? []

        const anualMap  = Object.fromEntries(anualData.map(r => [r.mes, r]))
        kpis.lineChart = MESES_ORDER.map(m => ({
          mes:       m.slice(0, 3),
          facturado: Number(anualMap[m]?.facturado ?? 0),
          ganancia:  Number(anualMap[m]?.ganancia  ?? 0),
        }))

        if (cancelled) return
        setData(kpis)
      } catch (err) {
        if (!cancelled) console.error('useDashboard:', err)
      } finally {
        if (!cancelled) setLoading(false)
      }
    }

    fetchData()
    return () => { cancelled = true }
  }, [mes, año])

  return { data, loading }
}
