import { useState, useEffect } from 'react'
import { supabase } from './lib/supabase'
import Layout from './components/Layout'
import LoginGate from './components/LoginGate'
import DashboardPage from './pages/DashboardPage'
import CargaPage from './pages/CargaPage'
import ConsultaPage from './pages/ConsultaPage'

const INACTIVITY_MS = 8 * 60 * 60 * 1000 // 8 horas

function App() {
  const [session, setSession]               = useState(undefined) // undefined = cargando
  const [page, setPage]                     = useState('dashboard')
  const [targetManifiesto, setTargetManifiesto] = useState(null)

  useEffect(() => {
    // Sesión inicial
    supabase.auth.getSession().then(({ data: { session } }) => {
      setSession(session)
    })

    // Escuchar cambios (login / logout)
    const { data: { subscription } } = supabase.auth.onAuthStateChange((_event, session) => {
      setSession(session)
    })

    return () => subscription.unsubscribe()
  }, [])

  // Cierre automático por inactividad (8 horas)
  useEffect(() => {
    if (!session) return
    let timer = setTimeout(() => supabase.auth.signOut(), INACTIVITY_MS)
    let lastReset = 0
    const reset = () => {
      const now = Date.now()
      if (now - lastReset < 30_000) return // throttle: máximo una vez cada 30s
      lastReset = now
      clearTimeout(timer)
      timer = setTimeout(() => supabase.auth.signOut(), INACTIVITY_MS)
    }
    const events = ['mousemove', 'keydown', 'click', 'touchstart']
    events.forEach(ev => window.addEventListener(ev, reset, { passive: true }))
    return () => {
      clearTimeout(timer)
      events.forEach(ev => window.removeEventListener(ev, reset))
    }
  }, [session])

  const openEnCarga = (num) => {
    setTargetManifiesto(num)
    setPage('carga')
  }

  // Mientras verifica la sesión no renderiza nada
  if (session === undefined) return null

  // Sin sesión → formulario de login
  if (!session) return <LoginGate />

  // Con sesión → app completa
  return (
    <Layout page={page} setPage={setPage} user={session.user}>
      {page === 'dashboard' && <DashboardPage />}
      {page === 'carga'     && (
        <CargaPage
          target={targetManifiesto}
          clearTarget={() => setTargetManifiesto(null)}
          user={session.user}
        />
      )}
      {page === 'consulta'  && <ConsultaPage openEnCarga={openEnCarga} />}
    </Layout>
  )
}

export default App
