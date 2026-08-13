import { useState, useEffect, lazy, Suspense } from 'react'
import { supabase } from './lib/supabase'
import Layout from './components/Layout'
import LoginGate from './components/LoginGate'
import ErrorBoundary from './components/ErrorBoundary'
import { ToastProvider } from './components/Toast'
import DashboardPage from './pages/DashboardPage'

const CargaPage     = lazy(() => import('./pages/CargaPage'))
const ConsultaPage  = lazy(() => import('./pages/ConsultaPage'))

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
      {page === 'dashboard' && <DashboardPage user={session.user} />}
      {page === 'carga'     && (
        <Suspense fallback={<PageLoader />}>
          <CargaPage
            target={targetManifiesto}
            clearTarget={() => setTargetManifiesto(null)}
            user={session.user}
          />
        </Suspense>
      )}
      {page === 'consulta'  && (
        <Suspense fallback={<PageLoader />}>
          <ConsultaPage openEnCarga={openEnCarga} user={session.user} />
        </Suspense>
      )}
    </Layout>
  )
}

function PageLoader() {
  return (
    <div className="flex-1 flex items-center justify-center py-20">
      <div className="w-8 h-8 rounded-full border-2 border-t-transparent animate-spin"
        style={{ borderColor: 'var(--primary)' }} />
    </div>
  )
}

export default function AppWithProviders() {
  return (
    <ErrorBoundary>
      <ToastProvider>
        <App />
      </ToastProvider>
    </ErrorBoundary>
  )
}
