import { useState } from 'react'
import Layout from './components/Layout'
import DashboardPage from './pages/DashboardPage'
import CargaPage from './pages/CargaPage'
import ConsultaPage from './pages/ConsultaPage'
import PasswordGate from './components/PasswordGate'

function App() {
  const [page, setPage] = useState('dashboard')
  const [targetManifiesto, setTargetManifiesto] = useState(null)

  const openEnCarga = (num) => {
    setTargetManifiesto(num)
    setPage('carga')
  }

  return (
    <PasswordGate>
      <Layout page={page} setPage={setPage}>
        {page === 'dashboard' && <DashboardPage />}
        {page === 'carga'     && (
          <CargaPage
            target={targetManifiesto}
            clearTarget={() => setTargetManifiesto(null)}
          />
        )}
        {page === 'consulta'  && <ConsultaPage openEnCarga={openEnCarga} />}
      </Layout>
    </PasswordGate>
  )
}

export default App
