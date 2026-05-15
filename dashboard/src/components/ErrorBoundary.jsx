import { Component } from 'react'

export default class ErrorBoundary extends Component {
  constructor(props) {
    super(props)
    this.state = { error: null }
  }

  static getDerivedStateFromError(error) {
    return { error }
  }

  componentDidCatch(error, info) {
    console.error('ErrorBoundary:', error, info)
  }

  render() {
    if (this.state.error) {
      return (
        <div style={{
          minHeight: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center',
          padding: 24, fontFamily: 'system-ui, sans-serif',
        }}>
          <div style={{ maxWidth: 480, textAlign: 'center' }}>
            <h2 style={{ marginBottom: 12 }}>Algo salió mal</h2>
            <p style={{ color: '#666', marginBottom: 20, fontSize: 14 }}>
              Ocurrió un error inesperado. Recarga la página para continuar.
            </p>
            <button
              onClick={() => window.location.reload()}
              style={{
                padding: '10px 20px', borderRadius: 6, border: 'none',
                background: '#1e40af', color: '#fff', cursor: 'pointer', fontSize: 14,
              }}
            >
              Recargar
            </button>
          </div>
        </div>
      )
    }
    return this.props.children
  }
}
