/* eslint-disable react-refresh/only-export-components */
import { createContext, useContext, useState, useCallback } from 'react'

const ToastCtx = createContext(null)

export function useToast() {
  const ctx = useContext(ToastCtx)
  if (!ctx) throw new Error('useToast debe usarse dentro de <ToastProvider>')
  return ctx
}

let nextId = 1

export function ToastProvider({ children }) {
  const [toasts, setToasts] = useState([])

  const remove = useCallback((id) => {
    setToasts(t => t.filter(x => x.id !== id))
  }, [])

  const show = useCallback((message, { type = 'info', duration = 4000 } = {}) => {
    const id = nextId++
    setToasts(t => [...t, { id, message, type }])
    if (duration > 0) setTimeout(() => remove(id), duration)
    return id
  }, [remove])

  const api = {
    show,
    success: (m, opts) => show(m, { ...opts, type: 'success' }),
    error:   (m, opts) => show(m, { ...opts, type: 'error', duration: 6000 }),
    info:    (m, opts) => show(m, { ...opts, type: 'info' }),
  }

  return (
    <ToastCtx.Provider value={api}>
      {children}
      <div style={STYLES.container}>
        {toasts.map(t => (
          <div key={t.id} style={{ ...STYLES.toast, ...STYLES[t.type] }} onClick={() => remove(t.id)}>
            {t.message}
          </div>
        ))}
      </div>
    </ToastCtx.Provider>
  )
}

const STYLES = {
  container: {
    position: 'fixed', bottom: 24, right: 24, zIndex: 9999,
    display: 'flex', flexDirection: 'column', gap: 8, maxWidth: 380,
  },
  toast: {
    padding: '12px 16px', borderRadius: 6, color: '#fff',
    fontSize: 14, fontWeight: 500, cursor: 'pointer',
    boxShadow: '0 4px 12px rgba(0,0,0,0.18)',
    animation: 'slideIn 0.2s ease-out',
  },
  success: { background: '#16a34a' },
  error:   { background: '#dc2626' },
  info:    { background: '#2563eb' },
}
