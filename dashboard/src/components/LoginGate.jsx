import { useState } from 'react'
import { supabase } from '../lib/supabase'

const BG   = '#FFFFFF'
const BDR  = '#E2E8F0'
const TICK = '#0F172A'
const BLUE = '#1E6FBF'
const MUTED = '#64748B'

export default function LoginGate() {
  const [input, setInput]     = useState({ cedula: '', password: '' })
  const [error, setError]     = useState('')
  const [loading, setLoading] = useState(false)
  const [visible, setVisible] = useState(false)

  const submit = async (e) => {
    e.preventDefault()
    setLoading(true)
    setError('')

    const { error: err } = await supabase.auth.signInWithPassword({
      email:    `${input.cedula.trim()}@altrans.internal`,
      password: input.password,
    })

    if (err) {
      setError('Cédula o contraseña incorrectos')
      setLoading(false)
    }
    // Si hay éxito, onAuthStateChange en App.jsx actualiza la sesión automáticamente
  }

  return (
    <div className="min-h-screen flex items-center justify-center p-4"
      style={{ background: 'radial-gradient(ellipse 80% 60% at 10% 0%, #E0EEFF 0%, #EFF4FB 60%)' }}>
      <div className="w-full max-w-sm flex flex-col gap-6"
        style={{
          background: 'linear-gradient(135deg, #FFFFFF 0%, #F5F8FE 60%, #F1F1FB 100%)',
          border: `1px solid ${BDR}`,
          borderRadius: 16,
          padding: 32,
          boxShadow: '0 4px 24px 0 rgba(30,111,191,0.10), 0 1px 2px 0 rgba(15,23,42,0.04)',
        }}>

        {/* Logo */}
        <div className="flex flex-col items-center gap-2 pb-2">
          <div className="w-12 h-12 rounded-xl flex items-center justify-center mb-1"
            style={{
              background: 'linear-gradient(135deg, #1E6FBF 0%, #6366F1 100%)',
              boxShadow: '0 4px 16px 0 rgba(30,111,191,0.30)',
            }}>
            <svg width="24" height="24" viewBox="0 0 24 24" fill="none">
              <path d="M3 9l9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"
                stroke="#FFFFFF" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
              <polyline points="9 22 9 12 15 12 15 22"
                stroke="#FFFFFF" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
            </svg>
          </div>
          <p className="text-lg font-bold tracking-wide" style={{ color: TICK }}>Altrans</p>
          <p className="text-xs" style={{ color: MUTED }}>Ingresa tu cédula y contraseña para continuar</p>
        </div>

        {/* Form */}
        <form onSubmit={submit} className="flex flex-col gap-3">
          <input
            type="text"
            inputMode="numeric"
            value={input.cedula}
            autoFocus
            placeholder="Número de cédula"
            onChange={e => setInput(p => ({ ...p, cedula: e.target.value }))}
            className="w-full rounded-md border px-3 py-2.5 text-sm focus:outline-none focus:ring-2 focus:ring-[#1E6FBF]/40 focus:border-[#1E6FBF] transition-all bg-transparent"
            style={{ borderColor: error ? '#ef4444' : BDR, color: TICK }}
          />

          <div className="relative">
            <input
              type={visible ? 'text' : 'password'}
              value={input.password}
              placeholder="Contraseña"
              onChange={e => setInput(p => ({ ...p, password: e.target.value }))}
              className="w-full rounded-md border px-3 py-2.5 text-sm focus:outline-none focus:ring-2 focus:ring-[#1E6FBF]/40 focus:border-[#1E6FBF] transition-all bg-transparent pr-10"
              style={{ borderColor: error ? '#ef4444' : BDR, color: TICK }}
            />
            <button
              type="button"
              onClick={() => setVisible(v => !v)}
              className="absolute right-3 top-1/2 -translate-y-1/2 text-xs"
              style={{ color: MUTED }}>
              {visible ? 'Ocultar' : 'Ver'}
            </button>
          </div>

          {error && (
            <p className="text-xs text-center" style={{ color: '#DC2626' }}>{error}</p>
          )}

          <button
            type="submit"
            disabled={!input.cedula || !input.password || loading}
            className="w-full py-2.5 rounded-lg text-sm font-semibold transition-all disabled:opacity-40 hover:shadow-lg"
            style={{
              background: 'linear-gradient(135deg, #1E6FBF 0%, #6366F1 100%)',
              color: '#FFFFFF',
              boxShadow: '0 2px 8px 0 rgba(30,111,191,0.25)',
            }}>
            {loading ? 'Ingresando...' : 'Entrar'}
          </button>
        </form>

        <p className="text-[10px] text-center" style={{ color: MUTED + '88' }}>
          Solo personal autorizado de Altrans
        </p>
      </div>
    </div>
  )
}
