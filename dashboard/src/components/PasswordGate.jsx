import { useState } from 'react'

const PASS = import.meta.env.VITE_APP_PASSWORD

const BG   = '#0f1e2b'
const BDR  = '#2A3F52'
const TICK = '#F0F4F8'
const BLUE = '#1E6FBF'
const GOLD = '#C9A84C'
const MUTED = '#8FA3B1'

export default function PasswordGate({ children }) {
  const [authed, setAuthed] = useState(
    () => sessionStorage.getItem('altrans_auth') === '1'
  )
  const [input, setInput] = useState('')
  const [error, setError] = useState(false)
  const [visible, setVisible] = useState(false)

  if (authed) return children

  const submit = (e) => {
    e.preventDefault()
    if (PASS && input === PASS) {
      sessionStorage.setItem('altrans_auth', '1')
      setAuthed(true)
    } else {
      setError(true)
      setInput('')
      setTimeout(() => setError(false), 2000)
    }
  }

  return (
    <div className="min-h-screen flex items-center justify-center p-4"
      style={{ background: '#0a1520' }}>
      <div className="w-full max-w-sm flex flex-col gap-6"
        style={{ background: BG, border: `1px solid ${BDR}`, borderRadius: 16, padding: 32 }}>

        {/* Logo / título */}
        <div className="flex flex-col items-center gap-2 pb-2">
          <div className="w-12 h-12 rounded-xl flex items-center justify-center mb-1"
            style={{ background: BLUE + '22', border: `1px solid ${BLUE}44` }}>
            <svg width="24" height="24" viewBox="0 0 24 24" fill="none">
              <path d="M3 9l9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"
                stroke={BLUE} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
              <polyline points="9 22 9 12 15 12 15 22"
                stroke={BLUE} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
            </svg>
          </div>
          <p className="text-lg font-bold tracking-wide" style={{ color: TICK }}>Altrans</p>
          <p className="text-xs" style={{ color: MUTED }}>Ingresa la contraseña para continuar</p>
        </div>

        {/* Form */}
        <form onSubmit={submit} className="flex flex-col gap-3">
          <div className="relative">
            <input
              type={visible ? 'text' : 'password'}
              value={input}
              autoFocus
              placeholder="Contraseña"
              onChange={e => setInput(e.target.value)}
              className="w-full rounded-md border px-3 py-2.5 text-sm focus:outline-none focus:ring-1 focus:ring-[#1E6FBF] transition-colors bg-transparent pr-10"
              style={{
                borderColor: error ? '#ef4444' : BDR,
                color: TICK,
              }}
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
            <p className="text-xs text-center" style={{ color: '#fca5a5' }}>
              Contraseña incorrecta
            </p>
          )}

          <button
            type="submit"
            disabled={!input}
            className="w-full py-2.5 rounded-lg text-sm font-semibold transition-opacity disabled:opacity-40"
            style={{ background: BLUE, color: TICK }}>
            Entrar
          </button>
        </form>

        <p className="text-[10px] text-center" style={{ color: MUTED + '88' }}>
          Solo personal autorizado de Altrans
        </p>
      </div>
    </div>
  )
}
