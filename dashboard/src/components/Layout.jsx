import { LayoutDashboard, Upload, Search, PanelLeftClose, PanelLeftOpen, Truck, LogOut } from 'lucide-react'
import { useState } from 'react'
import { supabase } from '../lib/supabase'

const navItems = [
  { icon: LayoutDashboard, label: 'Dashboard', id: 'dashboard' },
  { icon: Upload,          label: 'Carga',     id: 'carga'     },
  { icon: Search,          label: 'Consulta',  id: 'consulta'  },
]

const pageTitle = {
  dashboard: 'Dashboard de visualización',
  carga:     'Carga de datos',
  consulta:  'Consulta',
}

const ROL_CONFIG = {
  digitador:      { label: 'Digitador',      color: '#1E6FBF' },
  logistico:      { label: 'Logístico',      color: '#0891B2' },
  operativo:      { label: 'Operativo',      color: '#16A34A' },
  tesoreria:      { label: 'Tesorería',      color: '#7C3AED' },
  financiero:     { label: 'Financiero',     color: '#D97706' },
  administrativo: { label: 'Administrativo', color: '#DC2626' },
  contadora:      { label: 'Contadora',      color: '#BE185D' },
  gerencia:       { label: 'Gerencia',       color: '#C9A84C' },
  admin:          { label: 'Admin',          color: '#C9A84C' },
}

function abreviarNombre(nombre) {
  if (!nombre) return 'Usuario'
  const parts = nombre.trim().split(/\s+/)
  if (parts.length <= 2) return nombre.trim()
  // Nombres colombianos: nombre1 [nombre2] apellido1 apellido2
  // El primer apellido es siempre la penúltima palabra.
  return `${parts[0]} ${parts[parts.length - 2]}`
}

function RolBadge({ rol, size = 'md' }) {
  if (!rol) return null
  const cfg = ROL_CONFIG[rol] || { label: rol, color: '#64748B' }
  const px  = size === 'sm' ? '6px' : '10px'
  const py  = size === 'sm' ? '1px' : '3px'
  const fs  = size === 'sm' ? '9px' : '10px'
  return (
    <span style={{
      display:       'inline-block',
      padding:       `${py} ${px}`,
      borderRadius:  '9999px',
      fontSize:      fs,
      fontWeight:    700,
      letterSpacing: '0.04em',
      background:    cfg.color + '1A',
      color:         cfg.color,
      border:        `1px solid ${cfg.color}55`,
      lineHeight:    1.4,
      whiteSpace:    'nowrap',
    }}>
      {cfg.label}
    </span>
  )
}

export default function Layout({ children, page, setPage, user }) {
  const [collapsed, setCollapsed] = useState(false)

  const nombreCompleto = user?.user_metadata?.nombre || user?.app_metadata?.nombre || user?.email?.split('@')[0] || 'Usuario'
  const nombre = abreviarNombre(nombreCompleto)
  const rol    = user?.app_metadata?.role || ''

  return (
    <div className="flex min-h-screen bg-background">

      {/* Sidebar */}
      <aside
        className={`flex flex-col border-r transition-all duration-300 ease-in-out ${collapsed ? 'w-15' : 'w-56'}`}
        style={{ background: 'var(--gradient-sidebar)' }}
      >

        {/* Logo */}
        <div className={`flex items-center border-b h-14 shrink-0 ${collapsed ? 'justify-center px-0' : 'gap-2 px-4'}`}>
          <div
            className="flex items-center justify-center w-7 h-7 rounded-md text-primary-foreground shrink-0"
            style={{
              background: 'var(--gradient-brand)',
              boxShadow: '0 2px 8px 0 rgba(30,111,191,0.25)',
            }}
          >
            <Truck size={14} strokeWidth={2.5} />
          </div>
          {!collapsed && (
            <div className="leading-none">
              <p className="text-sm font-bold tracking-tight">Altrans</p>
              <p className="text-[10px] text-muted-foreground">S.A.S</p>
            </div>
          )}
        </div>

        {/* Nav */}
        <nav className="flex flex-col gap-0.5 p-2 flex-1">
          {!collapsed && (
            <p className="text-[10px] font-semibold text-muted-foreground/60 uppercase tracking-widest px-3 pt-2 pb-1">
              Módulos
            </p>
          )}
          {navItems.map(({ icon: Icon, label, id }) => { // eslint-disable-line no-unused-vars
            const active = page === id
            return (
              <button
                key={id}
                onClick={() => setPage(id)}
                title={collapsed ? label : undefined}
                style={active ? {
                  background: 'var(--gradient-brand)',
                  color: '#FFFFFF',
                  boxShadow: '0 2px 8px 0 rgba(30,111,191,0.25)',
                } : undefined}
                className={`flex items-center gap-3 px-3 py-2 rounded-lg text-sm font-medium transition-all duration-150
                  ${collapsed ? 'justify-center' : ''}
                  ${active
                    ? ''
                    : 'text-muted-foreground hover:bg-accent hover:text-foreground'
                  }`}
              >
                <Icon size={16} strokeWidth={active ? 2.5 : 2} />
                {!collapsed && <span>{label}</span>}
                {!collapsed && active && <span className="ml-auto w-1.5 h-1.5 rounded-full bg-white/70" />}
              </button>
            )
          })}
        </nav>

        {/* Footer sidebar */}
        <div className={`border-t p-3 flex ${collapsed ? 'justify-center' : 'items-center justify-between'}`}>
          {!collapsed && (
            <div className="min-w-0 flex flex-col gap-1">
              <p className="text-[11px] font-medium text-foreground truncate" title={nombreCompleto}>{nombre}</p>
              <RolBadge rol={rol} size="sm" />
            </div>
          )}
          <button
            onClick={() => setCollapsed(!collapsed)}
            className="p-1.5 rounded-md hover:bg-accent text-muted-foreground hover:text-foreground transition-colors shrink-0"
            title={collapsed ? 'Expandir' : 'Colapsar'}
          >
            {collapsed ? <PanelLeftOpen size={15} /> : <PanelLeftClose size={15} />}
          </button>
        </div>

      </aside>

      {/* Main */}
      <div className="flex flex-col flex-1 min-w-0">

        {/* Header */}
        <header
          className="flex items-center justify-between border-b px-6 h-14 shrink-0"
          style={{
            background: 'var(--gradient-sidebar)',
            boxShadow: 'var(--shadow-soft)',
          }}
        >
          <div className="flex items-center gap-2">
            <div className="w-1 h-5 rounded-full" style={{ background: 'var(--gradient-brand)' }} />
            <h1 className="text-sm font-semibold">{pageTitle[page]}</h1>
          </div>
          <div className="flex items-center gap-3">
            <div className="flex items-center gap-2">
              <div className="w-2 h-2 rounded-full bg-green-500" />
              <span className="text-xs text-muted-foreground">Conectado</span>
            </div>
            <div className="w-px h-4 bg-border" />
            <div className="flex items-center gap-2">
              <p className="text-xs font-medium leading-none" title={nombreCompleto}>{nombre}</p>
              <RolBadge rol={rol} size="md" />
            </div>
            <button
              onClick={() => supabase.auth.signOut()}
              className="p-1.5 rounded-md hover:bg-accent text-muted-foreground hover:text-foreground transition-colors"
              title="Cerrar sesión"
            >
              <LogOut size={15} />
            </button>
          </div>
        </header>

        {/* Content */}
        <main className="flex-1 px-6 py-3 overflow-hidden">
          {children}
        </main>

      </div>
    </div>
  )
}
