import { LayoutDashboard, Upload, Search, PanelLeftClose, PanelLeftOpen, Truck } from 'lucide-react'
import { useState } from 'react'

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

export default function Layout({ children, page, setPage }) {
  const [collapsed, setCollapsed] = useState(false)

  return (
    <div className="flex min-h-screen bg-background">

      {/* Sidebar */}
      <aside className={`flex flex-col border-r bg-card transition-all duration-300 ease-in-out ${collapsed ? 'w-15' : 'w-56'}`}>

        {/* Logo */}
        <div className={`flex items-center border-b h-14 shrink-0 ${collapsed ? 'justify-center px-0' : 'gap-2 px-4'}`}>
          <div className="flex items-center justify-center w-7 h-7 rounded-md bg-primary text-primary-foreground shrink-0">
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
          {navItems.map(({ icon: Icon, label, id }) => {
            const active = page === id
            return (
              <button
                key={id}
                onClick={() => setPage(id)}
                title={collapsed ? label : undefined}
                className={`flex items-center gap-3 px-3 py-2 rounded-lg text-sm font-medium transition-all duration-150
                  ${collapsed ? 'justify-center' : ''}
                  ${active
                    ? 'bg-primary text-primary-foreground shadow-sm'
                    : 'text-muted-foreground hover:bg-accent hover:text-foreground'
                  }`}
              >
                <Icon size={16} strokeWidth={active ? 2.5 : 2} />
                {!collapsed && <span>{label}</span>}
                {!collapsed && active && <span className="ml-auto w-1.5 h-1.5 rounded-full bg-primary-foreground/60" />}
              </button>
            )
          })}
        </nav>

        {/* Footer */}
        <div className={`border-t p-3 flex ${collapsed ? 'justify-center' : 'items-center justify-between'}`}>
          {!collapsed && <p className="text-[11px] text-muted-foreground/70">v1.0.0</p>}
          <button
            onClick={() => setCollapsed(!collapsed)}
            className="p-1.5 rounded-md hover:bg-accent text-muted-foreground hover:text-foreground transition-colors"
            title={collapsed ? 'Expandir' : 'Colapsar'}
          >
            {collapsed ? <PanelLeftOpen size={15} /> : <PanelLeftClose size={15} />}
          </button>
        </div>

      </aside>

      {/* Main */}
      <div className="flex flex-col flex-1 min-w-0">

        {/* Header */}
        <header className="flex items-center justify-between border-b px-6 h-14 bg-card shrink-0">
          <div className="flex items-center gap-2">
            <div className="w-1 h-5 rounded-full bg-primary" />
            <h1 className="text-sm font-semibold">{pageTitle[page]}</h1>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-2 h-2 rounded-full bg-green-500" />
            <span className="text-xs text-muted-foreground">Conectado</span>
          </div>
        </header>

        {/* Content */}
        <main className="flex-1 p-6 overflow-auto">
          {children}
        </main>

      </div>
    </div>
  )
}
