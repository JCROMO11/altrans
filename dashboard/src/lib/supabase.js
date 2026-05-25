import { createClient } from '@supabase/supabase-js'

export const supabase = createClient(
  import.meta.env.VITE_SUPABASE_URL,
  import.meta.env.VITE_SUPABASE_ANON_KEY,
  {
    auth: {
      // Sesión solo vive en esta pestaña/ventana; muere al cerrar el navegador
      storage: typeof window !== 'undefined' ? window.sessionStorage : undefined,
    },
  }
)