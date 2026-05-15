import '@testing-library/jest-dom/vitest'
import { vi } from 'vitest'

// Stub de env vars usadas por src/lib/supabase.js
if (!import.meta.env.VITE_SUPABASE_URL) {
  import.meta.env.VITE_SUPABASE_URL = 'http://test.local'
  import.meta.env.VITE_SUPABASE_ANON_KEY = 'test-anon-key'
}

// Mock global de supabase: encadenable y resoluble por defecto.
// Tests específicos sobrescriben con vi.fn().mockReturnValue/mockResolvedValue.
const makeChain = () => {
  const chain = {
    select: vi.fn(() => chain),
    eq:     vi.fn(() => chain),
    in:     vi.fn(() => chain),
    order:  vi.fn(() => chain),
    limit:  vi.fn(() => chain),
    range:  vi.fn(() => Promise.resolve({ data: [], error: null })),
    maybeSingle: vi.fn(() => Promise.resolve({ data: null, error: null })),
    then:   (resolve) => resolve({ data: [], error: null }),
  }
  return chain
}

vi.mock('../lib/supabase', () => ({
  supabase: {
    rpc:  vi.fn(() => Promise.resolve({ data: [], error: null })),
    from: vi.fn(() => makeChain()),
    auth: {
      getSession:        vi.fn(() => Promise.resolve({ data: { session: null } })),
      onAuthStateChange: vi.fn(() => ({ data: { subscription: { unsubscribe: vi.fn() } } })),
      signOut:           vi.fn(),
    },
  },
}))
