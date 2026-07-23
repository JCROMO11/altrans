// Config aislada de Vitest. NO importa vite.config.js para evitar arrastrar
// vite 8 + rolldown que requieren Node ≥ 20.12.
// Si en el futuro se sube Node, esto se puede fusionar con vite.config.js.
import path from 'path'
import { fileURLToPath } from 'node:url'
import { defineConfig } from 'vitest/config'

const __dirname = path.dirname(fileURLToPath(import.meta.url))

// Sin plugins de vite — el plugin-react arrastra rolldown que requiere Node 20+.
// Vitest usa esbuild internamente; le decimos que use el runtime JSX automático
// para que los .jsx no necesiten `import React from 'react'`. 
export default defineConfig({
  esbuild: {
    jsx: 'automatic',
  },
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  test: {
    environment: 'jsdom',
    globals: true,
    setupFiles: ['./src/__tests__/setup.js'],
    css: false,
    include: ['src/__tests__/**/*.test.{js,jsx}'],
  },
})
