import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  build: {
    outDir: '../internal/api/ui',
    emptyOutDir: true,
  },
  server: {
    proxy: {
      // ws:true is required for the /api/v1/ws live-update socket to upgrade.
      '/api': { target: 'http://localhost:8080', ws: true },
    },
  },
})
