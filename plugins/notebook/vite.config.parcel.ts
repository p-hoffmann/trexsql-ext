import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
import path from 'path'

export default defineConfig({
  plugins: [react(), tailwindcss()],
  worker: {
    format: 'es',
  },
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  define: {
    'process.env.NODE_ENV': JSON.stringify('production'),
  },
  build: {
    lib: {
      entry: path.resolve(__dirname, 'src/parcel.tsx'),
      formats: ['es'],
      fileName: () => 'notebook-parcel.js',
    },
    rollupOptions: {},
    outDir: 'dist',
    emptyOutDir: false,
    cssCodeSplit: false,
  },
})
