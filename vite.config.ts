import { defineConfig, type UserConfig } from 'vite';
import react from '@vitejs/plugin-react';
import path from 'path';

// =============================================================================
// Module Configuration
// =============================================================================

const MODULE_ID = 'catastro-spain';
const ENTRY = 'src/moduleEntry.ts';
const OUTPUT_FILE = 'nkz-module.js';

// =============================================================================
// Externals — shared deps provided by the host via window globals
// =============================================================================

const NKZ_EXTERNALS: Record<string, string> = {
  'react': 'React',
  'react-dom': 'ReactDOM',
  'react-dom/client': 'ReactDOM',
  'react-router-dom': 'ReactRouterDOM',
  '@nekazari/sdk': '__NKZ_SDK__',
  '@nekazari/ui-kit': '__NKZ_UI__',
  '@nekazari/design-tokens': '__NKZ_DESIGN_TOKENS__',
  '@nekazari/viewer-kit': '__NKZ_VIEWER_KIT__',
};

// =============================================================================
// Vite Config
// =============================================================================

export default defineConfig({
  plugins: [
    // CRITICAL: Use 'classic' runtime to output React.createElement() calls.
    // The 'automatic' runtime emits _jsx() which requires the jsx-runtime library.
    // Since we externalize React to window.React (UMD), and UMD builds typically
    // don't expose the jsx() function on the global object, we must use classic.
    react({ jsxRuntime: 'classic' }),

    // Banner plugin to add module metadata comment
    {
      name: 'nkz-module-banner',
      generateBundle(_options, bundle) {
        for (const chunk of Object.values(bundle)) {
          if (chunk.type === 'chunk' && chunk.isEntry) {
            chunk.code = `/* NKZ Module: ${MODULE_ID} | Built: ${new Date().toISOString()} */\n${chunk.code}`;
          }
        }
      },
    },
  ],

  define: {
    'process.env.NODE_ENV': JSON.stringify('production'),
    '__NKZ_MODULE_ID__': JSON.stringify(MODULE_ID),
  },

  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },

  server: {
    host: '0.0.0.0',
    port: 5004,
    proxy: {
      '/api': {
        target: 'https://nkz.robotika.cloud',
        changeOrigin: true,
        secure: true,
      },
    },
  },

  build: {
    lib: {
      entry: ENTRY,
      name: `NKZModule_${MODULE_ID.replace(/[^a-zA-Z0-9_]/g, '_')}`,
      formats: ['iife'],
      fileName: () => OUTPUT_FILE,
    },
    rollupOptions: {
      external: Object.keys(NKZ_EXTERNALS),
      output: {
        globals: NKZ_EXTERNALS,
        inlineDynamicImports: true,
      },
    },
    outDir: 'dist',
    emptyOutDir: true,
    sourcemap: true,
    minify: 'esbuild',
    copyPublicDir: false,
  },
});
