import { defineConfig } from 'vite';
import { nkzModulePreset } from '@nekazari/module-builder';
import path from 'path';

export default defineConfig(
  nkzModulePreset({
    viteConfig: {
      resolve: {
        alias: { '@': path.resolve(__dirname, './src') },
      },
      server: {
        port: 5004,
        proxy: {
          '/api': {
            target: process.env.VITE_PROXY_TARGET || 'http://localhost:8000',
            changeOrigin: true,
            secure: process.env.VITE_PROXY_TARGET?.startsWith('https') ?? false,
          },
        },
      },
    },
  }),
);
