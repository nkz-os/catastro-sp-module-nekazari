import { defineModule } from '@nekazari/module-kit';
import React, { lazy, Suspense } from 'react';
import './i18n';
import { moduleSlots } from './slots';
import { CadastralProvider } from './context/CadastralContext';
import pkg from '../package.json';

const LazyApp = lazy(() => import('./App'));

const MainWrapper: React.FC = () => (
  <CadastralProvider>
    <Suspense fallback={<div className="p-8 text-center">Loading Catastro España…</div>}>
      <LazyApp />
    </Suspense>
  </CadastralProvider>
);

const { moduleProvider: _moduleProvider, ...rawSlots } = moduleSlots as Record<string, unknown>;
const wrappedSlots = Object.fromEntries(
  Object.entries(rawSlots).map(([slot, entries]) => [
    slot,
    (entries as Array<Record<string, any>>).map((entry) => {
      const Inner = entry.localComponent as React.ComponentType<any> | undefined;
      if (!Inner) return entry;
      const Wrapped: React.FC<any> = (props) => (
        <CadastralProvider>
          <Inner {...props} />
        </CadastralProvider>
      );
      return { ...entry, localComponent: Wrapped };
    }),
  ]),
);

export default defineModule({
  id: 'catastro-spain',
  displayName: 'Catastro España',
  version: pkg.version,
  hostApiVersion: '^2.0.0',
  description: 'Spanish Cadastre integration — click to add parcels — Nekazari Platform Module',
  accent: { base: '#0EA5E9', soft: '#E0F2FE', strong: '#0369A1' },
  icon: 'map-pin',
  main: MainWrapper,
  api: { basePath: '/api/cadastral-api' },
  slots: wrappedSlots as never,
});
