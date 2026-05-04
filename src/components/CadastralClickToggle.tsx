import React from 'react';
import { SlotShellCompact } from '@nekazari/viewer-kit';
import { MapPin, MapPinOff } from 'lucide-react';
import { useCadastral } from '../context/CadastralContext';

const cadastralAccent = { base: '#A855F7', soft: '#F3E8FF', strong: '#7E22CE' };

/**
 * Control toggle for enabling/disabling cadastral click-to-add functionality
 * Appears in the layer-toggle slot (sidebar layer manager)
 */
export const CadastralClickToggle: React.FC = () => {
  const { isClickEnabled, toggleClickEnabled } = useCadastral();

  return (
    <SlotShellCompact moduleId="catastro-spain" accent={cadastralAccent}>
      <button
        onClick={toggleClickEnabled}
        className={`w-full flex items-center gap-nkz-inline px-3 py-2 rounded-lg transition-all ${
          isClickEnabled
            ? 'bg-nkz-accent-soft text-nkz-accent-strong'
            : 'hover:bg-nkz-bg-soft text-nkz-text-secondary'
        }`}
        title={isClickEnabled ? 'Desactivar clic catastral' : 'Activar clic catastral'}
      >
        {isClickEnabled ? (
          <MapPin className="w-4 h-4 text-nkz-accent-base" />
        ) : (
          <MapPinOff className="w-4 h-4 text-nkz-text-muted" />
        )}
        <span className="flex-1 text-left text-nkz-sm">
          {isClickEnabled ? 'Clic Catastral Activo' : 'Clic Catastral'}
        </span>
        <div
          className={`w-3 h-3 rounded-full transition-colors ${
            isClickEnabled ? 'bg-nkz-accent-base' : 'bg-nkz-border'
          }`}
        />
      </button>
    </SlotShellCompact>
  );
};

