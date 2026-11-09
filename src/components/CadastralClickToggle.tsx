import React from 'react';
import { useTranslation } from 'react-i18next';
import { SlotShellCompact } from '@nekazari/viewer-kit';
import { Button } from '@nekazari/ui-kit';
import { MapPin, MapPinOff } from 'lucide-react';
import { useCadastral, type ClickMode } from '../context/CadastralContext';

const cadastralAccent = { base: '#A855F7', soft: '#F3E8FF', strong: '#7E22CE' };

const MODES: { mode: ClickMode; key: string }[] = [
  { mode: 'sigpac', key: 'click.modeSigpac' },
  { mode: 'cadastral', key: 'click.modeCadastral' },
];

/**
 * Turns the map click on and off, and chooses what it selects:
 * SIGPAC enclosures (default) or the cadastral parcel.
 * Appears in the layer-toggle slot (sidebar layer manager).
 */
export const CadastralClickToggle: React.FC = () => {
  const { t } = useTranslation('cadastral');
  const { isClickEnabled, toggleClickEnabled, clickMode, setClickMode } = useCadastral();

  return (
    <SlotShellCompact moduleId="catastro-spain" accent={cadastralAccent}>
      <button
        onClick={toggleClickEnabled}
        className={`w-full flex items-center gap-nkz-inline px-3 py-2 rounded-lg transition-all ${
          isClickEnabled
            ? 'bg-nkz-accent-soft text-nkz-accent-strong'
            : 'hover:bg-nkz-bg-soft text-nkz-text-secondary'
        }`}
        title={isClickEnabled ? t('click.disable') : t('click.enable')}
      >
        {isClickEnabled ? (
          <MapPin className="w-4 h-4 text-nkz-accent-base" />
        ) : (
          <MapPinOff className="w-4 h-4 text-nkz-text-muted" />
        )}
        <span className="flex-1 text-left text-nkz-sm">
          {isClickEnabled ? t('click.active') : t('click.label')}
        </span>
        <div
          className={`w-3 h-3 rounded-full transition-colors ${
            isClickEnabled ? 'bg-nkz-accent-base' : 'bg-nkz-border'
          }`}
        />
      </button>
      {isClickEnabled && (
        <div className="flex gap-nkz-inline px-3 pb-2" role="radiogroup" aria-label={t('click.label')}>
          {MODES.map(({ mode, key }) => (
            <Button
              key={mode}
              size="sm"
              variant={clickMode === mode ? 'primary' : 'ghost'}
              role="radio"
              aria-checked={clickMode === mode}
              onClick={() => setClickMode(mode)}
            >
              {t(key)}
            </Button>
          ))}
        </div>
      )}
    </SlotShellCompact>
  );
};
