import React, { useState, useEffect } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { SlotShellCompact } from '@nekazari/viewer-kit';
import { Building2 } from 'lucide-react';

const cadastralAccent = { base: '#A855F7', soft: '#F3E8FF', strong: '#7E22CE' };

/**
 * Control toggle for enabling/disabling the 3D building layer
 * Appears in the layer-toggle slot (sidebar layer manager)
 */
export const CadastralBuildingsToggle: React.FC = () => {
  const { t } = useTranslation('cadastral');
  const [enabled, setEnabled] = useState(false);

  useEffect(() => {
    window.dispatchEvent(new CustomEvent('cadastral:buildings-toggle', {
      detail: { visible: enabled },
    }));
  }, [enabled]);

  return (
    <SlotShellCompact moduleId="catastro-spain" accent={cadastralAccent}>
      <label className="flex items-center gap-nkz-inline px-3 py-2 cursor-pointer select-none w-full">
        <Building2 className={`w-4 h-4 ${enabled ? 'text-nkz-accent-base' : 'text-nkz-text-muted'}`} />
        <input
          type="checkbox"
          checked={enabled}
          onChange={(e) => setEnabled(e.target.checked)}
          className="toggle toggle-sm"
        />
        <span className="flex-1 text-left text-nkz-sm text-nkz-text-secondary">
          {t('layer.buildings3d', 'Edificios 3D')}
        </span>
        <div
          className={`w-3 h-3 rounded-full transition-colors ${
            enabled ? 'bg-nkz-accent-base' : 'bg-nkz-border'
          }`}
        />
      </label>
    </SlotShellCompact>
  );
};

export default CadastralBuildingsToggle;
