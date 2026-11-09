import React from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Button, Input, Panel } from '@nekazari/ui-kit';
import type { EnclosureAt } from '../services/sigpacApi';

interface Props {
  selection: EnclosureAt[];
  name: string;
  onNameChange: (name: string) => void;
  onRemove: (reference: string) => void;
  onCreate: () => void;
  onCancel: () => void;
  creating: boolean;
}

const fmt = (n: number | null | undefined, digits = 2) =>
  typeof n === 'number' ? n.toFixed(digits) : '-';

/**
 * Recintos picked with the SIGPAC map click, before they become one parcel.
 * Data: FEGA SIGPAC (CC BY 4.0).
 */
export const SigpacSelectionPanel: React.FC<Props> = ({
  selection, name, onNameChange, onRemove, onCreate, onCancel, creating,
}) => {
  const { t } = useTranslation('cadastral');
  const total = selection.reduce((sum, e) => sum + (e.areaHa ?? 0), 0);
  const blocked = selection.some((e) => !!e.inParcel);
  const useName = (code: string | null) =>
    code ? t(`sigpac.uses.${code}`, { defaultValue: code }) : '-';

  return (
    <div className="fixed bottom-4 right-4 z-50 w-96 max-w-full">
      <Panel variant="solid">
        <Panel.Header>
          <Panel.Title>{t('sigpac.select.title')}</Panel.Title>
        </Panel.Header>
        <Panel.Body>
          <div className="space-y-2 text-nkz-sm text-nkz-text-secondary">
            <p className="text-nkz-text-muted">{t('sigpac.select.hint')}</p>
            <ul className="max-h-64 overflow-y-auto space-y-2">
              {selection.map((e) => (
                <li key={e.reference} className="border-b border-nkz-border pb-2 flex items-start gap-nkz-inline">
                  <div className="flex-1">
                    <div className="font-medium">{e.reference}</div>
                    <div>
                      {useName(e.sigpacUse)} · {fmt(e.areaHa)} ha
                    </div>
                    <div className="text-nkz-text-muted">
                      {t('sigpac.slope')}: {fmt(e.slopePct, 1)} % · {t('sigpac.irrigation')}: {fmt(e.irrigationCoef, 0)}
                    </div>
                    {e.inParcel && <div className="text-nkz-danger">{t('sigpac.select.inParcel')}</div>}
                  </div>
                  <Button size="sm" variant="ghost" onClick={() => onRemove(e.reference)} aria-label={t('sigpac.select.cancel')}>
                    ×
                  </Button>
                </li>
              ))}
            </ul>
            <div className="font-medium">{t('sigpac.select.total', { ha: total.toFixed(2) })}</div>
            <label className="block">
              <span className="block mb-1">{t('sigpac.select.name')}</span>
              <Input value={name} onChange={(ev) => onNameChange(ev.target.value)} placeholder={selection[0]?.reference} />
            </label>
            {blocked && <p className="text-nkz-danger">{t('sigpac.select.blocked')}</p>}
            <p className="text-nkz-text-muted">{t('sigpac.attribution')}</p>
          </div>
        </Panel.Body>
        <Panel.Footer>
          <div className="flex justify-end gap-nkz-inline">
            <Button variant="ghost" onClick={onCancel} disabled={creating}>
              {t('sigpac.select.cancel')}
            </Button>
            <Button variant="primary" onClick={onCreate} disabled={creating || blocked || selection.length === 0}>
              {creating ? t('sigpac.select.creating') : t('sigpac.select.create')}
            </Button>
          </div>
        </Panel.Footer>
      </Panel>
    </div>
  );
};

export default SigpacSelectionPanel;
