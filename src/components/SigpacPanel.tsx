import React, { useCallback, useEffect, useState } from 'react';
import { useTranslation, useViewer } from '@nekazari/sdk';
import { SlotShell } from '@nekazari/viewer-kit';
import { Button } from '@nekazari/ui-kit';
import { getSigpac, refreshSigpac, type SigpacEnclosure } from '../services/sigpacApi';

const cadastralAccent = { base: '#A855F7', soft: '#F3E8FF', strong: '#7E22CE' };

/**
 * Lists the SIGPAC enclosures stored on the selected AgriParcel and lets the
 * user re-run the lookup. Data: FEGA SIGPAC (CC BY 4.0).
 */
export const SigpacPanel: React.FC = () => {
  const { t } = useTranslation('cadastral');
  const { selectedEntityId, selectedEntityType } = useViewer();
  const [items, setItems] = useState<SigpacEnclosure[] | null>(null);
  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState<string | null>(null);
  const isParcel = selectedEntityType === 'AgriParcel' && !!selectedEntityId;

  useEffect(() => {
    setItems(null);
    setMsg(null);
    if (!isParcel) return;
    let live = true;
    getSigpac(selectedEntityId as string)
      .then((r) => live && setItems(r.enclosures))
      .catch(() => live && setMsg(t('sigpac.error')));
    return () => {
      live = false;
    };
  }, [isParcel, selectedEntityId, t]);

  const update = useCallback(async () => {
    setBusy(true);
    setMsg(null);
    try {
      const r = await refreshSigpac(selectedEntityId as string);
      if (r.status === 'pending') {
        setMsg(t('sigpac.pending'));
        window.setTimeout(() => {
          getSigpac(selectedEntityId as string).then((g) => setItems(g.enclosures)).catch(() => undefined);
        }, 15000);
      } else if (r.status === 'skipped' && r.reason === 'outside_spain') setMsg(t('sigpac.outside'));
      else setItems((r as any).enclosures ?? null);
    } catch {
      setMsg(t('sigpac.error'));
    } finally {
      setBusy(false);
    }
  }, [selectedEntityId, t]);

  if (!isParcel) return null;

  return (
    <SlotShell moduleId="catastro-spain" accent={cadastralAccent} title={t('sigpac.title')}>
      <div className="space-y-2 text-nkz-sm text-nkz-text-secondary">
        {items === null && !msg && <p>{t('sigpac.none')}</p>}
        {items && items.length === 0 && <p>{t('sigpac.empty')}</p>}
        {items?.map((e) => (
          <div key={e.reference} className="border-b border-nkz-border pb-2">
            <div className="font-medium">{e.reference}</div>
            <div>{t('sigpac.overlap', { pct: e.overlapPct, ha: e.overlapAreaHa })}</div>
            <div className="text-nkz-text-muted">
              {t('sigpac.use')}: {e.sigpacUse ?? '-'} · {t('sigpac.slope')}: {e.slopePct ?? '-'} % ·{' '}
              {t('sigpac.irrigation')}: {e.irrigationCoef ?? '-'}
            </div>
          </div>
        ))}
        {msg && <p>{msg}</p>}
        <Button variant="secondary" onClick={update} disabled={busy}>
          {busy ? t('sigpac.updating') : t('sigpac.update')}
        </Button>
        <p className="text-nkz-text-muted">{t('sigpac.attribution')}</p>
      </div>
    </SlotShell>
  );
};

export default SigpacPanel;
