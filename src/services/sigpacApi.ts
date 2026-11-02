export interface SigpacEnclosure {
  reference: string;
  overlapAreaHa: number;
  overlapPct: number;
  enclosureAreaHa: number | null;
  sigpacUse: string | null;
  slopePct: number | null;
  irrigationCoef: number | null;
  retrievedAt: string;
}

export interface SigpacResult {
  enclosures: SigpacEnclosure[] | null;
  status?: 'updated' | 'skipped' | 'pending';
  reason?: string;
}

// API origin is a runtime value injected by the host page.
const apiOrigin = (): string =>
  String((window as any).__ENV__?.VITE_API_URL ?? (import.meta as any).env?.VITE_API_URL ?? '').replace(/\/$/, '');

const base = (parcelId: string): string =>
  `${apiOrigin()}/api/cadastral-api/parcels/${encodeURIComponent(parcelId)}/sigpac`;

export async function getSigpac(parcelId: string): Promise<SigpacResult> {
  const res = await fetch(base(parcelId), { credentials: 'include' });
  if (!res.ok) throw new Error(`sigpac ${res.status}`);
  return res.json();
}

export async function refreshSigpac(parcelId: string): Promise<SigpacResult> {
  const res = await fetch(`${base(parcelId)}/refresh`, {
    method: 'POST',
    credentials: 'include',
    headers: { 'Content-Type': 'application/json' },
    body: '{}',
  });
  if (!res.ok) throw new Error(`sigpac ${res.status}`);
  return res.json();
}
