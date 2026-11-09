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

/** The recinto under a map click, as the backend summarises it. */
export interface EnclosureAt {
  reference: string;
  sigpacUse: string | null;
  areaHa: number | null;
  slopePct: number | null;
  irrigationCoef: number | null;
  geometry: { type: 'Polygon' | 'MultiPolygon'; coordinates: any };
  inParcel: string | null;
}

export class SigpacApiError extends Error {
  constructor(public status: number, public code?: string, public detail: Record<string, any> = {}) {
    super(`sigpac ${status}${code ? ` ${code}` : ''}`);
  }
}

async function postJson<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${apiOrigin()}/api/cadastral-api${path}`, {
    method: 'POST',
    credentials: 'include',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new SigpacApiError(res.status, data?.error, data);
  return data as T;
}

export function enclosureAt(longitude: number, latitude: number): Promise<EnclosureAt> {
  return postJson<EnclosureAt>('/sigpac/enclosure-at', { longitude, latitude });
}

export function createSigpacParcel(
  references: string[],
  name?: string,
): Promise<{ id: string; sigpacPending: boolean }> {
  return postJson('/sigpac/parcels', name ? { references, name } : { references });
}
