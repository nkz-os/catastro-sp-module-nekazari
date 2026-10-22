import { NKZClient } from '@nekazari/sdk';

export interface Parcel {
  id?: string;
  name?: string;
  geometry?: {
    type: 'Polygon';
    coordinates: number[][][];
  };
  municipality?: string;
  province?: string;
  cadastralReference?: string;
  cropType?: string;
  area?: number;
  notes?: string;
  category?: string;
  ndviEnabled?: boolean;
}

// Auth is handled via httpOnly cookie (NKZClient sends credentials: 'include').
const getAuthToken = (): string | null => null;

// Get tenant ID from host auth context.
const getTenantId = (): string | null => {
  if (typeof window === 'undefined') return null;
  return (window as any).__nekazariAuthContext?.tenantId ?? null;
};

// Get API URL from runtime config
const getApiUrl = (): string => {
  if (typeof window !== 'undefined') {
    const env = (window as any).__ENV__;
    if (env?.VITE_API_URL) {
      return String(env.VITE_API_URL).replace(/\/$/, '');
    }
    if (env?.API_URL) {
      return String(env.API_URL).replace(/\/$/, '');
    }
    // 2. Derive from current origin: nekazari.{domain} → nkz.{domain}
    const origin = window.location.origin;
    if (origin.includes('nekazari.')) {
      return origin.replace('nekazari.', 'nkz.');
    }
    // 3. Localhost fallback for dev
    return origin;
  }
  return '';
};

class ParcelApiService {
  private client: NKZClient;

  constructor() {
    // baseUrl is the API root: writes go to the entity-manager parcel API,
    // reads (dedup lookup) go to Orion under /ngsi-ld/v1.
    this.client = new NKZClient({
      baseUrl: `${getApiUrl()}`,
      getToken: getAuthToken,
      getTenantId: getTenantId,
      defaultHeaders: {
        'Content-Type': 'application/json',
      },
    });
  }

  async createParcel(parcel: Partial<Parcel>): Promise<Parcel> {
    // entity-manager is the SOLE writer of AgriParcel. The FE sends flat domain
    // fields; the server owns the URN id, the NGSI-LD envelope, the @context and
    // dedup by cadastralReference. Never write Orion directly (the api-gateway
    // blocks direct AgriParcel writes).
    const payload: Record<string, any> = {
      category: parcel.category || 'cadastral',
      ndviEnabled: parcel.ndviEnabled !== undefined ? parcel.ndviEnabled : true,
    };
    if (parcel.geometry) {
      payload.geometry = {
        type: parcel.geometry.type || 'Polygon',
        coordinates: parcel.geometry.coordinates,
      };
    }
    if (parcel.name) payload.name = parcel.name;
    if (parcel.municipality && parcel.municipality.trim() !== '') payload.municipality = parcel.municipality;
    if (parcel.province && parcel.province.trim() !== '') payload.province = parcel.province;
    if (parcel.cadastralReference) payload.cadastralReference = parcel.cadastralReference;
    if (parcel.cropType) payload.cropType = parcel.cropType;
    if (parcel.area !== undefined && parcel.area !== null) payload.area = parcel.area;
    if (parcel.notes) payload.notes = parcel.notes;

    const response = await this.client.post<{ id: string; created: boolean }>(
      '/api/entities/parcels',
      payload,
    );

    return { ...parcel, id: response?.id };
  }

  async findByCadastralReference(cadastralReference: string): Promise<boolean> {
    const sanitizedRef = cadastralReference.replace(/"/g, '\\"');
    const q = `cadastralReference=="${sanitizedRef}"`;
    const qs = new URLSearchParams({
      type: 'AgriParcel',
      q,
      limit: '1',
    });
    // NKZClient uses fetch (no axios-style `params`). GET must not reuse default
    // `Content-Type: application/ld+json` without a body — that confuses some proxies
    // and the API gateway JSON parsing path. Reads hit Orion directly (allowed).
    const response = await this.client.get(`/ngsi-ld/v1/entities?${qs.toString()}`, {
      headers: {
        'Content-Type': 'application/json',
        Accept: 'application/ld+json',
        Link: `<https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"`,
      },
    });

    return Array.isArray(response) && response.length > 0;
  }
}

let _parcelApi: ParcelApiService | null = null;
export const getParcelApi = (): ParcelApiService => {
  if (!_parcelApi) _parcelApi = new ParcelApiService();
  return _parcelApi;
};

