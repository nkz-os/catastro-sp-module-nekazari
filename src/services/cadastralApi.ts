import { NKZClient } from '@nekazari/sdk';

export interface CadastralData {
  cadastralReference: string;
  municipality: string;
  province: string;
  address: string;
  coordinates: { lon: number; lat: number };
  region: 'spain' | 'navarra' | 'euskadi';
  type?: string;
  geometry?: {
    type: 'Polygon';
    coordinates: number[][][];
  };
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
    // 1. Host runtime config (entrypoint.sh sets VITE_API_URL, not API_URL)
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

class CadastralApiService {
  private client: NKZClient;

  constructor() {
    this.client = new NKZClient({
      baseUrl: `${getApiUrl()}/api/cadastral-api`,
      getToken: getAuthToken,
      getTenantId: getTenantId,
    });
  }

  async queryByCoordinates(
    longitude: number,
    latitude: number,
    srs: string = '4326'
  ): Promise<CadastralData> {
    const response = await this.client.post<CadastralData>('/parcels/query-by-coordinates', {
      longitude,
      latitude,
      srs,
    });
    return response;
  }

  async getBuildings(params: { bbox?: string; parcelId?: string }): Promise<{
    type: string;
    features: unknown[];
  }> {
    if (params.parcelId) {
      return this.getParcelBuildings(params.parcelId);
    }
    const qs = new URLSearchParams();
    if (params.bbox) qs.set('bbox', params.bbox);
    return this.client.get(`/buildings?${qs.toString()}`, {
      headers: { Accept: 'application/json' },
    });
  }

  async getParcelBuildings(parcelId: string): Promise<{
    type: string;
    features: unknown[];
  }> {
    const id = parcelId.includes('AgriParcel:')
      ? parcelId.split(':').pop()!
      : parcelId;
    return this.client.get(`/parcels/${encodeURIComponent(id)}/buildings`, {
      headers: { Accept: 'application/json' },
    });
  }
}

let _cadastralApi: CadastralApiService | null = null;
export const getCadastralApi = (): CadastralApiService => {
  if (!_cadastralApi) _cadastralApi = new CadastralApiService();
  return _cadastralApi;
};

