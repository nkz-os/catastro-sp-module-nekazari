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
}

export const cadastralApi = new CadastralApiService();

