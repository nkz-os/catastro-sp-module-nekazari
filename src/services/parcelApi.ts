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
    this.client = new NKZClient({
      baseUrl: `${getApiUrl()}/ngsi-ld/v1`,
      getToken: getAuthToken,
      getTenantId: getTenantId,
      defaultHeaders: {
        'Content-Type': 'application/ld+json',
      },
    });
  }

  async createParcel(parcel: Partial<Parcel>): Promise<Parcel> {
    // Generate entity ID
    const entityId = parcel.id || `urn:ngsi-ld:AgriParcel:${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;

    // Build NGSI-LD entity (same format as host platform)
    const entity: any = {
      id: entityId,
      type: 'AgriParcel',
      category: {
        type: 'Property',
        value: parcel.category || 'cadastral',
      },
    };

    // Add location (GeoProperty)
    if (parcel.geometry) {
      entity.location = {
        type: 'GeoProperty',
        value: {
          type: parcel.geometry.type || 'Polygon',
          coordinates: parcel.geometry.coordinates,
        },
      };
    }

    // Add name
    if (parcel.name) {
      entity.name = {
        type: 'Property',
        value: parcel.name,
      };
    }

    // Add municipality (only if not empty)
    if (parcel.municipality && parcel.municipality.trim() !== '') {
      entity.municipality = {
        type: 'Property',
        value: parcel.municipality,
      };
    }

    // Add province (only if not empty)
    if (parcel.province && parcel.province.trim() !== '') {
      entity.province = {
        type: 'Property',
        value: parcel.province,
      };
    }

    // Add cadastral reference
    if (parcel.cadastralReference) {
      entity.cadastralReference = {
        type: 'Property',
        value: parcel.cadastralReference,
      };
    }

    // Add crop type
    if (parcel.cropType) {
      entity.cropType = {
        type: 'Property',
        value: parcel.cropType,
      };
    }

    // Add area
    if (parcel.area !== undefined && parcel.area !== null) {
      entity.area = {
        type: 'Property',
        value: parcel.area,
      };
    }

    // Add NDVI enabled flag
    entity.ndviEnabled = {
      type: 'Property',
      value: parcel.ndviEnabled !== undefined ? parcel.ndviEnabled : true,
    };

    // Add notes
    if (parcel.notes) {
      entity.notes = {
        type: 'Property',
        value: parcel.notes,
      };
    }

    // Use the parcel API from the host platform (same as parcelApi.createParcel in host)
    // Context URL is typically https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld
    const response = await this.client.post('/entities', entity, {
      headers: {
        'Link': `<https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"`,
      },
    });

    return response;
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
    // and the API gateway JSON parsing path.
    const response = await this.client.get(`/entities?${qs.toString()}`, {
      headers: {
        'Content-Type': 'application/json',
        Accept: 'application/ld+json',
        Link: `<https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"`,
      },
    });

    return Array.isArray(response) && response.length > 0;
  }
}

export const parcelApi = new ParcelApiService();

