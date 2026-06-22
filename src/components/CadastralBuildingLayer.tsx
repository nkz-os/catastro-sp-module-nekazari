import React, { useCallback, useEffect, useRef, useState } from 'react';
import { useViewerOptional } from '@nekazari/sdk';

interface Props {
  visible?: boolean;
  parcelId?: string;
}

const API_BASE = (import.meta as any).env?.VITE_API_URL || 'https://nkz.robotika.cloud';

export const CadastralBuildingLayer: React.FC<Props> = ({ visible = true, parcelId }) => {
  const viewerCtx = useViewerOptional();
  const viewer = viewerCtx?.cesiumViewer ?? null;
  const dsRef = useRef<any>(null);
  const [internalVisible, setInternalVisible] = useState(visible);

  // Listen for toggle events from CadastralBuildingsToggle
  useEffect(() => {
    const handler = (e: CustomEvent) => {
      setInternalVisible(e.detail.visible);
    };
    window.addEventListener('cadastral:buildings-toggle', handler as EventListener);
    return () => window.removeEventListener('cadastral:buildings-toggle', handler as EventListener);
  }, []);

  const isVisible = visible ?? internalVisible;

  const loadBuildings = useCallback(async () => {
    if (!viewer || !isVisible) return;
    const Cesium = (window as any).Cesium;
    if (!Cesium) return;

    // Clean up previous data source
    if (dsRef.current) {
      viewer.dataSources.remove(dsRef.current);
      dsRef.current = null;
    }

    const ds = new Cesium.GeoJsonDataSource('catastro-buildings-3d');
    dsRef.current = ds;

    try {
      const params = new URLSearchParams();
      if (parcelId) {
        params.set('parcel_id', parcelId);
      } else {
        const rect = viewer.camera.computeViewRectangle();
        if (rect) {
          params.set('bbox', `${rect.west},${rect.south},${rect.east},${rect.north}`);
        }
      }

      if (!params.has('bbox') && !params.has('parcel_id')) return;

      // Get auth token from cookie
      const getToken = () => {
        const match = document.cookie.match(/(?:^|;\s*)nkz_token=([^;]*)/);
        return match ? match[1] : '';
      };
      const token = getToken();

      const resp = await fetch(`${API_BASE}/api/cadastral-api/buildings?${params}`, {
        credentials: 'include',
        headers: token ? { Authorization: `Bearer ${token}` } : {},
      });
      if (!resp.ok) return;
      const geojson = await resp.json();
      if (!geojson.features || geojson.features.length === 0) return;

      await ds.load(geojson, { clampToGround: true });

      // Apply extrudedHeight styling to each entity
      const entities = ds.entities.values;
      for (let i = 0; i < entities.length; i++) {
        const entity = entities[i];
        const props = entity.properties;
        if (!props) continue;
        const height = props.height?.getValue?.(Cesium.JulianDate.now());
        if (height && height > 0) {
          entity.polygon = new Cesium.PolygonGraphics({
            extrudedHeight: height,
            material: Cesium.Color.fromCssColorString('#94a3b8').withAlpha(0.65),
            outline: true,
            outlineColor: Cesium.Color.fromCssColorString('#475569'),
            outlineWidth: 1,
          });
        }
      }

      viewer.dataSources.add(ds);
    } catch (err) {
      console.error('[CadastralBuildingLayer] Failed to load buildings:', err);
    }
  }, [viewer, isVisible, parcelId]);

  useEffect(() => {
    loadBuildings();
    return () => {
      if (dsRef.current && viewer) {
        viewer.dataSources.remove(dsRef.current);
        dsRef.current = null;
      }
    };
  }, [loadBuildings, viewer]);

  return null; // invisible — renders via Cesium data sources
};

export default CadastralBuildingLayer;
