import React, { useCallback, useEffect, useRef, useState } from 'react';
import { useViewerOptional } from '@nekazari/sdk';
import { getCadastralApi } from '../services/cadastralApi';

interface Props {
  visible?: boolean;
  parcelId?: string;
}

const DEFAULT_BUILDING_HEIGHT_M = 5;

export const CadastralBuildingLayer: React.FC<Props> = ({ visible, parcelId }) => {
  const viewerCtx = useViewerOptional();
  const viewer = viewerCtx?.cesiumViewer ?? null;
  const isViewerReady = viewerCtx?.isViewerReady !== false;
  const dsRef = useRef<any>(null);
  const [internalVisible, setInternalVisible] = useState(false);

  // Listen for toggle events from CadastralBuildingsToggle
  useEffect(() => {
    const handler = (e: CustomEvent) => {
      setInternalVisible(e.detail.visible);
    };
    window.addEventListener('cadastral:buildings-toggle', handler as EventListener);
    return () => window.removeEventListener('cadastral:buildings-toggle', handler as EventListener);
  }, []);

  const isVisible = visible !== undefined ? visible : internalVisible;

  const loadBuildings = useCallback(async () => {
    // A destroyed viewer is still truthy; touching .dataSources/.camera then
    // throws "_cesiumWidget is undefined". Guard on isDestroyed().
    if (!viewer || viewer.isDestroyed?.() || !isViewerReady || !isVisible) return;
    const Cesium = (window as any).Cesium;
    if (!Cesium) return;

    // Clean up previous data source
    if (dsRef.current && !viewer.isDestroyed?.()) {
      try {
        viewer.dataSources.remove(dsRef.current);
      } catch { /* viewer torn down mid-flight */ }
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
          const west = Cesium.Math.toDegrees(rect.west);
          const south = Cesium.Math.toDegrees(rect.south);
          const east = Cesium.Math.toDegrees(rect.east);
          const north = Cesium.Math.toDegrees(rect.north);
          params.set('bbox', `${west},${south},${east},${north}`);
        }
      }

      if (!params.has('bbox') && !params.has('parcel_id')) return;

      const geojson = await getCadastralApi().getBuildings({
        bbox: params.get('bbox') ?? undefined,
        parcelId: params.get('parcel_id') ?? undefined,
      });
      if (!geojson.features || geojson.features.length === 0) return;
      if (viewer.isDestroyed?.()) return;

      await ds.load(geojson, { clampToGround: false });
      if (viewer.isDestroyed?.()) return;

      // Apply extrudedHeight styling to each polygon entity
      const entities = ds.entities.values;
      for (let i = 0; i < entities.length; i++) {
        const entity = entities[i];
        if (!entity.polygon) continue;
        const props = entity.properties;
        const now = Cesium.JulianDate.now();
        const rawHeight = props?.height?.getValue?.(now);
        const extrudedHeight =
          typeof rawHeight === 'number' && rawHeight > 0 ? rawHeight : DEFAULT_BUILDING_HEIGHT_M;
        entity.polygon = new Cesium.PolygonGraphics({
          hierarchy: entity.polygon.hierarchy,
          heightReference: Cesium.HeightReference.CLAMP_TO_GROUND,
          extrudedHeight,
          extrudedHeightReference: Cesium.HeightReference.RELATIVE_TO_GROUND,
          material: Cesium.Color.fromCssColorString('#94a3b8').withAlpha(0.65),
          outline: true,
          outlineColor: Cesium.Color.fromCssColorString('#475569'),
          outlineWidth: 1,
        });
      }

      viewer.dataSources.add(ds);
    } catch (err) {
      console.error('[CadastralBuildingLayer] Failed to load buildings:', err);
    }
  }, [viewer, isVisible, isViewerReady, parcelId]);

  useEffect(() => {
    if (!viewer || viewer.isDestroyed?.() || !isViewerReady) return;

    const onCameraMoveEnd = () => {
      if (isVisible) loadBuildings();
    };
    viewer.camera.moveEnd.addEventListener(onCameraMoveEnd);
    return () => {
      if (!viewer.isDestroyed?.()) {
        viewer.camera.moveEnd.removeEventListener(onCameraMoveEnd);
      }
    };
  }, [viewer, isViewerReady, isVisible, loadBuildings]);

  useEffect(() => {
    if (!isVisible) {
      if (dsRef.current && viewer && !viewer.isDestroyed?.()) {
        try {
          viewer.dataSources.remove(dsRef.current);
        } catch { /* viewer torn down */ }
        dsRef.current = null;
      }
      return;
    }

    loadBuildings();
    return () => {
      if (dsRef.current && viewer && !viewer.isDestroyed?.()) {
        try {
          viewer.dataSources.remove(dsRef.current);
        } catch { /* viewer torn down */ }
        dsRef.current = null;
      }
    };
  }, [loadBuildings, viewer, isViewerReady, isVisible]);

  return null; // invisible — renders via Cesium data sources
};

export default CadastralBuildingLayer;
