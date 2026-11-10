import React, { useEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { useLocation } from 'react-router-dom';
import { useViewerOptional } from '@nekazari/sdk';
import { useTranslation } from '@nekazari/sdk';
import { getCadastralApi, CadastralData } from '../services/cadastralApi';
import { getParcelApi } from '../services/parcelApi';
import { CheckCircle, XCircle, Loader2, Clock } from 'lucide-react';
import { useCadastral } from '../context/CadastralContext';
import { CadastralConfirmDialog } from './CadastralConfirmDialog';
import { CadastralSelectionDialog } from './CadastralSelectionDialog';
import { SigpacSelectionPanel } from './SigpacSelectionPanel';
import {
  enclosureAt,
  createSigpacParcel,
  SigpacApiError,
  type EnclosureAt,
} from '../services/sigpacApi';

const SIGPAC_ENTITY_PREFIX = 'sigpac-selection:';

/** Error from the SIGPAC routes -> translation key (and its params). */
const sigpacErrorKey = (err: unknown): [string, Record<string, any>] => {
  if (err instanceof SigpacApiError) {
    if (err.status === 404) return ['sigpac.select.noEnclosure', {}];
    if (err.status === 409) return ['sigpac.select.taken', { ref: err.detail.reference }];
    if (err.status === 503) return ['sigpac.select.fega', {}];
    if (err.code === 'outside_coverage') return ['sigpac.select.outside', {}];
    if (err.code === 'not_contiguous') return ['sigpac.select.notContiguous', {}];
    if (err.code === 'too_many') return ['sigpac.select.tooMany', { max: err.detail.max }];
  }
  return ['sigpac.select.error', {}];
};

// Error types for specific UX messages
type ErrorType = 'not_found' | 'timeout' | 'network' | 'service_unavailable' | 'generic';

const getErrorMessage = (error: any, t: (key: string) => string): { message: string; type: ErrorType } => {
  // Check for timeout
  if (error.code === 'ECONNABORTED' || error.message?.includes('timeout')) {
    return {
      message: t('errors.timeout'),
      type: 'timeout'
    };
  }

  // Check for network error
  if (error.code === 'ERR_NETWORK' || !navigator.onLine) {
    return {
      message: t('errors.network'),
      type: 'network'
    };
  }

  // Check for 404 - not found
  if (error.response?.status === 404) {
    return {
      message: t('errors.notFound'),
      type: 'not_found'
    };
  }

  // Check for service unavailable
  if (error.response?.status === 503 || error.response?.status === 502) {
    return {
      message: t('errors.serviceUnavailable'),
      type: 'service_unavailable'
    };
  }

  // Generic error with server message if available
  const serverMessage = error.response?.data?.error || error.response?.data?.message;
  return {
    message: serverMessage || error.message || t('errors.generic'),
    type: 'generic'
  };
};

/**
 * Component that intercepts map clicks on the /entities page
 * and allows adding parcels with a single click
 */
export const CadastralMapClickHandler: React.FC = () => {
  const location = useLocation();
  const { t } = useTranslation('cadastral');
  const viewerContext = useViewerOptional();
  const cesiumViewer = viewerContext?.cesiumViewer;
  const isViewerReady = viewerContext?.isViewerReady !== false;
  const { isClickEnabled, clickMode } = useCadastral();
  const [selection, setSelection] = useState<EnclosureAt[]>([]);
  const [sigpacName, setSigpacName] = useState('');
  const [sigpacBusy, setSigpacBusy] = useState(false);
  const [creating, setCreating] = useState(false);
  const clickModeRef = useRef(clickMode);
  const clickEnabledRef = useRef(isClickEnabled);
  const selectionRef = useRef<EnclosureAt[]>([]);
  const sigpacBusyRef = useRef(false);
  const sigpacEntitiesRef = useRef<Map<string, any>>(new Map());
  const [isProcessing, setIsProcessing] = useState(false);
  const [elapsedSeconds, setElapsedSeconds] = useState(0);
  const [notification, setNotification] = useState<{
    type: 'success' | 'error' | 'warning';
    message: string;
  } | null>(null);
  const [pendingParcel, setPendingParcel] = useState<{
    data: CadastralData;
    area: number;
  } | null>(null);
  const [candidates, setCandidates] = useState<CadastralData[] | null>(null);
  const handlerRef = useRef<any>(null);
  const notificationTimeoutRef = useRef<NodeJS.Timeout | null>(null);
  const timerRef = useRef<NodeJS.Timeout | null>(null);
  const confirmingRef = useRef(false);
  const processingRef = useRef(false);
  const pendingParcelRef = useRef<{
    data: CadastralData;
    area: number;
  } | null>(null);
  const candidatesRef = useRef<CadastralData[] | null>(null);

  // Only activate on /entities page
  const isEntitiesPage = location.pathname === '/entities';

  // Keep refs in sync to avoid stale state inside Cesium click callback.
  useEffect(() => {
    processingRef.current = isProcessing;
  }, [isProcessing]);

  useEffect(() => {
    pendingParcelRef.current = pendingParcel;
  }, [pendingParcel]);

  useEffect(() => {
    candidatesRef.current = candidates;
  }, [candidates]);

  useEffect(() => {
    clickModeRef.current = clickMode;
    clickEnabledRef.current = isClickEnabled;
  }, [clickMode, isClickEnabled]);

  useEffect(() => {
    selectionRef.current = selection;
  }, [selection]);

  // Changing mode or turning the click off drops a half-made SIGPAC selection.
  useEffect(() => {
    setSelection([]);
    setSigpacName('');
  }, [clickMode, isClickEnabled]);

  // Draw each selected recinto; entities follow the selection.
  useEffect(() => {
    // @ts-ignore
    const Cesium = window.Cesium;
    if (!cesiumViewer || !Cesium) return;
    const drawn = sigpacEntitiesRef.current;
    const wanted = new Set(selection.map((e) => e.reference));
    for (const [ref, entity] of drawn) {
      if (!wanted.has(ref)) {
        cesiumViewer.entities.remove(entity);
        drawn.delete(ref);
      }
    }
    for (const e of selection) {
      if (drawn.has(e.reference)) continue;
      const hierarchy = createCesiumHierarchy(Cesium, e.geometry);
      if (!hierarchy) continue;
      const color = e.inParcel ? Cesium.Color.ORANGE : Cesium.Color.LIME;
      drawn.set(e.reference, cesiumViewer.entities.add({
        name: `${SIGPAC_ENTITY_PREFIX}${e.reference}`,
        polygon: {
          hierarchy,
          material: color.withAlpha(0.45),
          height: 0,
          classificationType: Cesium.ClassificationType.TERRAIN,
        },
      }));
    }
  }, [cesiumViewer, selection]);

  // Remove the drawn recintos when the component goes away.
  useEffect(() => () => {
    const drawn = sigpacEntitiesRef.current;
    if (cesiumViewer) for (const entity of drawn.values()) cesiumViewer.entities.remove(entity);
    drawn.clear();
  }, [cesiumViewer]);
  useEffect(() => {
    console.log('[CadastralMapClickHandler] useEffect triggered', {
      isEntitiesPage,
      hasCesiumViewer: !!cesiumViewer,
      isClickEnabled,
      isProcessing,
      hasPendingParcel: !!pendingParcel
    });

    // Only activate if click is enabled and we're on the entities page
    if (!isEntitiesPage || !cesiumViewer || !isClickEnabled) {
      console.log('[CadastralMapClickHandler] Conditions not met, cleaning up handler', {
        isEntitiesPage,
        hasCesiumViewer: !!cesiumViewer,
        isClickEnabled
      });
      if (handlerRef.current && !handlerRef.current.isDestroyed()) {
        handlerRef.current.destroy();
        handlerRef.current = null;
      }
      return;
    }

    // @ts-ignore
    const Cesium = window.Cesium;
    if (!Cesium) {
      console.warn('[CadastralMapClickHandler] Cesium not available on window');
      return;
    }

    console.log('[CadastralMapClickHandler] Setting up click handler on Cesium viewer', {
      canvas: cesiumViewer.scene?.canvas ? 'found' : 'missing'
    });

    // Create click handler
    const handler = new Cesium.ScreenSpaceEventHandler(cesiumViewer.scene.canvas);
    handlerRef.current = handler;

    handler.setInputAction(async (click: any) => {
      console.log('[CadastralMapClickHandler] Click detected!', {
        isProcessing: processingRef.current,
        hasPendingParcel: !!pendingParcelRef.current
      });

      // Check if we're processing a previous click or have a pending dialog
      if (processingRef.current || pendingParcelRef.current || candidatesRef.current) {
        console.log('[CadastralMapClickHandler] Click ignored (processing or pending)');
        return;
      }

      // Check if clicked on an existing entity
      const pickedObject = cesiumViewer.scene.pick(click.position);
      const pickedName: string = (Cesium.defined(pickedObject) && pickedObject.id?.name) || '';
      if (clickModeRef.current === 'sigpac' && pickedName.startsWith(SIGPAC_ENTITY_PREFIX)) {
        // A second click on a selected recinto removes it.
        const ref = pickedName.slice(SIGPAC_ENTITY_PREFIX.length);
        setSelection((prev) => prev.filter((e) => e.reference !== ref));
        return;
      }
      if (Cesium.defined(pickedObject) && pickedObject.id) {
        console.log('[CadastralMapClickHandler] Clicked on existing entity, ignoring');
        // Clicked on an entity, don't handle
        return;
      }

      // Get coordinates from click
      let cartesian = cesiumViewer.scene.pickPosition(click.position);
      if (!cartesian) {
        cartesian = cesiumViewer.camera.pickEllipsoid(click.position, cesiumViewer.scene.globe.ellipsoid);
      }

      if (!cartesian) {
        return;
      }

      const cartographic = Cesium.Cartographic.fromCartesian(cartesian);
      const longitude = Cesium.Math.toDegrees(cartographic.longitude);
      const latitude = Cesium.Math.toDegrees(cartographic.latitude);

      console.log('[CadastralMapClickHandler] Map clicked at:', { longitude, latitude });

      // Process the click
      if (clickModeRef.current === 'sigpac') {
        await handleSigpacClick(longitude, latitude);
        return;
      }
      await handleMapClick(longitude, latitude);
    }, Cesium.ScreenSpaceEventType.LEFT_CLICK);

    return () => {
      if (handlerRef.current && !handlerRef.current.isDestroyed()) {
        handlerRef.current.destroy();
        handlerRef.current = null;
      }
    };
  }, [isEntitiesPage, cesiumViewer, isClickEnabled]);

  // Render pending parcel on map
  useEffect(() => {
    if (!cesiumViewer || !isViewerReady || !pendingParcel) return;

    // @ts-ignore
    const Cesium = window.Cesium;
    if (!Cesium) return;

    let entity: any = null;

    try {
      console.log('[CadastralMapClickHandler] Rendering pending parcel geometry...', pendingParcel.data.geometry?.type);

      const geometry = pendingParcel.data.geometry;
      if (!geometry || !geometry.coordinates) return;

      // Handle Polygon and MultiPolygon
      const hierarchy = createCesiumHierarchy(Cesium, geometry);

      if (hierarchy) {
        entity = cesiumViewer.entities.add({
          name: 'Pending Parcel',
          polygon: {
            hierarchy: hierarchy,
            material: Cesium.Color.YELLOW.withAlpha(0.5),
            outline: true,
            outlineColor: Cesium.Color.YELLOW,
            outlineWidth: 2,
            height: 0, // Clamp to ground
            classificationType: Cesium.ClassificationType.TERRAIN // Ensure it renders on terrain
          }
        });

        cesiumViewer.flyTo(entity, { duration: 1.0, offset: new Cesium.HeadingPitchRange(0, -1.0, 1500) });
      }
    } catch (e) {
      console.error('[CadastralMapClickHandler] Error rendering pending parcel:', e);
    }

    return () => {
      if (entity && cesiumViewer && isViewerReady) {
        cesiumViewer.entities.remove(entity);
      }
    };
  }, [cesiumViewer, isViewerReady, pendingParcel]);

  // Helper to create Cesium hierarchy from GeoJSON
  const createCesiumHierarchy = (Cesium: any, geometry: any) => {
    if (geometry.type === 'Polygon') {
      return new Cesium.PolygonHierarchy(
        Cesium.Cartesian3.fromDegreesArray(geometry.coordinates[0].flat())
      );
    } else if (geometry.type === 'MultiPolygon') {
      // Flatten MultiPolygon to single hierarchy (simple approach) or return array?
      // Cesium Entity Polygon hierarchy supports holes but not disjoint polygons well in one entity 
      // unless we use a callback or composite. 
      // For simplicity, let's render the FIRST polygon of the MultiPolygon for visualization
      // Or merge them if relevant. Navarra usually returns 1 main polygon.
      if (geometry.coordinates.length > 0) {
        return new Cesium.PolygonHierarchy(
          Cesium.Cartesian3.fromDegreesArray(geometry.coordinates[0][0].flat())
        );
      }
    }
    return null;
  };

  const handleMapClick = async (longitude: number, latitude: number) => {
    processingRef.current = true;
    setIsProcessing(true);
    setNotification(null);
    setElapsedSeconds(0);

    // Start elapsed time counter
    timerRef.current = setInterval(() => {
      setElapsedSeconds(prev => prev + 1);
    }, 1000);

    try {
      console.log('[CadastralMapClickHandler] Querying cadastral service...');
      const cadastralData = await getCadastralApi().queryByCoordinates(longitude, latitude);

      // Clear timer on success
      if (timerRef.current) {
        clearInterval(timerRef.current);
        timerRef.current = null;
      }

      // Retrieve candidates list if available (or wrap single result)
      const candidatesList = cadastralData.candidates && cadastralData.candidates.length > 0
        ? cadastralData.candidates
        : [cadastralData];

      if (candidatesList.length > 1) {
        console.log(`[CadastralMapClickHandler] Found ${candidatesList.length} candidates, showing selection dialog`);
        candidatesRef.current = candidatesList;
        setCandidates(candidatesList);
        setIsProcessing(false);
        return;
      }

      // Single result flow
      const primary = candidatesList[0];
      await processSelectedCandidate(primary);

    } catch (error: any) {
      console.error('[CadastralMapClickHandler] Error:', error);

      // Clear timer on error
      if (timerRef.current) {
        clearInterval(timerRef.current);
        timerRef.current = null;
      }

      const { message, type } = getErrorMessage(error, t);
      setNotification({
        type: type === 'not_found' ? 'warning' : 'error',
        message,
      });
      clearNotificationAfterDelay(type === 'timeout' ? 8000 : 5000);
    } finally {
      processingRef.current = false;
      setIsProcessing(false);
      setElapsedSeconds(0);
      if (timerRef.current) {
        clearInterval(timerRef.current);
        timerRef.current = null;
      }
    }
  };

  const clearNotificationAfterDelay = (delay: number = 5000) => {
    if (notificationTimeoutRef.current) {
      clearTimeout(notificationTimeoutRef.current);
    }
    notificationTimeoutRef.current = setTimeout(() => {
      setNotification(null);
    }, delay);
  };

  const processSelectedCandidate = async (data: CadastralData) => {
    // Validate geometry
    const hasGeometry = data.geometry &&
      (data.geometry.type === 'Polygon' || data.geometry.type === 'MultiPolygon') &&
      data.geometry.coordinates &&
      data.geometry.coordinates.length > 0;

    if (!hasGeometry) {
      console.warn('[CadastralMapClickHandler] Parcel missing geometry:', data);
      setNotification({
        type: 'warning',
        message: t('warnings.noGeometry'),
      });
      clearNotificationAfterDelay();
      setIsProcessing(false);
      return;
    }

    // Calculate area
    const area = calculatePolygonAreaHectares(data.geometry!);

    // Show confirmation
    pendingParcelRef.current = { data, area };
    candidatesRef.current = null;
    setPendingParcel({ data, area });
    setCandidates(null);
    setIsProcessing(false);
  };

  const handleSelectCandidate = (candidate: CadastralData) => {
    // If we need to fetch geometry specifically for this candidate, we could do it here.
    // However, backend should have pre-warmed geometries for top candidates.
    // If geometry is missing, processSelectedCandidate will show warning.
    processSelectedCandidate(candidate);
  };

  const handleCancelSelection = () => {
    candidatesRef.current = null;
    processingRef.current = false;
    setCandidates(null);
    setIsProcessing(false);
  };

  // Calculate polygon area in hectares using equirectangular projection
  // Supports Polygon and MultiPolygon
  const calculatePolygonAreaHectares = (geometry: { type: string; coordinates: any[] }): number => {
    if (!geometry || !geometry.coordinates || geometry.coordinates.length === 0) {
      return 0;
    }

    if (geometry.type === 'Polygon') {
      return calculateSinglePolygonArea(geometry.coordinates);
    }

    if (geometry.type === 'MultiPolygon') {
      return geometry.coordinates.reduce((total, polyCoords) => {
        return total + calculateSinglePolygonArea(polyCoords);
      }, 0);
    }

    return 0;
  };

  const calculateSinglePolygonArea = (coordinates: number[][][]): number => {
    const ring = coordinates[0]; // First ring (exterior)
    if (!ring || ring.length < 4) {
      return 0;
    }

    // Remove duplicate closing point for calculations
    const coords = ring.slice(0, -1);
    if (coords.length < 3) {
      return 0;
    }

    // Calculate average latitude for projection
    const lat0 = coords.reduce((sum, [, lat]) => sum + lat, 0) / coords.length;
    const lat0Rad = (lat0 * Math.PI) / 180;

    const EARTH_RADIUS_METERS = 6378137;

    // Project coordinates to meters using equirectangular projection
    const projected = coords.map(([lon, lat]) => {
      const x = ((lon * Math.PI) / 180) * EARTH_RADIUS_METERS * Math.cos(lat0Rad);
      const y = ((lat * Math.PI) / 180) * EARTH_RADIUS_METERS;
      return [x, y] as [number, number];
    });

    // Shoelace formula
    let area = 0;
    for (let i = 0; i < projected.length; i++) {
      const [x1, y1] = projected[i];
      const [x2, y2] = projected[(i + 1) % projected.length];
      area += x1 * y2 - x2 * y1;
    }

    const areaSqMeters = Math.abs(area) / 2;
    return Number((areaSqMeters / 10_000).toFixed(4)); // Convert to hectares
  };

  const handleConfirmParcel = async () => {
    if (!pendingParcel || confirmingRef.current) return;
    confirmingRef.current = true;

    setIsProcessing(true);
    // Keep pendingParcel until creation finishes so CadastralConfirmDialog stays mounted
    // and map-click guard (pendingParcel | isProcessing) remains effective.

    try {
      const { data: cadastralData, area } = pendingParcel;
      const cadastralReference = cadastralData.cadastralReference || '';

      if (cadastralReference) {
        const alreadyExists = await getParcelApi().findByCadastralReference(cadastralReference);
        if (alreadyExists) {
          setNotification({
            type: 'warning',
            message: t('warnings.alreadyExists', { ref: cadastralReference }),
          });
          clearNotificationAfterDelay();
          pendingParcelRef.current = null;
          setPendingParcel(null);
          return;
        }
      }

      console.log('[CadastralMapClickHandler] Creating parcel...');
      const newParcel = {
        name:
          cadastralData.cadastralReference ||
          t('status.defaultParcelName', { municipality: cadastralData.municipality || '' }),
        geometry: cadastralData.geometry!,
        municipality: cadastralData.municipality || '',
        province: cadastralData.province || '',
        cadastralReference: cadastralData.cadastralReference,
        cropType: '', // User can edit later
        area: area,
        category: 'cadastral',
        ndviEnabled: true,
      };

      await getParcelApi().createParcel(newParcel);

      pendingParcelRef.current = null;
      setPendingParcel(null);

      setNotification({
        type: 'success',
        message: t('status.parcelCreated', {
          ref: cadastralData.cadastralReference,
          area: area.toFixed(2),
        }),
      });

      clearNotificationAfterDelay();

      // Signal host to reload entities
      setTimeout(() => {
        (viewerContext as any)?.triggerEntityRefresh?.();
      }, 500);
    } catch (error: any) {
      console.error('[CadastralMapClickHandler] Error creating parcel:', error);
      const errorMessage =
        error.response?.data?.error ||
        error.response?.data?.message ||
        error.message ||
        t('status.createParcelError');
      setNotification({
        type: 'error',
        message: errorMessage,
      });
      clearNotificationAfterDelay();
    } finally {
      setIsProcessing(false);
      confirmingRef.current = false;
    }
  };

  const handleSigpacClick = async (longitude: number, latitude: number) => {
    if (sigpacBusyRef.current) return;
    sigpacBusyRef.current = true;
    setSigpacBusy(true);
    try {
      const enclosure = await enclosureAt(longitude, latitude);
      // The user may have switched mode or turned the click off while waiting.
      if (clickModeRef.current !== 'sigpac' || !clickEnabledRef.current) return;
      if (selectionRef.current.some((e) => e.reference === enclosure.reference)) {
        setSelection((prev) => prev.filter((e) => e.reference !== enclosure.reference));
      } else {
        setSelection((prev) => [...prev, enclosure]);
      }
    } catch (err) {
      const [key, params] = sigpacErrorKey(err);
      setNotification({ type: 'warning', message: String(t(key, params)) });
      clearNotificationAfterDelay();
    } finally {
      sigpacBusyRef.current = false;
      setSigpacBusy(false);
    }
  };

  const handleCreateSigpacParcel = async () => {
    const refs = selection.map((e) => e.reference);
    if (refs.length === 0) return;
    setCreating(true);
    try {
      const result = await createSigpacParcel(refs, sigpacName.trim() || undefined);
      const name = sigpacName.trim() || refs[0] + (refs.length > 1 ? ` +${refs.length - 1}` : '');
      setNotification({
        type: 'success',
        message: result.sigpacPending ? t('sigpac.select.pending') : t('sigpac.select.created', { name }),
      });
      setSelection([]);
      setSigpacName('');
      // Signal host to reload entities, as the cadastral flow does.
      setTimeout(() => {
        (viewerContext as any)?.triggerEntityRefresh?.();
      }, 500);
    } catch (err) {
      const [key, params] = sigpacErrorKey(err);
      setNotification({ type: 'error', message: String(t(key, params)) });
    } finally {
      setCreating(false);
      clearNotificationAfterDelay();
    }
  };

  const handleCancelSigpac = () => {
    setSelection([]);
    setSigpacName('');
  };

  const handleCancelParcel = () => {
    pendingParcelRef.current = null;
    processingRef.current = false;
    setPendingParcel(null);
    setIsProcessing(false);
  };

  if (!isEntitiesPage) {
    return null;
  }

  // Rendered on <body>: the map-layer slot sits in a low stacking context, under the side rails.
  return createPortal(
    <>
      {/* Processing indicator with elapsed time */}
      {isProcessing && !pendingParcel && (
        <div className="fixed top-4 right-4 z-50 bg-white rounded-lg shadow-lg border border-gray-200 p-4 flex items-center gap-3 min-w-[300px]">
          <Loader2 className="w-5 h-5 text-blue-600 animate-spin" />
          <div className="flex flex-col">
            <span className="text-sm text-gray-700">{t('status.querying')}</span>
            {elapsedSeconds > 0 && (
              <span className="text-xs text-gray-500 flex items-center gap-1">
                <Clock className="w-3 h-3" />
                {elapsedSeconds}s
                {elapsedSeconds >= 5 && ` - ${t('status.slowService')}`}
              </span>
            )}
          </div>
        </div>
      )}

      {/* SIGPAC lookup in progress */}
      {sigpacBusy && (
        <div className="fixed top-4 right-4 z-50 bg-white rounded-lg shadow-lg border border-gray-200 p-4 flex items-center gap-3">
          <Loader2 className="w-5 h-5 text-blue-600 animate-spin" />
          <span className="text-sm text-gray-700">{t('sigpac.select.querying')}</span>
        </div>
      )}

      {/* SIGPAC selection */}
      {clickMode === 'sigpac' && selection.length > 0 && (
        <SigpacSelectionPanel
          selection={selection}
          name={sigpacName}
          onNameChange={setSigpacName}
          onRemove={(ref) => setSelection((prev) => prev.filter((e) => e.reference !== ref))}
          onCreate={handleCreateSigpacParcel}
          onCancel={handleCancelSigpac}
          creating={creating}
        />
      )}

      {/* Selection Dialog */}
      {candidates && (
        <CadastralSelectionDialog
          candidates={candidates}
          onSelect={handleSelectCandidate}
          onCancel={handleCancelSelection}
        />
      )}

      {/* Confirmation Dialog */}
      {pendingParcel && (
        <CadastralConfirmDialog
          cadastralData={pendingParcel.data}
          area={pendingParcel.area}
          onConfirm={handleConfirmParcel}
          onCancel={handleCancelParcel}
          isProcessing={isProcessing}
        />
      )}

      {/* Notification */}
      {notification && (
        <div className={`fixed top-4 right-4 z-50 bg-white rounded-lg shadow-lg border ${notification.type === 'success'
          ? 'border-green-200'
          : notification.type === 'warning'
            ? 'border-yellow-200'
            : 'border-red-200'
          } p-4 flex items-center gap-3 min-w-[300px] max-w-[400px]`}>
          {notification.type === 'success' ? (
            <CheckCircle className="w-5 h-5 text-green-600 flex-shrink-0" />
          ) : notification.type === 'warning' ? (
            <XCircle className="w-5 h-5 text-yellow-600 flex-shrink-0" />
          ) : (
            <XCircle className="w-5 h-5 text-red-600 flex-shrink-0" />
          )}
          <span className={`text-sm ${notification.type === 'success'
            ? 'text-green-700'
            : notification.type === 'warning'
              ? 'text-yellow-700'
              : 'text-red-700'
            }`}>
            {notification.message}
          </span>
        </div>
      )}
    </>,
    document.body,
  );
};

