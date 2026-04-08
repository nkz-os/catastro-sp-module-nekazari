import React, { useState, useRef, useEffect, useCallback } from 'react';
import { Search, MapPin, X, Loader2 } from 'lucide-react';
import { useViewerOptional } from '@nekazari/sdk';
import { useTranslation } from '@nekazari/sdk';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface NominatimResult {
    place_id: number;
    display_name: string;
    lat: string;
    lon: string;
    boundingbox: [string, string, string, string]; // [south, north, west, east]
    type: string;
    class: string;
    address?: {
        city?: string;
        town?: string;
        village?: string;
        municipality?: string;
        county?: string;
        state?: string;
        postcode?: string;
        country?: string;
    };
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Get a human-friendly type badge for the result */
const getTypeBadge = (result: NominatimResult, t: (key: string) => string): string => {
    const addr = result.address;
    if (addr?.postcode && result.class === 'place') return t('geosearch.typeBadge.postcode');
    if (addr?.city || addr?.town) return t('geosearch.typeBadge.city');
    if (addr?.village) return t('geosearch.typeBadge.village');
    if (addr?.municipality) return t('geosearch.typeBadge.municipality');
    if (addr?.county) return t('geosearch.typeBadge.county');
    if (addr?.state) return t('geosearch.typeBadge.state');
    if (result.type === 'administrative') return t('geosearch.typeBadge.region');
    return t('geosearch.typeBadge.place');
};

/** Get the short display name (city + province) instead of the full Nominatim string */
const getShortName = (result: NominatimResult): string => {
    const addr = result.address;
    if (!addr) return result.display_name;

    const place = addr.city || addr.town || addr.village || addr.municipality || '';
    const region = addr.state || addr.county || '';

    if (place && region) return `${place}, ${region}`;
    if (place) return place;
    if (region) return region;

    // Fallback: first two segments of display_name
    const parts = result.display_name.split(', ');
    return parts.slice(0, 2).join(', ');
};

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

/**
 * Floating geocoding search bar for the CesiumJS map.
 * Calls OpenStreetMap Nominatim to resolve locations and flies the camera
 * to the selected result's bounding box.
 *
 * Rendered via the `map-layer` slot of the catastro-spain module.
 */
export const GeoSearchBar: React.FC = () => {
    const { t } = useTranslation('cadastral');
    const viewerContext = useViewerOptional();
    const cesiumViewer = viewerContext?.cesiumViewer;

    const [isExpanded, setIsExpanded] = useState(false);
    const [query, setQuery] = useState('');
    const [results, setResults] = useState<NominatimResult[]>([]);
    const [isLoading, setIsLoading] = useState(false);
    const [activeIndex, setActiveIndex] = useState(-1);

    const inputRef = useRef<HTMLInputElement>(null);
    const containerRef = useRef<HTMLDivElement>(null);
    const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);

    // -----------------------------------------------------------------------
    // Nominatim API
    // -----------------------------------------------------------------------

    const searchNominatim = useCallback(async (q: string) => {
        if (q.length < 3) {
            setResults([]);
            return;
        }

        setIsLoading(true);
        try {
            const url =
                `https://nominatim.openstreetmap.org/search?` +
                `q=${encodeURIComponent(q)}` +
                `&format=json` +
                `&countrycodes=es` +
                `&limit=5` +
                `&addressdetails=1`;

            const res = await fetch(url, {
                headers: {
                    'Accept': 'application/json',
                    // Nominatim usage policy requires a descriptive User-Agent
                    'User-Agent': 'Nekazari/1.0 (https://nekazari.robotika.cloud)',
                },
            });

            if (!res.ok) throw new Error(`Nominatim ${res.status}`);
            const data: NominatimResult[] = await res.json();
            setResults(data);
            setActiveIndex(-1);
        } catch (err) {
            console.warn('[GeoSearchBar] Nominatim error:', err);
            setResults([]);
        } finally {
            setIsLoading(false);
        }
    }, []);

    // -----------------------------------------------------------------------
    // Debounced search
    // -----------------------------------------------------------------------

    useEffect(() => {
        if (debounceRef.current) clearTimeout(debounceRef.current);

        if (query.length < 3) {
            setResults([]);
            return;
        }

        debounceRef.current = setTimeout(() => {
            searchNominatim(query);
        }, 350);

        return () => {
            if (debounceRef.current) clearTimeout(debounceRef.current);
        };
    }, [query, searchNominatim]);

    // -----------------------------------------------------------------------
    // Camera fly-to
    // -----------------------------------------------------------------------

    const flyToResult = useCallback(
        (result: NominatimResult) => {
            if (!cesiumViewer) return;

            // @ts-ignore - Cesium on window
            const Cesium = window.Cesium;
            if (!Cesium) return;

            const [south, north, west, east] = result.boundingbox.map(Number);

            cesiumViewer.camera.flyTo({
                destination: Cesium.Rectangle.fromDegrees(west, south, east, north),
                duration: 1.5,
            });

            // Collapse after selection
            setQuery('');
            setResults([]);
            setIsExpanded(false);
        },
        [cesiumViewer],
    );

    // -----------------------------------------------------------------------
    // Keyboard navigation
    // -----------------------------------------------------------------------

    const handleKeyDown = (e: React.KeyboardEvent) => {
        if (e.key === 'ArrowDown') {
            e.preventDefault();
            setActiveIndex((prev) => Math.min(prev + 1, results.length - 1));
        } else if (e.key === 'ArrowUp') {
            e.preventDefault();
            setActiveIndex((prev) => Math.max(prev - 1, 0));
        } else if (e.key === 'Enter' && activeIndex >= 0 && results[activeIndex]) {
            e.preventDefault();
            flyToResult(results[activeIndex]);
        } else if (e.key === 'Escape') {
            e.preventDefault();
            setResults([]);
            setQuery('');
            setIsExpanded(false);
        }
    };

    // -----------------------------------------------------------------------
    // Click-outside to close
    // -----------------------------------------------------------------------

    useEffect(() => {
        const handler = (e: MouseEvent) => {
            if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
                setResults([]);
                if (!query) setIsExpanded(false);
            }
        };
        document.addEventListener('mousedown', handler);
        return () => document.removeEventListener('mousedown', handler);
    }, [query]);

    // Focus input when expanded
    useEffect(() => {
        if (isExpanded) inputRef.current?.focus();
    }, [isExpanded]);

    // -----------------------------------------------------------------------
    // Don't render if no viewer
    // -----------------------------------------------------------------------
    if (!cesiumViewer) return null;

    // -----------------------------------------------------------------------
    // Render — collapsed icon button
    // -----------------------------------------------------------------------
    if (!isExpanded) {
        return (
            <div className="absolute top-4 left-1/2 -translate-x-1/2 z-[1000]">
                <button
                    onClick={() => setIsExpanded(true)}
                    className="
            flex items-center justify-center
            w-10 h-10 rounded-xl
            bg-white/90 backdrop-blur-md
            border border-slate-200
            shadow-lg
            text-slate-600
            hover:bg-white hover:text-blue-600 hover:border-blue-300
            transition-all duration-200
          "
                    title={t('geosearch.searchLocation')}
                >
                    <Search className="w-5 h-5" />
                </button>
            </div>
        );
    }

    // -----------------------------------------------------------------------
    // Render — expanded search bar
    // -----------------------------------------------------------------------
    return (
        <div
            ref={containerRef}
            className="absolute top-4 left-1/2 -translate-x-1/2 z-[1000] w-80 pointer-events-auto"
        >
            {/* Input container */}
            <div
                className="
          flex items-center gap-2
          bg-white/95 backdrop-blur-md
          border border-slate-200
          rounded-xl shadow-lg
          px-3 py-2
          focus-within:border-blue-400 focus-within:ring-2 focus-within:ring-blue-100
          transition-all duration-200
        "
            >
                <Search className="w-4 h-4 text-slate-400 flex-shrink-0" />
                <input
                    ref={inputRef}
                    type="text"
                    value={query}
                    onChange={(e) => setQuery(e.target.value)}
                    onKeyDown={handleKeyDown}
                    placeholder={t('geosearch.placeholder')}
                    className="
            flex-1 bg-transparent
            text-sm text-slate-800
            placeholder:text-slate-400
            outline-none
          "
                    autoComplete="off"
                />
                {isLoading && (
                    <Loader2 className="w-4 h-4 text-blue-500 animate-spin flex-shrink-0" />
                )}
                {query && !isLoading && (
                    <button
                        onClick={() => {
                            setQuery('');
                            setResults([]);
                            inputRef.current?.focus();
                        }}
                        className="text-slate-400 hover:text-slate-600 transition-colors"
                    >
                        <X className="w-4 h-4" />
                    </button>
                )}
                <button
                    onClick={() => {
                        setQuery('');
                        setResults([]);
                        setIsExpanded(false);
                    }}
                    className="
            ml-1 p-1 rounded-lg
            text-slate-400 hover:text-slate-600 hover:bg-slate-100
            transition-colors
          "
                    title={t('geosearch.close')}
                >
                    <X className="w-3.5 h-3.5" />
                </button>
            </div>

            {/* Results dropdown */}
            {results.length > 0 && (
                <div
                    className="
            mt-1.5
            bg-white/95 backdrop-blur-md
            border border-slate-200
            rounded-xl shadow-lg
            overflow-hidden
            divide-y divide-slate-100
          "
                >
                    {results.map((result, idx) => (
                        <button
                            key={result.place_id}
                            onClick={() => flyToResult(result)}
                            onMouseEnter={() => setActiveIndex(idx)}
                            className={`
                w-full flex items-start gap-2.5 px-3 py-2.5
                text-left text-sm transition-colors
                ${idx === activeIndex
                                    ? 'bg-blue-50 text-blue-800'
                                    : 'text-slate-700 hover:bg-slate-50'
                                }
              `}
                        >
                            <MapPin
                                className={`w-4 h-4 mt-0.5 flex-shrink-0 ${idx === activeIndex ? 'text-blue-500' : 'text-slate-400'
                                    }`}
                            />
                            <div className="flex-1 min-w-0">
                                <div className="font-medium truncate">
                                    {getShortName(result)}
                                </div>
                                <div className="text-xs text-slate-500 truncate mt-0.5">
                                    {result.display_name}
                                </div>
                            </div>
                            <span
                                className={`
                  text-[10px] font-semibold uppercase tracking-wider
                  px-1.5 py-0.5 rounded-md flex-shrink-0 mt-0.5
                  ${idx === activeIndex
                                        ? 'bg-blue-100 text-blue-600'
                                        : 'bg-slate-100 text-slate-500'
                                    }
                `}
                            >
                                {getTypeBadge(result, t)}
                            </span>
                        </button>
                    ))}
                </div>
            )}

            {/* Empty state */}
            {query.length >= 3 && !isLoading && results.length === 0 && (
                <div
                    className="
            mt-1.5 px-4 py-3
            bg-white/95 backdrop-blur-md
            border border-slate-200
            rounded-xl shadow-lg
            text-sm text-slate-500 text-center
          "
                >
                    No se encontraron resultados
                </div>
            )}
        </div>
    );
};
