"""SIGPAC enclosures (recintos) for an AgriParcel, computed from its geometry.

Source: FEGA SIGPAC catalogue (CC BY 4.0, see ``sources.py``).
Flow: OGC API candidates by bbox -> local intersection (UTM) -> drop overlaps
below ``SIGPAC_MIN_OVERLAP_PCT`` -> attributes per interior point of each overlap.

The result is stored on the AgriParcel as the ``sigpacEnclosures`` Property.
A failed lookup never removes a previously stored value.
"""

import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Callable, Optional

import requests
from pyproj import Transformer
from shapely.geometry import Point, shape
from shapely.geometry.base import BaseGeometry
from shapely.ops import transform
from shapely.prepared import prep
from shapely.validation import make_valid

from app.sources import FEGA_SIGPAC

logger = logging.getLogger(__name__)

FEGA_BASE_URL = os.getenv('FEGA_SIGPAC_URL', 'https://sigpac-hubcloud.es').rstrip('/')

# Overlaps below this share of the parcel area are dropped (coordinator decision).
SIGPAC_MIN_OVERLAP_PCT = 1.0
MAX_CONCURRENCY = 4
REQUEST_TIMEOUT_S = 15
MAX_RETRIES = 3
BACKOFF_BASE_S = 0.5
OGC_PAGE_LIMIT = 500
MAX_OGC_PAGES = 20

ATTR_NAME = 'sigpacEnclosures'
# Names under which the attribute can come back from Orion: short name, the
# platform-vocabulary IRI (once in the @context) and the default-context IRI.
ATTR_ALIASES = (
    ATTR_NAME,
    'https://nkz-os.org/ns/sigpacEnclosures',
    'https://uri.etsi.org/ngsi-ld/default-context/sigpacEnclosures',
)

# Spain outline (Natural Earth, public domain; see the file's properties), buffered ~2 km.
_OUTLINE_PATH = os.path.join(os.path.dirname(__file__), 'geometry_data', 'spain_outline.geojson')
_outline = None
_outline_lock = threading.Lock()

# One limit for every path (user refresh, notifications, backfill) against the FEGA.
_fega_slots = threading.BoundedSemaphore(MAX_CONCURRENCY)


class FegaError(Exception):
    """The FEGA service could not answer; the caller must not touch stored data."""


# ---------------------------------------------------------------------------
# Geometry
# ---------------------------------------------------------------------------

def _to_shape(geometry: dict) -> BaseGeometry:
    geom = shape(geometry)
    if geom.is_empty:
        raise ValueError('empty geometry')
    if not geom.is_valid:
        geom = make_valid(geom)
    polys = _polygonal(geom)
    if polys.is_empty:
        raise ValueError('geometry has no polygonal part')
    return polys


def _polygonal(geom: BaseGeometry) -> BaseGeometry:
    """Keep only polygonal parts (make_valid may return a GeometryCollection)."""
    if geom.geom_type in ('Polygon', 'MultiPolygon'):
        return geom
    from shapely.geometry import MultiPolygon, Polygon
    parts = []
    for part in getattr(geom, 'geoms', []):
        sub = _polygonal(part)
        if sub.is_empty:
            continue
        parts.extend(sub.geoms if sub.geom_type == 'MultiPolygon' else [sub])
    return MultiPolygon(parts) if parts else Polygon()


def _spain_outline():
    global _outline
    with _outline_lock:
        if _outline is None:
            with open(_OUTLINE_PATH, encoding='utf-8') as fh:
                _outline = prep(shape(json.load(fh)['geometry']))
        return _outline


def is_in_spain(geometry: dict) -> bool:
    try:
        c = _to_shape(geometry).centroid
    except Exception:
        return False
    return _spain_outline().contains(Point(c.x, c.y))


def _metric_epsg(lon: float) -> int:
    """ETRS89 UTM zone by longitude: 28N Canarias, 29N west of -6, 30N, 31N east of 0."""
    if lon < -12.0:
        return 25828
    if lon < -6.0:
        return 25829
    if lon < 0.0:
        return 25830
    return 25831


def _transformers(lon: float) -> tuple[Callable, Callable]:
    epsg = _metric_epsg(lon)
    fwd = Transformer.from_crs(4326, epsg, always_xy=True).transform
    inv = Transformer.from_crs(epsg, 4326, always_xy=True).transform
    return fwd, inv


def _interior_point(geom: BaseGeometry) -> BaseGeometry:
    if geom.geom_type == 'MultiPolygon':
        geom = max(geom.geoms, key=lambda g: g.area)
    return geom.representative_point()


def find_overlaps(parcel_geometry: dict, candidates: list[dict]) -> list[dict]:
    """Candidates (GeoJSON features) overlapping >= SIGPAC_MIN_OVERLAP_PCT of the parcel.

    Returns dicts with ``feature``, ``overlap_ha``, ``overlap_pct`` and
    ``point`` (lon, lat of an interior point of the overlap), largest first.
    """
    parcel = _to_shape(parcel_geometry)
    fwd, inv = _transformers(parcel.centroid.x)
    parcel_m = transform(fwd, parcel)
    parcel_area = parcel_m.area
    if parcel_area <= 0:
        raise ValueError('parcel has zero area')
    out = []
    for feat in candidates:
        try:
            cand = _to_shape(feat['geometry'])
        except Exception as exc:
            logger.warning('sigpac skip candidate %s: %s', feat.get('id'), exc)
            continue
        inter = parcel_m.intersection(transform(fwd, cand))
        inter = _polygonal(inter)
        if inter.is_empty:
            continue
        pct = inter.area / parcel_area * 100.0
        if pct < SIGPAC_MIN_OVERLAP_PCT:
            continue
        pt = transform(inv, _interior_point(inter))
        out.append({
            'feature': feat,
            'overlap_ha': inter.area / 10000.0,
            'overlap_pct': pct,
            'point': (pt.x, pt.y),
        })
    out.sort(key=lambda o: o['overlap_ha'], reverse=True)
    return out


# ---------------------------------------------------------------------------
# FEGA client
# ---------------------------------------------------------------------------

class FegaClient:
    def __init__(self, base_url: str = FEGA_BASE_URL, session: Any = None,
                 sleep: Callable[[float], None] = time.sleep,
                 deadline_s: Optional[float] = None):
        self.base_url = base_url.rstrip('/')
        self._injected = session
        self._local = threading.local()
        self._sleep = sleep
        self._deadline = None if deadline_s is None else time.monotonic() + deadline_s

    def _session(self) -> Any:
        """One requests.Session per thread (Session is not guaranteed thread-safe)."""
        if self._injected is not None:
            return self._injected
        if not hasattr(self._local, 'session'):
            self._local.session = requests.Session()
        return self._local.session

    def _remaining(self) -> Optional[float]:
        return None if self._deadline is None else self._deadline - time.monotonic()

    def _get_json(self, url: str, params: Optional[dict] = None) -> Any:
        last: Optional[Exception] = None
        for attempt in range(MAX_RETRIES):
            if attempt:
                self._sleep(BACKOFF_BASE_S * (2 ** (attempt - 1)))
            left = self._remaining()
            if left is not None and left <= 0:
                raise FegaError('FEGA deadline exceeded')
            if not _fega_slots.acquire(timeout=left if left is not None else 60):
                raise FegaError('FEGA concurrency slot not available in time')
            try:
                resp = self._session().get(url, params=params, timeout=REQUEST_TIMEOUT_S,
                                           headers={'Accept': 'application/json'})
            except requests.RequestException as exc:
                last = exc
                continue
            finally:
                _fega_slots.release()
            if resp.status_code == 429 or resp.status_code >= 500:
                last = FegaError(f'HTTP {resp.status_code}')
                continue
            if resp.status_code != 200:
                raise FegaError(f'HTTP {resp.status_code} from FEGA')
            try:
                return resp.json()
            except ValueError as exc:
                last = exc
                continue
        raise FegaError(f'FEGA request failed after {MAX_RETRIES} attempts: {last}')

    def candidates_in_bbox(self, bbox: tuple[float, float, float, float]) -> list[dict]:
        """All recinto features whose bbox meets ``bbox`` (lon/lat), following pagination."""
        url = f'{self.base_url}/ogcapi/collections/recintos/items'
        params: Optional[dict] = {
            'f': 'json', 'limit': OGC_PAGE_LIMIT,
            'bbox': ','.join(f'{v:.7f}' for v in bbox),
        }
        features: list[dict] = []
        for _ in range(MAX_OGC_PAGES):
            data = self._get_json(url, params)
            # A 200 that is not a FeatureCollection is an error body, never "no recintos".
            if (not isinstance(data, dict) or data.get('type') != 'FeatureCollection'
                    or not isinstance(data.get('features'), list)):
                raise FegaError('FEGA answered something other than a FeatureCollection')
            features.extend(data['features'])
            nxt = next((l.get('href') for l in data.get('links') or []
                        if l.get('rel') == 'next' and l.get('href')), None)
            if not nxt:
                return features
            url, params = nxt, None
        raise FegaError('FEGA pagination did not end')

    def enclosure_at(self, lon: float, lat: float) -> Optional[dict]:
        """Attributes of the recinto at a point, or None when there is none."""
        url = f'{self.base_url}/servicioconsultassigpac/query/recinfobypoint/4326/{lon:.7f}/{lat:.7f}.json'
        data = self._get_json(url)
        if isinstance(data, list) and data:
            return data[0]
        return None


# ---------------------------------------------------------------------------
# Build the attribute value
# ---------------------------------------------------------------------------

def make_reference(p: dict) -> str:
    """'pp:mmm:a:z:pol:par:rec', no leading zeros (FEGA refrecinto / FaST format)."""
    return ':'.join(str(int(p[k])) for k in
                    ('provincia', 'municipio', 'agregado', 'zona', 'poligono', 'parcela', 'recinto'))


def _same_enclosure(props: dict, attrs: dict) -> bool:
    keys = ('provincia', 'municipio', 'agregado', 'zona', 'poligono', 'parcela', 'recinto')
    try:
        return all(int(props[k]) == int(attrs[k]) for k in keys)
    except (KeyError, TypeError, ValueError):
        return False


def _iso(now: datetime) -> str:
    return now.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def compute_enclosures(geometry: dict, client: Optional[FegaClient] = None,
                       now: Optional[datetime] = None) -> list[dict]:
    """Enclosures for a parcel geometry. Raises FegaError if FEGA cannot answer."""
    client = client or FegaClient()
    now = now or datetime.now(timezone.utc)
    parcel = _to_shape(geometry)
    candidates = client.candidates_in_bbox(parcel.bounds)
    overlaps = find_overlaps(geometry, candidates)

    def _attrs(ov: dict) -> Optional[dict]:
        return client.enclosure_at(*ov['point'])

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENCY) as pool:
        fetched = list(pool.map(_attrs, overlaps))

    stamp = _iso(now)
    result = []
    for ov, attrs in zip(overlaps, fetched):
        props = ov['feature']['properties']
        if attrs is not None and not _same_enclosure(props, attrs):
            logger.warning('sigpac point lookup returned a different enclosure than the candidate; '
                           'attributes left empty for %s', make_reference(props))
            attrs = None
        attrs = attrs or {}
        result.append({
            'reference': make_reference(props),
            'overlapAreaHa': round(ov['overlap_ha'], 4),
            'overlapPct': round(ov['overlap_pct'], 2),
            'enclosureAreaHa': attrs.get('superficie'),
            'sigpacUse': attrs.get('uso_sigpac'),
            'slopePct': attrs.get('pendiente_media', props.get('pendiente_media')),
            'irrigationCoef': attrs.get('coef_regadio'),
            'retrievedAt': stamp,
        })
    return result


def attribute_payload(enclosures: list[dict]) -> dict:
    return {
        'type': 'Property',
        'value': enclosures,
        'source': {'type': 'Property', 'value': FEGA_SIGPAC},
    }


def read_enclosures(entity: Optional[dict]) -> Optional[list[dict]]:
    """Stored enclosures from an Orion entity, whichever name Orion used; None if absent."""
    if not isinstance(entity, dict):
        return None
    for name in ATTR_ALIASES:
        attr = entity.get(name)
        if attr is None:
            continue
        value = attr.get('value') if isinstance(attr, dict) else attr
        if isinstance(value, dict):
            return [value]      # Orion-LD compacts a one-element array to the bare element
        if isinstance(value, list):
            return value
    return None


# ---------------------------------------------------------------------------
# Refresh (compute + write)
# ---------------------------------------------------------------------------

def _log(event: str, **fields: Any) -> None:
    logger.info('sigpac.%s %s', event, json.dumps(fields, default=str, sort_keys=True))


def refresh_parcel(tenant_id: str, parcel_urn: str, geometry: Optional[dict] = None,
                   client: Optional[FegaClient] = None, orion: Any = None,
                   only_if_missing: bool = False,
                   now: Optional[datetime] = None) -> dict:
    """Compute and store the enclosures of one parcel.

    Returns ``{'status': 'updated'|'skipped'|'error', ...}``; never raises.
    On any failure the stored value is left as it was.
    """
    started = time.time()
    try:
        if orion is None:
            from app.orion_client import get_orion_client
            orion = get_orion_client(tenant_id)
        entity = None
        if geometry is None or only_if_missing:
            entity = orion.get_entity(parcel_urn)
        if only_if_missing and read_enclosures(entity) is not None:
            return {'status': 'skipped', 'reason': 'already_present'}
        if geometry is None:
            loc = (entity or {}).get('location') or (entity or {}).get(
                'https://uri.etsi.org/ngsi-ld/location') or {}
            geometry = loc.get('value') if isinstance(loc, dict) else None
        if not isinstance(geometry, dict) or not geometry.get('type'):
            return {'status': 'skipped', 'reason': 'no_geometry'}
        if not is_in_spain(geometry):
            return {'status': 'skipped', 'reason': 'outside_spain'}
        enclosures = compute_enclosures(geometry, client, now)
        orion.append_entity_attrs(parcel_urn, {ATTR_NAME: attribute_payload(enclosures)},
                                  overwrite=True)
        _log('refresh', tenant=tenant_id, parcel=parcel_urn, status='updated',
             enclosures=len(enclosures), ms=int((time.time() - started) * 1000))
        return {'status': 'updated', 'enclosures': enclosures}
    except Exception as exc:  # fail-safe: keep previous value, report
        logger.error('sigpac.refresh %s', json.dumps(
            {'tenant': tenant_id, 'parcel': parcel_urn, 'status': 'error',
             'error': f'{type(exc).__name__}: {exc}',
             'ms': int((time.time() - started) * 1000)}, sort_keys=True))
        return {'status': 'error', 'error': type(exc).__name__}
