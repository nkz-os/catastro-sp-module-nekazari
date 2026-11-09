"""HTTP surface for SIGPAC enclosures: manual refresh, read, backfill, Orion notifications.

All writes go through ``sigpac_service.refresh_parcel`` (SDK, merge write, never
deleting the previous value on failure).
"""

import hashlib
import hmac
import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from typing import Any, Optional

from flask import Blueprint, g, jsonify, request
from shapely.geometry import box, mapping

from app import sigpac_select as sel
from app import sigpac_service as svc
from app.auth_middleware import require_auth
from app.entity_client import create_parcel as em_create_parcel
from app.sources import FEGA_SIGPAC, get_source

logger = logging.getLogger(__name__)

sigpac_bp = Blueprint('sigpac', __name__, url_prefix='/api/cadastral-api')

_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix='sigpac')
# Backfills are long; they must not starve interactive refreshes.
_backfill_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix='sigpac-backfill')

# gunicorn runs sync with a 120 s timeout: the interactive route waits at most this long
# and answers 202 if the job is still running (the job keeps going and stores its result).
REFRESH_WAIT_S = 45
REFRESH_DEADLINE_S = 40
BACKGROUND_DEADLINE_S = 300

# Background triggers (create / geometry-change notification) overlap for one
# creation; repeat work for the same geometry inside this window is skipped.
RECENT_TTL_S = 600
_recent: dict[tuple, float] = {}
_recent_lock = threading.Lock()
_BACKFILL_PAGE = 100


def parcel_urn(parcel_id: str) -> str:
    raw = (parcel_id or '').strip()
    return raw if raw.startswith('urn:') else f'urn:ngsi-ld:AgriParcel:{raw}'


def _geometry(entity: dict) -> Optional[dict]:
    loc = entity.get('location') or entity.get('https://uri.etsi.org/ngsi-ld/location')
    if isinstance(loc, dict):
        val = loc.get('value') or loc
        if isinstance(val, dict) and val.get('type') in ('Polygon', 'MultiPolygon'):
            return val
    return None


def _round_coords(value: Any) -> Any:
    if isinstance(value, (int, float)):
        return round(float(value), 6)
    if isinstance(value, (list, tuple)):
        return [_round_coords(v) for v in value]
    return value


def geometry_digest(geometry: dict) -> str:
    """Stable across an Orion round-trip: type + coordinates rounded to 1e-6 deg (~0.1 m)."""
    norm = {'type': geometry.get('type'), 'coordinates': _round_coords(geometry.get('coordinates'))}
    return hashlib.sha256(json.dumps(norm, sort_keys=True).encode()).hexdigest()


def _is_parcel(entity: dict) -> bool:
    return str(entity.get('type', '')).rsplit('/', 1)[-1] == 'AgriParcel'


def _seen_recently(tenant_id: str, urn: str, geometry: dict) -> bool:
    key = (tenant_id.lower(), urn, geometry_digest(geometry))
    now = time.time()
    with _recent_lock:
        for k in [k for k, t in _recent.items() if now - t > RECENT_TTL_S]:
            del _recent[k]
        if key in _recent:
            return True
        _recent[key] = now
        return False


def trigger_refresh(tenant_id: str, urn: str, geometry: dict, **kw: Any) -> bool:
    """Queue a background refresh unless the same geometry was handled recently."""
    if _seen_recently(tenant_id, urn, geometry):
        return False
    _executor.submit(_refresh_job, tenant_id.lower(), urn, geometry, BACKGROUND_DEADLINE_S, **kw)
    return True


def _refresh_job(tenant_id: str, urn: str, geometry: Optional[dict], deadline_s: float,
                 only_if_missing: bool = False) -> dict:
    return svc.refresh_parcel(tenant_id, urn, geometry,
                              client=svc.FegaClient(deadline_s=deadline_s),
                              only_if_missing=only_if_missing)


def _internal_authorized() -> bool:
    secret = os.getenv('INTERNAL_SERVICE_SECRET', '')
    provided = request.headers.get('X-Internal-Service-Secret', '')
    return bool(secret) and hmac.compare_digest(provided.encode(), secret.encode())


# ---------------------------------------------------------------------------
# User routes
# ---------------------------------------------------------------------------

@sigpac_bp.route('/parcels/<path:parcel_id>/sigpac/refresh', methods=['POST'])
@require_auth
def refresh_sigpac(parcel_id):
    from app.orion_client import get_entity
    body = request.get_json(silent=True) or {}
    urn = parcel_urn(parcel_id)
    entity = get_entity(g.tenant_id, urn)
    if not entity or not _is_parcel(entity):
        return jsonify({'error': 'Parcel not found'}), 404
    future = _executor.submit(_refresh_job, g.tenant_id.lower(), urn, _geometry(entity),
                              REFRESH_DEADLINE_S, bool(body.get('onlyIfMissing')))
    try:
        result = future.result(timeout=REFRESH_WAIT_S)
    except FutureTimeout:
        return jsonify({'status': 'pending'}), 202
    if result['status'] == 'error':
        return jsonify({'error': 'SIGPAC lookup failed'}), 502
    result['source'] = get_source(FEGA_SIGPAC)
    return jsonify(result), 200


@sigpac_bp.route('/parcels/<path:parcel_id>/sigpac', methods=['GET'])
@require_auth
def get_sigpac(parcel_id):
    from app.orion_client import get_entity
    entity = get_entity(g.tenant_id, parcel_urn(parcel_id))
    if not entity:
        return jsonify({'error': 'Parcel not found'}), 404
    return jsonify({'enclosures': svc.read_enclosures(entity),
                    'source': get_source(FEGA_SIGPAC)}), 200


# ---------------------------------------------------------------------------
# Internal backfill
# ---------------------------------------------------------------------------

def run_backfill(tenant_id: str, force: bool = False, client=None, orion=None) -> dict:
    """Refresh every Spanish AgriParcel of a tenant; sequential, one at a time."""
    if orion is None:
        from app.orion_client import get_orion_client
        orion = get_orion_client(tenant_id)
    counts = {'updated': 0, 'skipped': 0, 'error': 0}
    offset = 0
    while True:
        page = orion.query_entities(type='AgriParcel', limit=_BACKFILL_PAGE, offset=offset)
        for entity in page:
            urn = entity.get('id')
            geometry = _geometry(entity)
            if not urn or not geometry or (not force and svc.read_enclosures(entity) is not None):
                counts['skipped'] += 1
                continue
            res = svc.refresh_parcel(tenant_id, urn, geometry, client=client, orion=orion)
            counts[res['status']] = counts.get(res['status'], 0) + 1
        if len(page) < _BACKFILL_PAGE:
            break
        offset += _BACKFILL_PAGE
    logger.info('sigpac.backfill %s', json.dumps({'tenant': tenant_id, **counts}, sort_keys=True))
    return counts


def backfill_job(tenant_id: str, force: bool) -> None:
    """Executor entry point: a failed backfill is logged with its tenant, never lost silently."""
    try:
        run_backfill(tenant_id, force)
    except Exception as exc:
        logger.error('sigpac.backfill %s', json.dumps(
            {'tenant': tenant_id, 'status': 'error',
             'error': f'{type(exc).__name__}: {exc}'}, sort_keys=True))


@sigpac_bp.route('/internal/sigpac/backfill', methods=['POST'])
def backfill_sigpac():
    if not _internal_authorized():
        return jsonify({'error': 'Unauthorized'}), 401
    body = request.get_json(silent=True) or {}
    tenant_id = body.get('tenant_id')
    if not isinstance(tenant_id, str) or not tenant_id:
        return jsonify({'error': 'tenant_id required'}), 400
    _backfill_executor.submit(backfill_job, tenant_id, bool(body.get('force')))
    return jsonify({'status': 'accepted', 'tenant_id': tenant_id}), 202


# ---------------------------------------------------------------------------
# Map click: select recintos and create one parcel from them
# ---------------------------------------------------------------------------

_tenant_locks: dict[str, threading.Lock] = {}
_tenant_locks_guard = threading.Lock()


def _tenant_lock(tenant_id: str) -> threading.Lock:
    """Serialises check-then-create per tenant (one replica: an in-process lock suffices)."""
    with _tenant_locks_guard:
        return _tenant_locks.setdefault(tenant_id.lower(), threading.Lock())


def _orion_for(tenant_id: str) -> Any:
    from app.orion_client import get_orion_client
    return get_orion_client(tenant_id)


def _tenant_parcels(tenant_id: str) -> list[dict]:
    orion = _orion_for(tenant_id)
    parcels: list[dict] = []
    offset = 0
    while True:
        page = orion.query_entities(type='AgriParcel', limit=_BACKFILL_PAGE, offset=offset)
        parcels.extend(page)
        if len(page) < _BACKFILL_PAGE:
            return parcels
        offset += _BACKFILL_PAGE


def _place_names(geometry: dict) -> dict:
    """Municipality and province from the Catastro at an interior point; best effort."""
    try:
        from app.catastro_clients import SpanishStateCatastroClient
        point = svc._to_shape(geometry).representative_point()
        data = SpanishStateCatastroClient().query_by_coordinates(point.x, point.y, '4326') or {}
    except Exception as exc:
        logger.info('sigpac.place_names unavailable: %s', type(exc).__name__)
        return {}
    return {k: data[k] for k in ('municipality', 'province')
            if isinstance(data.get(k), str) and data[k].strip()}


def _fega_unavailable():
    return jsonify({'error': 'SIGPAC lookup failed', 'source': 'fega'}), 503


@sigpac_bp.route('/sigpac/enclosure-at', methods=['POST'])
@require_auth
def enclosure_at():
    body = request.get_json(silent=True) or {}
    try:
        lon, lat = float(body['longitude']), float(body['latitude'])
    except (KeyError, TypeError, ValueError):
        return jsonify({'error': 'longitude and latitude are required numbers'}), 400
    # is_in_spain takes an area: a box of about 1 m around the point.
    if not svc.is_in_spain(mapping(box(lon - 1e-5, lat - 1e-5, lon + 1e-5, lat + 1e-5))):
        return jsonify({'error': 'outside_coverage'}), 422
    try:
        attrs = svc.FegaClient(deadline_s=REFRESH_DEADLINE_S).enclosure_at(lon, lat)
    except svc.FegaError:
        return _fega_unavailable()
    if not attrs:
        return jsonify({'error': 'no_enclosure'}), 404
    summary = sel.enclosure_summary(attrs)
    taken = sel.parcels_by_enclosure(_tenant_parcels(g.tenant_id))
    summary['inParcel'] = taken.get(summary['reference'])
    return jsonify(summary), 200


@sigpac_bp.route('/sigpac/parcels', methods=['POST'])
@require_auth
def create_from_enclosures():
    body = request.get_json(silent=True) or {}
    refs = body.get('references')
    if not isinstance(refs, list):
        return jsonify({'error': 'invalid_reference'}), 422
    refs = list(dict.fromkeys(refs))
    if not refs:
        return jsonify({'error': 'empty'}), 422
    if len(refs) > sel.MAX_ENCLOSURES_PER_PARCEL:
        return jsonify({'error': 'too_many', 'max': sel.MAX_ENCLOSURES_PER_PARCEL}), 422
    try:
        for ref in refs:
            sel.parse_reference(ref)
    except ValueError:
        return jsonify({'error': 'invalid_reference'}), 422

    client = svc.FegaClient(deadline_s=REFRESH_DEADLINE_S)
    try:
        with ThreadPoolExecutor(max_workers=svc.MAX_CONCURRENCY) as pool:
            fetched = list(pool.map(client.enclosure_by_reference, refs))
    except svc.FegaError:
        return _fega_unavailable()
    missing = [ref for ref, attrs in zip(refs, fetched) if not attrs]
    if missing:
        return jsonify({'error': 'invalid_reference', 'reference': missing[0]}), 422
    try:
        geometry = sel.union_enclosures([sel.enclosure_summary(a)['geometry'] for a in fetched])
    except sel.NotContiguous:
        return jsonify({'error': 'not_contiguous'}), 422

    tenant = g.tenant_id
    with _tenant_lock(tenant):
        taken = sel.parcels_by_enclosure(_tenant_parcels(tenant))
        for ref in refs:
            if ref in taken:
                return jsonify({'error': 'enclosure_in_parcel', 'reference': ref,
                                'parcel': taken[ref]}), 409
        payload = {
            'geometry': geometry,
            'name': sel.default_name(refs, body.get('name') if isinstance(body.get('name'), str) else None),
            'category': 'cadastral',
            'area': round(sel.area_ha(geometry), 4),
            'ndviEnabled': True,
            **_place_names(geometry),
        }
        em_id = em_create_parcel(tenant, payload, g.user_id)
        if not em_id:
            return jsonify({'error': 'Failed to create parcel in entity-manager'}), 502
        urn = parcel_urn(em_id)
        # Written before answering, so the next creation in this tenant sees these recintos.
        result = svc.refresh_parcel(tenant.lower(), urn, geometry,
                                    client=svc.FegaClient(deadline_s=REFRESH_DEADLINE_S))
    pending = result.get('status') != 'updated'
    if pending:
        trigger_refresh(tenant, urn, geometry)
    svc._log('create', tenant=tenant, parcel=urn, enclosures=len(refs), pending=pending)
    return jsonify({'id': urn, 'sigpacPending': pending}), 201


# ---------------------------------------------------------------------------
# Orion notifications (AgriParcel location changed)
# ---------------------------------------------------------------------------

@sigpac_bp.route('/orion/sigpac-notify', methods=['POST'])
def sigpac_notify():
    from nkz_platform_sdk.notify_auth import accept_master_from_env, verify_notification
    if not verify_notification(request.headers,
                               secret=os.getenv('INTERNAL_SERVICE_SECRET', ''),
                               accept_master=accept_master_from_env()):
        return jsonify({'error': 'Unauthorized'}), 401
    tenant_id = request.headers.get('NGSILD-Tenant') or request.headers.get('Fiware-Service')
    payload = request.get_json(silent=True) or {}
    if tenant_id:
        for entity in payload.get('data') or []:
            urn, geometry = entity.get('id'), _geometry(entity)
            if urn and geometry and not entity.get('deleted'):
                trigger_refresh(tenant_id.lower(), urn, geometry)
    return '', 204
