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

from app import sigpac_service as svc
from app.auth_middleware import require_auth
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
