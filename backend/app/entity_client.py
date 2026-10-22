# =============================================================================
# Entity Manager API Client
# =============================================================================
# The sole write path for AgriParcel entities.
#
# Per AGENTS.md / PENDING.md:
#   - entity-manager is the ONLY writer for AgriParcel
#   - Catastro Spain must NOT write directly to Orion-LD
#   - POST/PATCH/DELETE go through entity-manager API
#
# The entity-manager API:
#   POST   /api/entities/parcels        — create parcel
#   PATCH  /api/entities/parcels/{id}   — update parcel
#   DELETE /api/entities/parcels/{id}   — delete parcel
#
# Requests carry X-Tenant-ID + X-User-ID + X-Auth-Signature (HMAC).

import os
import json
import hashlib
import hmac
import logging
import time
from typing import Any, Optional

import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
ENTITY_MANAGER_URL = os.getenv(
    'ENTITY_MANAGER_URL',
    'http://entity-manager-service:5000',
)
HMAC_SECRET = os.getenv('HMAC_SECRET', '')

# User-ID sent for internal write operations.
WORKER_USER_ID = os.getenv('CADASTRAL_WORKER_USER', 'catastro-worker')


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _hmac_signature(token: str, tenant_id: str) -> str:
    """Generate canonical HMAC-SHA256: payload={token}|{tenant_id}|{ts}, output={sig}:{ts}

    Aligned with services/common/keycloak_auth.py:generate_hmac_signature.
    """
    if not HMAC_SECRET:
        return ''
    timestamp = int(time.time())
    payload = f'{token}|{tenant_id}|{timestamp}'
    sig = hmac.new(HMAC_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()
    return f'{sig}:{timestamp}'


def _headers(tenant_id: str, user_id: str) -> dict[str, str]:
    """Build headers with tenant context + HMAC signature.

    Uses token='' for internal service-to-service calls because
    there is no end-user Bearer token — catastro-worker signs
    via HMAC directly.
    """
    signature = _hmac_signature('', tenant_id)  # token='' for internal service-to-service
    return {
        'X-Tenant-ID': tenant_id,
        'X-User-ID': user_id,
        'X-Auth-Signature': signature,
        'Content-Type': 'application/json',
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def create_parcel(
    tenant_id: str,
    parcel_data: dict[str, Any],
    user_id: str = WORKER_USER_ID,
) -> Optional[str]:
    """Create an AgriParcel via entity-manager.

    Args:
        tenant_id: Target tenant.
        parcel_data: NGSI-LD AgriParcel payload.
        user_id: Identity for the write (default: catastro-worker).

    Returns:
        The created parcel ID (string) on success, None on failure.
    """
    try:
        resp = requests.post(
            f'{ENTITY_MANAGER_URL}/api/entities/parcels',
            json=parcel_data,
            headers=_headers(tenant_id, user_id),
            timeout=30,
        )
        if resp.status_code in (200, 201):
            return resp.json().get('id')
        logger.error(
            'entity-manager create failed: %d %s',
            resp.status_code,
            resp.text[:300],
        )
        return None
    except requests.RequestException as exc:
        logger.error('entity-manager create request error: %s', exc)
        return None


def update_parcel(
    tenant_id: str,
    parcel_id: str,
    parcel_data: dict[str, Any],
    user_id: str = WORKER_USER_ID,
) -> bool:
    """Update an AgriParcel via entity-manager PATCH.

    Args:
        tenant_id: Target tenant.
        parcel_id: The parcel identifier (without URN prefix).
        parcel_data: NGSI-LD attributes to patch.
        user_id: Identity for the write.

    Returns:
        True on success, False on failure.
    """
    try:
        resp = requests.patch(
            f'{ENTITY_MANAGER_URL}/api/entities/parcels/{parcel_id}',
            json=parcel_data,
            headers=_headers(tenant_id, user_id),
            timeout=30,
        )
        if resp.status_code in (200, 204):
            return True
        logger.error(
            'entity-manager update failed: %d %s',
            resp.status_code,
            resp.text[:300],
        )
        return False
    except requests.RequestException as exc:
        logger.error('entity-manager update request error: %s', exc)
        return False


def delete_parcel(
    tenant_id: str,
    parcel_id: str,
    user_id: str = WORKER_USER_ID,
) -> bool:
    """Delete an AgriParcel via entity-manager.

    Args:
        tenant_id: Target tenant.
        parcel_id: The parcel identifier.
        user_id: Identity for the write.

    Returns:
        True on success, False on failure.
    """
    try:
        resp = requests.delete(
            f'{ENTITY_MANAGER_URL}/api/entities/parcels/{parcel_id}',
            headers=_headers(tenant_id, user_id),
            timeout=30,
        )
        if resp.status_code in (200, 204):
            return True
        logger.error(
            'entity-manager delete failed: %d %s',
            resp.status_code,
            resp.text[:300],
        )
        return False
    except requests.RequestException as exc:
        logger.error('entity-manager delete request error: %s', exc)
        return False
