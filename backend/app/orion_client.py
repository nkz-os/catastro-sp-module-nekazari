# =============================================================================
# Orion-LD Client Wrapper
# =============================================================================
# Uses nkz-platform-sdk SyncOrionClient for all Orion-LD operations.
# Automatically injects @context, NGSILD-Tenant, Fiware-Service headers.
#
# Per AGENTS.md: NO raw requests to Orion-LD without proper headers.
# Per AGENTS.md: NO direct DB writes for timeseries/telemetry.
#
# For the primary AgriParcel write path, use entity-client.py → entity-manager.
# This client is for reverse-sync (catastro WFS → Orion-LD) and reads.

import os
import logging
from typing import Any, Optional

from nkz_platform_sdk import SyncOrionClient

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
ORION_URL = os.getenv('ORION_URL', 'http://orion-ld-service:1026')
CONTEXT_URL = os.getenv(
    'CONTEXT_URL',
    'https://nekazari.robotika.cloud/ngsi-ld-context.json',
)

# ---------------------------------------------------------------------------
# Client cache (one per tenant)
# ---------------------------------------------------------------------------
_clients: dict[str, SyncOrionClient] = {}


def get_orion_client(tenant_id: str) -> SyncOrionClient:
    """Get or create a SyncOrionClient for a tenant.

    The SyncOrionClient automatically injects NGSILD-Tenant, Fiware-Service,
    and @context Link header on every request.
    """
    if tenant_id not in _clients:
        _clients[tenant_id] = SyncOrionClient(
            orion_url=ORION_URL,
            tenant_id=tenant_id,
            context_url=CONTEXT_URL,
        )
    return _clients[tenant_id]


# ---------------------------------------------------------------------------
# High-level helpers
# ---------------------------------------------------------------------------

def upsert_agriparcel(
    tenant_id: str,
    parcel_id: str,
    attrs: dict[str, Any],
) -> bool:
    """Upsert an AgriParcel entity via Orion-LD PATCH /attrs.

    Uses SyncOrionClient which handles @context injection automatically.
    This is for syncing catastro data TO Orion-LD when the parcel
    was discovered via catastro WFS (reverse sync).

    For the primary write path, use entity-manager API (entity_client.py).

    Returns True on success, False on failure.
    """
    try:
        client = get_orion_client(tenant_id)
        client.patch_attributes(
            f'urn:ngsi-ld:AgriParcel:{parcel_id}',
            attrs,
        )
        return True
    except Exception as exc:
        logger.error(
            'Failed to upsert AgriParcel %s: %s',
            parcel_id,
            exc,
        )
        return False


def get_entity(
    tenant_id: str,
    entity_id: str,
    entity_type: Optional[str] = None,
) -> Optional[dict[str, Any]]:
    """Retrieve an entity from Orion-LD by ID.

    Returns the entity dict or None if not found.
    """
    try:
        client = get_orion_client(tenant_id)
        return client.get_entity(entity_id, entity_type=entity_type)
    except Exception as exc:
        logger.error(
            'Failed to get entity %s: %s',
            entity_id,
            exc,
        )
        return None


def query_entities(
    tenant_id: str,
    entity_type: str,
    attrs: Optional[list[str]] = None,
    **params: Any,
) -> list[dict[str, Any]]:
    """Query entities from Orion-LD by type.

    Wraps SyncOrionClient.query() for typed queries.
    """
    try:
        client = get_orion_client(tenant_id)
        return client.query(entity_type, attrs=attrs, **params)
    except Exception as exc:
        logger.error(
            'Failed to query entities type=%s: %s',
            entity_type,
            exc,
        )
        return []
