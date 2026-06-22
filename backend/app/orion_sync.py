#!/usr/bin/env python3
"""
DEPRECATED: Direct Orion-LD writes and PostGIS sync are being removed.
Use entity-manager API (entity_client.py) for all AgriParcel writes.
This module will be deleted in a future version.
"""

import logging

logger = logging.getLogger(__name__)
logger.warning(
    "orion_sync is deprecated — all AgriParcel writes must go through "
    "entity-manager via entity_client.py"
)
