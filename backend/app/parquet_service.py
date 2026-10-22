"""GeoParquet service — converts cadastral GeoJSON to GeoParquet using geolibre-wasm.

Provides faster spatial queries and smaller storage than raw GeoJSON.
"""

import json
import logging
from typing import Optional

import geolibre_wasm as gl

logger = logging.getLogger(__name__)


def geojson_to_parquet(geojson_data: dict) -> Optional[bytes]:
    """Convert GeoJSON FeatureCollection to GeoParquet bytes.
    
    Uses geolibre-wasm's write_geoparquet tool with:
    - Hilbert-curve sorting
    - BBOX covering column  
    - ZSTD compression
    """
    try:
        geojson_bytes = json.dumps(geojson_data).encode('utf-8')
        result = gl.run_tool(
            'write_geoparquet',
            args=['--input=/work/input.geojson', '--output=/work/output.parquet'],
            input={'input.geojson': geojson_bytes},
        )
        if result.exit_code != 0:
            logger.error("write_geoparquet failed (exit %d): %s", result.exit_code, result.stdout)
            return None
        return result.files.get('output.parquet')
    except Exception as exc:
        logger.error("GeoParquet conversion failed: %s", exc)
        return None


def parquet_to_geojson(parquet_bytes: bytes) -> Optional[dict]:
    """Convert GeoParquet bytes back to GeoJSON FeatureCollection."""
    try:
        result = gl.run_tool(
            'read_geoparquet',
            args=['--input=/work/input.parquet', '--output=/work/output.geojson'],
            input={'input.parquet': parquet_bytes},
        )
        if result.exit_code != 0:
            logger.error("read_geoparquet failed (exit %d): %s", result.exit_code, result.stdout)
            return None
        return json.loads(result.files['output.geojson'].decode('utf-8'))
    except Exception as exc:
        logger.error("GeoParquet read failed: %s", exc)
        return None
