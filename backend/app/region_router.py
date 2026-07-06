#!/usr/bin/env python3
# =============================================================================
# Region Router - Geographic Region Detection for Cadastral Services
# =============================================================================
# Determines which cadastral service to use based on coordinates
# Returns: 'navarra', 'euskadi', or 'spain'

import logging
import json
import os
from typing import Literal, Tuple
from shapely.geometry import Point, shape, Polygon
from shapely.errors import GEOSException

logger = logging.getLogger(__name__)

RegionType = Literal['navarra', 'euskadi', 'spain']


class RegionRouter:
    """
    Router that determines which cadastral service to use based on geographic coordinates.
    Uses simplified GeoJSON boundaries or bounding boxes for efficient point-in-polygon checks.
    """

    def __init__(self, boundaries_dir: str = None):
        """
        Initialize the router with boundary geometries.
        
        Args:
            boundaries_dir: Directory containing GeoJSON boundary files.
                          If None, uses bounding boxes as fallback.
        """
        self.boundaries_dir = boundaries_dir or os.path.join(
            os.path.dirname(__file__), 'geometry_data'
        )
        self.navarra_geom = None
        self.euskadi_geom = None
        self._load_boundaries()

    def _load_boundaries(self):
        """Load boundary geometries from GeoJSON files or use bounding boxes."""
        try:
            # Try to load from GeoJSON files
            navarra_path = os.path.join(self.boundaries_dir, 'navarra_boundary_simplified.geojson')
            euskadi_path = os.path.join(self.boundaries_dir, 'euskadi_boundary_simplified.geojson')

            if os.path.exists(navarra_path):
                with open(navarra_path, 'r') as f:
                    navarra_geojson = json.load(f)
                    self.navarra_geom = shape(navarra_geojson.get('geometry', navarra_geojson))
                logger.info("Loaded Navarra boundary from GeoJSON")
            else:
                # Use bounding box as fallback
                self.navarra_geom = self._get_navarra_bbox()
                logger.info("Using Navarra bounding box (GeoJSON not found)")

            if os.path.exists(euskadi_path):
                with open(euskadi_path, 'r') as f:
                    euskadi_geojson = json.load(f)
                    self.euskadi_geom = shape(euskadi_geojson.get('geometry', euskadi_geojson))
                logger.info("Loaded Euskadi boundary from GeoJSON")
            else:
                # Use bounding box as fallback
                self.euskadi_geom = self._get_euskadi_bbox()
                logger.info("Using Euskadi bounding box (GeoJSON not found)")

        except Exception as e:
            logger.warning(f"Error loading boundary files, using bounding boxes: {e}")
            self.navarra_geom = self._get_navarra_bbox()
            self.euskadi_geom = self._get_euskadi_bbox()

    def _get_navarra_bbox(self) -> Polygon:
        """
        Get Navarra bounding box (simplified rectangle).
        Navarra approximate bounds:
        - Longitude: -2.5° to -1.0° (West to East)
        - Latitude: 42.0° to 43.5° (South to North)
        """
        return Polygon([
            (-2.5, 42.0),  # SW
            (-1.0, 42.0),  # SE
            (-1.0, 43.5),  # NE
            (-2.5, 43.5),  # NW
            (-2.5, 42.0)   # Close polygon
        ])

    def _get_euskadi_bbox(self) -> Polygon:
        """
        Get País Vasco (Euskadi) bounding box (simplified rectangle).
        Euskadi approximate bounds:
        - Longitude: -3.5° to -1.5° (West to East)
        - Latitude: 42.8° to 43.6° (South to North)
        """
        return Polygon([
            (-3.5, 42.4),  # SW (Rioja Alavesa is ~42.5)
            (-1.5, 42.4),  # SE
            (-1.5, 43.6),  # NE
            (-3.5, 43.6),  # NW
            (-3.5, 42.4)   # Close polygon
        ])

    def get_region(self, latitude: float, longitude: float) -> RegionType:
        """
        Determine which region a coordinate point belongs to.
        
        Args:
            latitude: Latitude in decimal degrees (WGS84)
            longitude: Longitude in decimal degrees (WGS84)
            
        Returns:
            'navarra', 'euskadi', or 'spain'
        """
        try:
            point = Point(longitude, latitude)  # Note: shapely uses (x, y) = (lon, lat)

            # Check Treviño (Burgos Enclave inside Araba) - MUST be Spain
            # Approximate bounding box for Treviño:
            # Lat: 42.66 to 42.78
            # Lon: -2.9 to -2.6
            if (42.65 <= latitude <= 42.80) and (-2.95 <= longitude <= -2.55):
                # More precise check if needed, but for now force Spain for this box
                # to ensure we don't query Euskadi WFS which fails.
                logger.debug(f"Point ({longitude}, {latitude}) is in Treviño (Burgos) -> Spain")
                return 'spain'

            # Check Navarra first (smaller area, more specific)
            if self.navarra_geom and self.navarra_geom.contains(point):
                logger.debug(f"Point ({longitude}, {latitude}) is in Navarra")
                return 'navarra'

            # Check Euskadi
            if self.euskadi_geom and self.euskadi_geom.contains(point):
                logger.debug(f"Point ({longitude}, {latitude}) is in Euskadi")
                return 'euskadi'

            # Default to Spain (rest of territory)
            logger.debug(f"Point ({longitude}, {latitude}) is in Spain (default)")
            return 'spain'

        except (GEOSException, ValueError, TypeError) as e:
            logger.error(f"Error determining region for ({longitude}, {latitude}): {e}")
            # Default to Spain on error
            return 'spain'


def get_region_for_bbox(bbox: Tuple[float, float, float, float]) -> RegionType:
    """
    Determine cadastral region for a bounding box.

    Uses corners + centre so Navarra parcels are not misclassified as Euskadi
    when the camera bbox centre falls outside Navarra.
    """
    minx, miny, maxx, maxy = bbox
    router = get_region_router()
    samples = (
        (miny, minx),
        (miny, maxx),
        (maxy, minx),
        (maxy, maxx),
        ((miny + maxy) / 2.0, (minx + maxx) / 2.0),
    )
    regions = {router.get_region(lat, lon) for lat, lon in samples}
    if 'navarra' in regions:
        return 'navarra'
    if 'euskadi' in regions:
        return 'euskadi'
    return 'spain'


# Global instance (singleton pattern)
_router_instance = None


def get_region_router() -> RegionRouter:
    """Get or create the global RegionRouter instance."""
    global _router_instance
    if _router_instance is None:
        _router_instance = RegionRouter()
    return _router_instance


def get_region(latitude: float, longitude: float) -> RegionType:
    """
    Convenience function to get region for coordinates.
    
    Args:
        latitude: Latitude in decimal degrees
        longitude: Longitude in decimal degrees
        
    Returns:
        'navarra', 'euskadi', or 'spain'
    """
    router = get_region_router()
    return router.get_region(latitude, longitude)

























