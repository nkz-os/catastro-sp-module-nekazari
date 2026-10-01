#!/usr/bin/env python3
# =============================================================================
# Cache Service - Redis caching for cadastral queries
# =============================================================================
# Provides TTL-based caching to reduce latency on repeated WFS queries
# Uses the platform's shared Redis instance

import logging
import json
import os
import hashlib
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)

# Redis URL from environment (platform's shared Redis)
REDIS_URL = os.getenv('REDIS_URL', 'redis://:@redis-service:6379/0')


class CadastralCache:
    """
    Redis cache for cadastral queries.
    
    Caching strategy:
    - Coordinate queries: TTL 24 hours (cadastral data rarely changes)
    - WFS capabilities: TTL 7 days (service metadata is stable)
    - Geometry data: TTL 7 days (parcel geometries are stable)
    
    Keys are namespaced to avoid collisions with other services.
    """
    
    # TTL values in seconds
    TTL_COORDINATES = 86400      # 24 hours
    TTL_CAPABILITIES = 604800    # 7 days
    TTL_GEOMETRY = 604800        # 7 days
    TTL_PARCEL_BUILDINGS = 604800  # 7 days (key carries the parcel geometry hash)
    
    # Key prefixes
    PREFIX = "cadastral"
    
    def __init__(self, redis_url: str = None):
        """
        Initialize Redis connection.
        
        Args:
            redis_url: Redis connection URL. If None, uses REDIS_URL env var.
        """
        self._redis = None
        self._redis_url = redis_url or REDIS_URL
        self._available = False
        self._init_redis()
    
    def _init_redis(self):
        """Initialize Redis client with error handling."""
        try:
            import redis
            self._redis = redis.from_url(
                self._redis_url,
                decode_responses=True,  # Return strings, not bytes
                socket_connect_timeout=5,
                socket_timeout=5
            )
            # Test connection
            self._redis.ping()
            self._available = True
            logger.info(f"Redis cache initialized successfully")
        except ImportError:
            logger.warning("Redis package not installed, caching disabled")
            self._available = False
        except Exception as e:
            logger.warning(f"Redis connection failed, caching disabled: {e}")
            self._available = False
    
    @property
    def is_available(self) -> bool:
        """Check if Redis is available."""
        return self._available
    
    def _coord_key(self, lat: float, lon: float, precision: int = 6) -> str:
        """
        Generate cache key from coordinates.
        
        Uses 6 decimal places (~10cm precision) to group nearby queries.
        This prevents cache fragmentation while maintaining accuracy.
        
        Args:
            lat: Latitude
            lon: Longitude
            precision: Decimal places (default 6 = ~10cm)
            
        Returns:
            Cache key string
        """
        lat_rounded = round(lat, precision)
        lon_rounded = round(lon, precision)
        return f"{self.PREFIX}:coord:{lat_rounded}:{lon_rounded}"
    
    def _capabilities_key(self, wfs_url: str) -> str:
        """
        Generate cache key for WFS capabilities.
        
        Args:
            wfs_url: WFS service URL
            
        Returns:
            Cache key string
        """
        url_hash = hashlib.md5(wfs_url.encode()).hexdigest()[:12]
        return f"{self.PREFIX}:capabilities:{url_hash}"
    
    def _geometry_key(self, cadastral_ref: str) -> str:
        """
        Generate cache key for parcel geometry.
        
        Args:
            cadastral_ref: Cadastral reference
            
        Returns:
            Cache key string
        """
        # Normalize reference (remove dashes, uppercase)
        ref_normalized = cadastral_ref.replace('-', '').upper()
        return f"{self.PREFIX}:geometry:{ref_normalized}"
    
    def get_by_coordinates(self, lat: float, lon: float) -> Optional[Dict[str, Any]]:
        """
        Get cached cadastral data by coordinates.
        
        Args:
            lat: Latitude
            lon: Longitude
            
        Returns:
            Cached data dict or None if not found/cache miss
        """
        if not self._available:
            return None
        
        try:
            key = self._coord_key(lat, lon)
            data = self._redis.get(key)
            if data:
                logger.debug(f"Cache HIT for coordinates ({lat}, {lon})")
                return json.loads(data)
            else:
                logger.debug(f"Cache MISS for coordinates ({lat}, {lon})")
                return None
        except Exception as e:
            logger.warning(f"Cache read error for coordinates: {e}")
            return None
    
    def set_by_coordinates(self, lat: float, lon: float, data: Dict[str, Any]) -> bool:
        """
        Cache cadastral data by coordinates.
        
        Args:
            lat: Latitude
            lon: Longitude
            data: Cadastral data to cache
            
        Returns:
            True if cached successfully, False otherwise
        """
        if not self._available:
            return False
        
        try:
            key = self._coord_key(lat, lon)
            self._redis.setex(key, self.TTL_COORDINATES, json.dumps(data))
            logger.debug(f"Cached data for coordinates ({lat}, {lon}), TTL={self.TTL_COORDINATES}s")
            return True
        except Exception as e:
            logger.warning(f"Cache write error for coordinates: {e}")
            return False
    
    def get_capabilities(self, wfs_url: str) -> Optional[List[str]]:
        """
        Get cached WFS capabilities (feature types).
        
        Args:
            wfs_url: WFS service URL
            
        Returns:
            List of feature type names or None if not cached
        """
        if not self._available:
            return None
        
        try:
            key = self._capabilities_key(wfs_url)
            data = self._redis.get(key)
            if data:
                logger.debug(f"Cache HIT for capabilities: {wfs_url}")
                return json.loads(data)
            else:
                logger.debug(f"Cache MISS for capabilities: {wfs_url}")
                return None
        except Exception as e:
            logger.warning(f"Cache read error for capabilities: {e}")
            return None
    
    def set_capabilities(self, wfs_url: str, feature_types: List[str]) -> bool:
        """
        Cache WFS capabilities (feature types).
        
        Args:
            wfs_url: WFS service URL
            feature_types: List of feature type names
            
        Returns:
            True if cached successfully, False otherwise
        """
        if not self._available:
            return False
        
        try:
            key = self._capabilities_key(wfs_url)
            self._redis.setex(key, self.TTL_CAPABILITIES, json.dumps(feature_types))
            logger.debug(f"Cached capabilities for {wfs_url}, TTL={self.TTL_CAPABILITIES}s")
            return True
        except Exception as e:
            logger.warning(f"Cache write error for capabilities: {e}")
            return False
    
    def get_geometry(self, cadastral_ref: str) -> Optional[Dict[str, Any]]:
        """
        Get cached parcel geometry.
        
        Args:
            cadastral_ref: Cadastral reference
            
        Returns:
            GeoJSON geometry dict or None if not cached
        """
        if not self._available:
            return None
        
        try:
            key = self._geometry_key(cadastral_ref)
            data = self._redis.get(key)
            if data:
                logger.debug(f"Cache HIT for geometry: {cadastral_ref}")
                return json.loads(data)
            else:
                logger.debug(f"Cache MISS for geometry: {cadastral_ref}")
                return None
        except Exception as e:
            logger.warning(f"Cache read error for geometry: {e}")
            return None
    
    def set_geometry(self, cadastral_ref: str, geometry: Dict[str, Any]) -> bool:
        """
        Cache parcel geometry.
        
        Args:
            cadastral_ref: Cadastral reference
            geometry: GeoJSON geometry dict
            
        Returns:
            True if cached successfully, False otherwise
        """
        if not self._available:
            return False
        
        try:
            key = self._geometry_key(cadastral_ref)
            self._redis.setex(key, self.TTL_GEOMETRY, json.dumps(geometry))
            logger.debug(f"Cached geometry for {cadastral_ref}, TTL={self.TTL_GEOMETRY}s")
            return True
        except Exception as e:
            logger.warning(f"Cache write error for geometry: {e}")
            return False
    
    def get_parcel_buildings(self, key: str) -> Optional[List[Dict[str, Any]]]:
        """Cached building features for one parcel, or None when not cached."""
        if not self._available:
            return None
        try:
            data = self._redis.get(f"{self.PREFIX}:parcel_buildings:{key}")
            return json.loads(data) if data else None
        except Exception as e:
            logger.warning(f"Cache read error for parcel buildings: {e}")
            return None

    def set_parcel_buildings(self, key: str, features: List[Dict[str, Any]]) -> bool:
        """Cache building features for one parcel (empty lists included)."""
        if not self._available:
            return False
        try:
            self._redis.setex(
                f"{self.PREFIX}:parcel_buildings:{key}",
                self.TTL_PARCEL_BUILDINGS,
                json.dumps(features),
            )
            return True
        except Exception as e:
            logger.warning(f"Cache write error for parcel buildings: {e}")
            return False

    def invalidate_by_coordinates(self, lat: float, lon: float) -> bool:
        """
        Invalidate cached data for specific coordinates.
        
        Args:
            lat: Latitude
            lon: Longitude
            
        Returns:
            True if key was deleted, False otherwise
        """
        if not self._available:
            return False
        
        try:
            key = self._coord_key(lat, lon)
            deleted = self._redis.delete(key)
            logger.debug(f"Invalidated cache for coordinates ({lat}, {lon}): {deleted > 0}")
            return deleted > 0
        except Exception as e:
            logger.warning(f"Cache invalidation error: {e}")
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.
        
        Returns:
            Dict with cache stats (keys count, memory usage, etc.)
        """
        if not self._available:
            return {"available": False}
        
        try:
            info = self._redis.info("memory")
            keys = self._redis.keys(f"{self.PREFIX}:*")
            return {
                "available": True,
                "keys_count": len(keys),
                "used_memory": info.get("used_memory_human", "unknown"),
                "ttl_coordinates": self.TTL_COORDINATES,
                "ttl_capabilities": self.TTL_CAPABILITIES,
                "ttl_geometry": self.TTL_GEOMETRY,
            }
        except Exception as e:
            logger.warning(f"Error getting cache stats: {e}")
            return {"available": True, "error": str(e)}


# Global cache instance (singleton pattern)
_cache_instance: Optional[CadastralCache] = None


def get_cache() -> CadastralCache:
    """
    Get or create the global cache instance.
    
    Returns:
        CadastralCache instance
    """
    global _cache_instance
    if _cache_instance is None:
        _cache_instance = CadastralCache()
    return _cache_instance
