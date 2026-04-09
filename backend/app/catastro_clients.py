#!/usr/bin/env python3
# =============================================================================
# Cadastral Clients - Integration with Official Cadastral Services
# =============================================================================
# Clients for Spanish State, Navarra, and Euskadi cadastral services

import logging
import json
import hashlib
from typing import Dict, Any, Optional, Tuple, List
from zeep import Client, Settings
from zeep.exceptions import Fault, TransportError
import requests
from xml.etree import ElementTree as ET
import re

logger = logging.getLogger(__name__)

try:
    from shapely.geometry import Point, shape
except Exception:
    Point = None
    shape = None

# Try to import cache service for capabilities caching
try:
    from cache_service import get_cache
    _cache = get_cache()
except ImportError:
    logger.warning("Cache service not available for capabilities discovery")
    _cache = None


def _feature_distance_rank(feature: Dict[str, Any], longitude: float, latitude: float) -> Tuple[int, float]:
    """
    Rank a GeoJSON feature against a click point.

    Returns:
        (contains_priority, distance)
        contains_priority: 0 when point is contained, 1 otherwise.
        distance: geometric distance in source CRS units.
    """
    if not feature or not feature.get('geometry') or Point is None or shape is None:
        return (1, float('inf'))

    try:
        geom = shape(feature['geometry'])
        if geom.is_empty:
            return (1, float('inf'))
        click_point = Point(longitude, latitude)
        if geom.contains(click_point) or geom.touches(click_point):
            return (0, 0.0)
        return (1, float(geom.distance(click_point)))
    except Exception:
        return (1, float('inf'))


def _select_best_feature(features: List[Dict[str, Any]], longitude: float, latitude: float) -> Optional[Dict[str, Any]]:
    """
    Select the best cadastral feature for a click point.
    Prioritizes geometries containing/touching the point, then nearest by distance.
    """
    if not features:
        return None
    if len(features) == 1:
        return features[0]
    return min(features, key=lambda f: _feature_distance_rank(f, longitude, latitude))


class WFSCapabilitiesDiscovery:
    """
    Utility class for discovering WFS capabilities and available feature types.
    Uses Redis caching to avoid repeated GetCapabilities requests (TTL 7 days).
    """
    
    # Cache TTL for capabilities (7 days in seconds)
    CAPABILITIES_TTL = 604800
    
    @staticmethod
    def discover_feature_types(
        wfs_url: str,
        fallback_types: List[str] = None,
        timeout: int = 10
    ) -> List[str]:
        """
        Discover available feature types from a WFS service using GetCapabilities.
        
        Args:
            wfs_url: Base URL of the WFS service
            fallback_types: List of feature types to return if discovery fails
            timeout: Request timeout in seconds
            
        Returns:
            List of feature type names (e.g., ['CATAST_Pol_ParcelaUrba', 'CATAST_Pol_ParcelaRusti'])
        """
        fallback_types = fallback_types or []
        
        # Check cache first
        if _cache and _cache.is_available:
            cached = _cache.get_capabilities(wfs_url)
            if cached:
                logger.debug(f"Using cached capabilities for {wfs_url}: {len(cached)} feature types")
                return cached
        
        try:
            # Make GetCapabilities request
            params = {
                'service': 'WFS',
                'version': '2.0.0',
                'request': 'GetCapabilities'
            }
            
            logger.info(f"Discovering capabilities for WFS: {wfs_url}")
            response = requests.get(wfs_url, params=params, timeout=timeout)
            
            if response.status_code != 200:
                logger.warning(f"GetCapabilities failed for {wfs_url}: status {response.status_code}")
                return fallback_types
            
            # Parse XML response
            root = ET.fromstring(response.content)
            
            # WFS 2.0 namespaces
            namespaces = {
                'wfs': 'http://www.opengis.net/wfs/2.0',
                'ows': 'http://www.opengis.net/ows/1.1',
            }
            
            # Try to find FeatureTypeList
            feature_types = []
            
            # Try WFS 2.0 structure first
            feature_type_list = root.find('.//wfs:FeatureTypeList', namespaces)
            if feature_type_list is not None:
                for ft in feature_type_list.findall('.//wfs:FeatureType', namespaces):
                    name = ft.find('wfs:Name', namespaces)
                    if name is not None and name.text:
                        feature_types.append(name.text.strip())
            
            # Try without namespaces if nothing found
            if not feature_types:
                feature_type_list = root.find('.//FeatureTypeList')
                if feature_type_list is not None:
                    for ft in feature_type_list.findall('.//FeatureType'):
                        name = ft.find('Name')
                        if name is not None and name.text:
                            feature_types.append(name.text.strip())
            
            # Try OWS namespace (some WFS servers use this)
            if not feature_types:
                for ft in root.findall('.//{http://www.opengis.net/wfs/2.0}FeatureType'):
                    name = ft.find('{http://www.opengis.net/wfs/2.0}Name')
                    if name is not None and name.text:
                        feature_types.append(name.text.strip())
            
            if feature_types:
                logger.info(f"Discovered {len(feature_types)} feature types from {wfs_url}")
                
                # Cache the discovered types
                if _cache and _cache.is_available:
                    _cache.set_capabilities(wfs_url, feature_types)
                
                return feature_types
            else:
                logger.warning(f"No feature types found in GetCapabilities for {wfs_url}")
                return fallback_types
                
        except requests.exceptions.Timeout:
            logger.warning(f"GetCapabilities timeout for {wfs_url}")
            return fallback_types
        except requests.exceptions.RequestException as e:
            logger.warning(f"GetCapabilities request failed for {wfs_url}: {e}")
            return fallback_types
        except ET.ParseError as e:
            logger.warning(f"GetCapabilities XML parse error for {wfs_url}: {e}")
            return fallback_types
        except Exception as e:
            logger.error(f"Unexpected error in GetCapabilities for {wfs_url}: {e}", exc_info=True)
            return fallback_types
    
    @staticmethod
    def filter_cadastral_types(feature_types: List[str]) -> List[str]:
        """
        Filter and sort feature types to prioritize cadastral parcels.
        Sorts to specific priority keywords first, then others.
        Excludes administrative boundaries explicitly.
        
        Args:
            feature_types: List of all feature types
            
        Returns:
            Filtered and sorted list
        """
        # Primary keywords (definitely parcels)
        primary_keywords = ['parcel', 'finca', 'predio', 'cp:cadastralparcel']
        
        # Secondary keywords (related to cadastre but less specific)
        secondary_keywords = ['catast', 'rustic', 'urban', 'cp:']
        
        # Excluded keywords (administrative units, text, lines)
        excluded_keywords = [
            'municipio', 'concejo', 'cascourbano', 'poligono', 
            'txt', 'lin_', 'line', 'pt_', 'text', 'edif'
        ]
        
        primary_matches = []
        secondary_matches = []
        
        for ft in feature_types:
            ft_lower = ft.lower()
            
            # Skip excluded types
            if any(keyword in ft_lower for keyword in excluded_keywords):
                continue
            
            # Check primary match
            if any(keyword in ft_lower for keyword in primary_keywords):
                primary_matches.append(ft)
                continue
                
            # Check secondary match
            if any(keyword in ft_lower for keyword in secondary_keywords):
                secondary_matches.append(ft)
                
        # Combine lists with primary first
        result = primary_matches + secondary_matches
        
        # If filtering removed everything (e.g. strict exclusion), fallback to original list
        # but try to filter excluded ones at least
        if not result and feature_types:
            return [ft for ft in feature_types if not any(k in ft.lower() for k in excluded_keywords)]
            
        return result


class SpanishStateCatastroClient:
    """
    Client for Spanish State Cadastre (DGC - Dirección General del Catastro).
    Uses SOAP service: OVCCoordenadas.asmx
    """

    SOAP_WSDL_URL = "https://ovc.catastro.meh.es/ovcservweb/OVCSWLocalizacionRC/OVCCoordenadas.asmx?WSDL"
    SOAP_SERVICE_URL = "https://ovc.catastro.meh.es/ovcservweb/OVCSWLocalizacionRC/OVCCoordenadas.asmx"

    def __init__(self):
        """Initialize the SOAP client."""
        self.client = None
        self._init_client()

    def _init_client(self):
        """Initialize Zeep SOAP client with appropriate settings."""
        try:
            settings = Settings(
                strict=False,
                xml_huge_tree=True,
                raw_response=True  # Get raw XML response for better control
            )
            self.client = Client(wsdl=self.SOAP_WSDL_URL, settings=settings)
            logger.info("Spanish State Catastro SOAP client initialized")
        except Exception as e:
            logger.error(f"Failed to initialize SOAP client: {e}")
            self.client = None

    def get_parcel_geometry(
        self,
        cadastral_reference: str,
        srs: str = "4326"
    ) -> Optional[Dict[str, Any]]:
        """
        Get parcel geometry (polygon) by cadastral reference.
        Uses WFS INSPIRE service for full polygon geometry.
        Falls back to Consulta_CPMRC for centroid if WFS fails.
        
        Args:
            cadastral_reference: Cadastral reference (e.g., "19078B6-1009001")
            srs: Spatial reference system (default: "4326" for WGS84)
            
        Returns:
            Dictionary with geometry (GeoJSON Polygon) or None
        """
        # Try WFS INSPIRE service first (provides full polygon geometry)
        geometry = self._get_geometry_from_wfs(cadastral_reference, srs)
        if geometry:
            return geometry
        
        # Fallback: Use Consulta_CPMRC for centroid (creates buffer polygon)
        logger.info(f"WFS failed, trying Consulta_CPMRC for {cadastral_reference}")
        return self._get_geometry_from_soap(cadastral_reference, srs)
    
    def _get_geometry_from_wfs(
        self,
        cadastral_reference: str,
        srs: str = "4326"
    ) -> Optional[Dict[str, Any]]:
        """
        Get parcel geometry from WFS INSPIRE service using stored query GetParcel.
        This provides the full polygon geometry.
        
        Documentation: https://www.catastro.hacienda.gob.es/webinspire/documentos/inspire-cp-WFS.pdf
        """
        try:
            import requests
            from lxml import etree
            
            wfs_url = "http://ovc.catastro.meh.es/INSPIRE/wfsCP.aspx"
            
            # Normalizar SRS a formato esperado (EPSG::XXXX). Evitar triples dos puntos.
            srs_code = str(srs)
            if srs_code.upper().startswith("EPSG"):
                srs_code = srs_code.split(":")[-1]
            srs_name = f"EPSG::{srs_code}"
            
            # Use stored query GetParcel with refcat parameter
            # Format: refcat should be the cadastral reference without dashes, exactly 14 characters
            refcat = cadastral_reference.replace('-', '')
            # Ensure it's exactly 14 characters (pad or truncate if needed)
            if len(refcat) > 14:
                refcat = refcat[:14]
            elif len(refcat) < 14:
                refcat = refcat.ljust(14, '0')
            
            # Parámetros según documentación WFS 2.0 con storedquery_id
            params = {
                'service': 'WFS',
                'version': '2.0.0',
                'request': 'GetFeature',
                'storedquery_id': 'GetParcel',
                'REFCAT': refcat,
                'SRSNAME': srs_name,
            }
            
            logger.info(f"Requesting geometry from WFS INSPIRE for refcat={refcat}, srs={srs_name}")
            logger.info(f"WFS URL: {wfs_url}, params: {params}")
            response = requests.get(wfs_url, params=params, timeout=15)
            logger.info(f"WFS response status: {response.status_code}, content length: {len(response.content)}")
            # Si devuelve 404, intentar versión 1.1.0 como fallback
            if response.status_code == 404:
                fallback_params = params.copy()
                fallback_params['version'] = '1.1.0'
                logger.warning(f"WFS 2.0 returned 404, retrying with 1.1.0 for refcat={refcat}")
                response = requests.get(wfs_url, params=fallback_params, timeout=15)
                logger.info(f"WFS 1.1.0 response status: {response.status_code}")
            response.raise_for_status()
            
            # Log raw response BEFORE parsing (critical for debugging)
            try:
                response_text = response.text if hasattr(response, 'text') else response.content.decode('utf-8', errors='ignore')
                logger.info(f"WFS raw response (first 1000 chars):\n{response_text[:1000]}")
                logger.debug(f"WFS raw response full length: {len(response_text)} chars")
            except Exception as e:
                logger.warning(f"Could not log raw response: {e}")
            
            # Parse GML/XML response
            try:
                # Try to parse as XML/GML
                logger.info(f"Parsing WFS XML response, content length: {len(response.content)}")
                xml_elem = etree.fromstring(response.content)
                logger.info(f"Parsed WFS XML response, root tag: {xml_elem.tag}")
                
                # Look for gml:Polygon or gml:MultiPolygon in the response
                # Namespaces used by INSPIRE
                namespaces = {
                    'gml': 'http://www.opengis.net/gml/3.2',
                    'cp': 'http://inspire.ec.europa.eu/schemas/cp/4.0',
                    'wfs': 'http://www.opengis.net/wfs/2.0',
                    'fes': 'http://www.opengis.net/fes/2.0'
                }
                
                # First, try to find cp:geometry element (INSPIRE structure)
                logger.debug(f"Searching for cp:geometry with namespaces: {namespaces}")
                cp_geometry = xml_elem.find('.//cp:geometry', namespaces)
                if cp_geometry is None:
                    cp_geometry = xml_elem.find('.//{http://inspire.ec.europa.eu/schemas/cp/4.0}geometry')
                if cp_geometry is None:
                    cp_geometry = xml_elem.find('.//geometry')
                
                # Search for polygon inside cp:geometry if found, otherwise search in entire XML
                search_root = cp_geometry if cp_geometry is not None else xml_elem
                
                if cp_geometry is not None:
                    logger.info(f"Found cp:geometry element, searching for polygon inside it")
                else:
                    logger.warning(f"cp:geometry not found, searching in entire XML")
                
                # INSPIRE WFS often returns MultiSurface with Surface/PolygonPatch structure
                # Try MultiSurface first (most common in INSPIRE WFS)
                polygon = None
                multi_surface = search_root.find('.//gml:MultiSurface', namespaces)
                if multi_surface is None:
                    multi_surface = search_root.find('.//{http://www.opengis.net/gml/3.2}MultiSurface')
                if multi_surface is None:
                    multi_surface = search_root.find('.//MultiSurface')
                
                if multi_surface is not None:
                    logger.info(f"Found MultiSurface, searching for Surface/PolygonPatch")
                    # Find Surface inside MultiSurface (can be in surfaceMember)
                    surface = multi_surface.find('.//gml:Surface', namespaces)
                    if surface is None:
                        surface = multi_surface.find('.//{http://www.opengis.net/gml/3.2}Surface')
                    if surface is None:
                        surface = multi_surface.find('.//Surface')
                    
                    if surface is not None:
                        logger.info(f"Found Surface, searching for patches/PolygonPatch")
                        # Find patches inside Surface - try direct child first, then recursive
                        patches = surface.find('gml:patches', namespaces)
                        if patches is None:
                            patches = surface.find('{http://www.opengis.net/gml/3.2}patches')
                        if patches is None:
                            patches = surface.find('patches')
                        if patches is None:
                            # Try recursive search
                            patches = surface.find('.//gml:patches', namespaces)
                        if patches is None:
                            patches = surface.find('.//{http://www.opengis.net/gml/3.2}patches')
                        if patches is None:
                            patches = surface.find('.//patches')
                        
                        logger.info(f"patches element found: {patches is not None}")
                        
                        # Find PolygonPatch inside patches or directly in Surface
                        if patches is not None:
                            # Try direct child first
                            polygon = patches.find('gml:PolygonPatch', namespaces)
                            if polygon is None:
                                polygon = patches.find('{http://www.opengis.net/gml/3.2}PolygonPatch')
                            if polygon is None:
                                polygon = patches.find('PolygonPatch')
                            if polygon is None:
                                # Try recursive
                                polygon = patches.find('.//gml:PolygonPatch', namespaces)
                            if polygon is None:
                                polygon = patches.find('.//{http://www.opengis.net/gml/3.2}PolygonPatch')
                            if polygon is None:
                                polygon = patches.find('.//PolygonPatch')
                        else:
                            # Try directly in Surface if no patches element
                            polygon = surface.find('.//gml:PolygonPatch', namespaces)
                            if polygon is None:
                                polygon = surface.find('.//{http://www.opengis.net/gml/3.2}PolygonPatch')
                            if polygon is None:
                                polygon = surface.find('.//PolygonPatch')
                        
                        if polygon is not None:
                            logger.info(f"Found PolygonPatch inside MultiSurface/Surface/patches structure")
                        else:
                            logger.warning(f"PolygonPatch not found in Surface/patches. Surface XML: {etree.tostring(surface, encoding='unicode', pretty_print=True)[:1000]}")
                    else:
                        logger.warning(f"Surface not found in MultiSurface")
                
                # If MultiSurface didn't work, try to find PolygonPatch directly (fallback)
                if polygon is None:
                    polygon_patch = search_root.find('.//gml:PolygonPatch', namespaces)
                    if polygon_patch is None:
                        polygon_patch = search_root.find('.//{http://www.opengis.net/gml/3.2}PolygonPatch')
                    if polygon_patch is None:
                        polygon_patch = search_root.find('.//PolygonPatch')
                    polygon = polygon_patch
                
                # Try to find polygon geometry (standard Polygon)
                if polygon is None:
                    polygon = search_root.find('.//gml:Polygon', namespaces)
                if polygon is None:
                    polygon = search_root.find('.//{http://www.opengis.net/gml/3.2}Polygon')
                if polygon is None:
                    polygon = search_root.find('.//Polygon')
                
                # Try MultiPolygon if Polygon not found
                if polygon is None:
                    multi_polygon = search_root.find('.//gml:MultiPolygon', namespaces)
                    if multi_polygon is None:
                        multi_polygon = search_root.find('.//{http://www.opengis.net/gml/3.2}MultiPolygon')
                    if multi_polygon is None:
                        multi_polygon = search_root.find('.//MultiPolygon')
                    
                    if multi_polygon is not None:
                        # For MultiPolygon, take the first polygon member
                        polygon = multi_polygon.find('.//gml:Polygon', namespaces)
                        if polygon is None:
                            polygon = multi_polygon.find('.//{http://www.opengis.net/gml/3.2}Polygon')
                        if polygon is None:
                            polygon = multi_polygon.find('.//Polygon')
                
                
                if polygon is None:
                    # Log response for debugging
                    xml_str = etree.tostring(xml_elem, encoding='unicode', pretty_print=True)
                    logger.warning(f"No polygon found in WFS response for {cadastral_reference}")
                    logger.warning(f"cp:geometry found: {cp_geometry is not None}")
                    logger.warning(f"MultiSurface found: {multi_surface is not None if 'multi_surface' in locals() else False}")
                    if 'multi_surface' in locals() and multi_surface is not None:
                        logger.warning(f"MultiSurface XML: {etree.tostring(multi_surface, encoding='unicode', pretty_print=True)[:1000]}")
                    logger.warning(f"Response (first 3000 chars):\n{xml_str[:3000]}")
                    
                    # Try recursive search for posList as fallback (more permissive parser)
                    logger.info(f"Attempting recursive posList search as fallback for {cadastral_reference}")
                    coords = self._extract_coordinates_recursive(xml_elem, namespaces)
                    if coords and len(coords) >= 3:
                        logger.info(f"Recursive parser found {len(coords)} coordinates for {cadastral_reference}")
                        
                        # Validate coordinates before creating geometry
                        if not self._validate_coordinates(coords, cadastral_reference):
                            logger.warning(f"Invalid coordinates from recursive parser for {cadastral_reference}")
                            return None
                        
                        # Close polygon if not already closed
                        if coords[0] != coords[-1]:
                            coords.append(coords[0])
                        
                        geometry = {
                            'type': 'Polygon',
                            'coordinates': [coords]
                        }
                        
                        # Final validation of geometry structure
                        if not self._validate_geometry(geometry, cadastral_reference):
                            logger.warning(f"Invalid geometry structure from recursive parser for {cadastral_reference}")
                            return None
                        
                        logger.info(f"Successfully extracted polygon geometry using recursive parser for {cadastral_reference} with {len(coords)} points")
                        return geometry
                    else:
                        logger.warning(f"Recursive parser also failed to find coordinates for {cadastral_reference}")
                    return None
                
                # Extract coordinates from gml:exterior/gml:LinearRing/gml:posList or gml:pos
                exterior = polygon.find('.//gml:exterior', namespaces)
                if exterior is None:
                    exterior = polygon.find('.//{http://www.opengis.net/gml/3.2}exterior')
                if exterior is None:
                    exterior = polygon.find('.//exterior')
                
                if exterior is None:
                    logger.warning(f"No exterior ring found in polygon for {cadastral_reference}")
                    return None
                
                # Find LinearRing
                linear_ring = exterior.find('.//gml:LinearRing', namespaces)
                if linear_ring is None:
                    linear_ring = exterior.find('.//{http://www.opengis.net/gml/3.2}LinearRing')
                if linear_ring is None:
                    linear_ring = exterior.find('.//LinearRing')
                
                if linear_ring is None:
                    logger.warning(f"No LinearRing found in exterior for {cadastral_reference}")
                    return None
                
                # Try posList first (most common)
                pos_list = linear_ring.find('.//gml:posList', namespaces)
                if pos_list is None:
                    pos_list = linear_ring.find('.//{http://www.opengis.net/gml/3.2}posList')
                if pos_list is None:
                    pos_list = linear_ring.find('.//posList')
                
                coords = []
                
                if pos_list is not None and pos_list.text:
                    # posList contains space-separated coordinates
                    # INSPIRE WFS may return "lat lon" or "lon lat" - check srsName or try both
                    coord_pairs = pos_list.text.strip().split()
                    # Check if coordinates are in lat/lon order (common in INSPIRE) or lon/lat
                    # Try lat/lon first (INSPIRE standard), then lon/lat if that doesn't make sense
                    for i in range(0, len(coord_pairs) - 1, 2):
                        if i + 1 < len(coord_pairs):
                            try:
                                val1 = float(coord_pairs[i])
                                val2 = float(coord_pairs[i + 1])
                                # INSPIRE often uses lat/lon order, but we need lon/lat for GeoJSON
                                # If first value is > 90 or < -90, it's likely longitude
                                # If first value is between -90 and 90, it's likely latitude
                                if abs(val1) <= 90 and abs(val2) <= 180:
                                    # Likely lat/lon order, swap to lon/lat
                                    lat = val1
                                    lon = val2
                                else:
                                    # Likely lon/lat order
                                    lon = val1
                                    lat = val2
                                coords.append([lon, lat])
                            except (ValueError, IndexError):
                                continue
                else:
                    # Try individual gml:pos elements
                    pos_elements = linear_ring.findall('.//gml:pos', namespaces)
                    if not pos_elements:
                        pos_elements = linear_ring.findall('.//{http://www.opengis.net/gml/3.2}pos')
                    if not pos_elements:
                        pos_elements = linear_ring.findall('.//pos')
                    
                    for pos in pos_elements:
                        if pos.text:
                            try:
                                parts = pos.text.strip().split()
                                if len(parts) >= 2:
                                    lon = float(parts[0])
                                    lat = float(parts[1])
                                    coords.append([lon, lat])
                            except (ValueError, IndexError):
                                continue
                
                if len(coords) < 3:
                    logger.warning(f"Insufficient coordinates extracted from WFS response for {cadastral_reference}: {len(coords)}")
                    return None
                
                # Validate coordinates before creating geometry
                if not self._validate_coordinates(coords, cadastral_reference):
                    logger.warning(f"Invalid coordinates for {cadastral_reference} - validation failed")
                    return None
                
                # Close polygon if not already closed
                if coords[0] != coords[-1]:
                    coords.append(coords[0])
                
                # Transform coordinates if needed (WFS may return in different SRS)
                # For now, assume coordinates are already in the requested SRS
                
                geometry = {
                    'type': 'Polygon',
                    'coordinates': [coords]
                }
                
                # Final validation of geometry structure
                if not self._validate_geometry(geometry, cadastral_reference):
                    logger.warning(f"Invalid geometry structure for {cadastral_reference}")
                    return None
                
                logger.info(f"Successfully extracted polygon geometry from WFS for {cadastral_reference} with {len(coords)} points")
                return geometry
                
            except etree.XMLSyntaxError as e:
                logger.error(f"XML parsing error in WFS response for {cadastral_reference}: {e}")
                logger.debug(f"Response content (first 1000 chars): {response.text[:1000] if hasattr(response, 'text') else response.content[:1000]}")
                return None
            except Exception as e:
                logger.error(f"Error parsing WFS GML response for {cadastral_reference}: {e}", exc_info=True)
                return None
                
        except requests.exceptions.RequestException as e:
            logger.warning(f"WFS request failed for {cadastral_reference}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error in WFS geometry retrieval for {cadastral_reference}: {e}", exc_info=True)
            return None
    
    def _extract_coordinates_recursive(self, xml_elem: Any, namespaces: Dict[str, str]) -> Optional[List[List[float]]]:
        """
        Permissive recursive parser that searches for posList or coordinates anywhere in the XML tree,
        regardless of parent structure. This is more robust when XML structure varies.
        
        Args:
            xml_elem: XML element to search in
            namespaces: XML namespaces dictionary
            
        Returns:
            List of coordinate pairs [[lon, lat], ...] or None if not found
        """
        try:
            from lxml import etree
            
            coords = []
            
            # Strategy 1: Find all posList elements recursively (most common in GML)
            pos_lists = []
            # Try with namespaces
            pos_lists.extend(xml_elem.findall('.//{http://www.opengis.net/gml/3.2}posList', namespaces))
            pos_lists.extend(xml_elem.findall('.//gml:posList', namespaces))
            # Try without namespace
            pos_lists.extend(xml_elem.findall('.//posList'))
            
            if pos_lists:
                # Use the first posList found
                pos_list = pos_lists[0]
                if pos_list.text:
                    logger.info(f"Found posList via recursive search: {len(pos_list.text)} chars")
                    coord_pairs = pos_list.text.strip().split()
                    for i in range(0, len(coord_pairs) - 1, 2):
                        if i + 1 < len(coord_pairs):
                            try:
                                val1 = float(coord_pairs[i])
                                val2 = float(coord_pairs[i + 1])
                                # Detect order: lat/lon vs lon/lat
                                if abs(val1) <= 90 and abs(val2) <= 180:
                                    # Likely lat/lon order, swap to lon/lat
                                    coords.append([val2, val1])
                                else:
                                    # Likely lon/lat order
                                    coords.append([val1, val2])
                            except (ValueError, IndexError):
                                continue
                    if len(coords) >= 3:
                        return coords
            
            # Strategy 2: Find all pos elements recursively
            pos_elements = []
            pos_elements.extend(xml_elem.findall('.//{http://www.opengis.net/gml/3.2}pos', namespaces))
            pos_elements.extend(xml_elem.findall('.//gml:pos', namespaces))
            pos_elements.extend(xml_elem.findall('.//pos'))
            
            if pos_elements:
                logger.info(f"Found {len(pos_elements)} pos elements via recursive search")
                for pos in pos_elements:
                    if pos.text:
                        try:
                            parts = pos.text.strip().split()
                            if len(parts) >= 2:
                                lon = float(parts[0])
                                lat = float(parts[1])
                                coords.append([lon, lat])
                        except (ValueError, IndexError):
                            continue
                if len(coords) >= 3:
                    return coords
            
            # Strategy 3: Try to find coordinates as text content (last resort)
            # Look for patterns like numbers that could be coordinates
            all_text = etree.tostring(xml_elem, method='text', encoding='unicode')
            # This is less reliable, but could catch edge cases
            # We'll skip this for now as it's too error-prone
            
            return coords if len(coords) >= 3 else None
            
        except Exception as e:
            logger.error(f"Error in recursive coordinate extraction: {e}", exc_info=True)
            return None
    
    def _validate_coordinates(self, coords: List[List[float]], cadastral_reference: str) -> bool:
        """
        Validate that coordinates are valid (within reasonable ranges for lat/lon).
        
        Args:
            coords: List of coordinate pairs [[lon, lat], ...]
            cadastral_reference: Cadastral reference for logging
            
        Returns:
            True if coordinates are valid, False otherwise
        """
        if not coords or len(coords) < 3:
            logger.warning(f"Coordinates validation failed for {cadastral_reference}: insufficient points ({len(coords) if coords else 0})")
            return False
        
        for i, coord in enumerate(coords):
            if not isinstance(coord, (list, tuple)) or len(coord) < 2:
                logger.warning(f"Coordinates validation failed for {cadastral_reference}: invalid coordinate format at index {i}")
                return False
            
            try:
                lon, lat = float(coord[0]), float(coord[1])
                
                # Validate longitude range (-180 to 180)
                if not -180 <= lon <= 180:
                    logger.warning(f"Coordinates validation failed for {cadastral_reference}: invalid longitude {lon} at index {i}")
                    return False
                
                # Validate latitude range (-90 to 90)
                if not -90 <= lat <= 90:
                    logger.warning(f"Coordinates validation failed for {cadastral_reference}: invalid latitude {lat} at index {i}")
                    return False
                
                # Check for NaN or infinite values
                if not (isinstance(lon, float) and isinstance(lat, float)) or \
                   lon != lon or lat != lat:  # NaN check (NaN != NaN)
                    logger.warning(f"Coordinates validation failed for {cadastral_reference}: NaN or invalid float at index {i}")
                    return False
                    
            except (ValueError, TypeError) as e:
                logger.warning(f"Coordinates validation failed for {cadastral_reference}: could not convert to float at index {i}: {e}")
                return False
        
        logger.debug(f"Coordinates validation passed for {cadastral_reference}: {len(coords)} valid points")
        return True
    
    def _validate_geometry(self, geometry: Dict[str, Any], cadastral_reference: str) -> bool:
        """
        Validate that geometry structure is correct (GeoJSON Polygon format).
        
        Args:
            geometry: Geometry dictionary (should be GeoJSON Polygon)
            cadastral_reference: Cadastral reference for logging
            
        Returns:
            True if geometry is valid, False otherwise
        """
        if not isinstance(geometry, dict):
            logger.warning(f"Geometry validation failed for {cadastral_reference}: not a dictionary")
            return False
        
        if geometry.get('type') != 'Polygon':
            logger.warning(f"Geometry validation failed for {cadastral_reference}: invalid type '{geometry.get('type')}', expected 'Polygon'")
            return False
        
        if 'coordinates' not in geometry:
            logger.warning(f"Geometry validation failed for {cadastral_reference}: missing 'coordinates' field")
            return False
        
        coords = geometry.get('coordinates')
        if not isinstance(coords, list) or len(coords) == 0:
            logger.warning(f"Geometry validation failed for {cadastral_reference}: invalid coordinates structure")
            return False
        
        # Polygon coordinates should be a list of rings (first is exterior, rest are holes)
        ring = coords[0]
        if not isinstance(ring, list) or len(ring) < 3:
            logger.warning(f"Geometry validation failed for {cadastral_reference}: exterior ring has insufficient points ({len(ring) if isinstance(ring, list) else 0})")
            return False
        
        # Check that polygon is closed (first and last point should be the same)
        if ring[0] != ring[-1]:
            logger.debug(f"Geometry validation: polygon for {cadastral_reference} is not closed, but this is acceptable (will be handled)")
        
        logger.debug(f"Geometry validation passed for {cadastral_reference}")
        return True
    
    def _get_geometry_from_soap(
        self,
        cadastral_reference: str,
        srs: str = "4326"
    ) -> Optional[Dict[str, Any]]:
        """
        Get parcel geometry from SOAP Consulta_CPMRC (centroid only).
        Creates a small buffer polygon around the centroid.
        """
        if not self.client:
            logger.error("SOAP client not initialized")
            return None
        
        try:
            from lxml import etree
            
            # Call Consulta_CPMRC method
            # Method signature: Consulta_CPMRC(RC, Provincia, Municipio, SRS)
            srs_str = f"EPSG:{srs}" if not srs.startswith("EPSG:") else srs
            
            # Extract province and municipality codes from reference
            ref_parts = cadastral_reference.split('-')
            if len(ref_parts) > 0 and len(ref_parts[0]) >= 5:
                provincia_code = ref_parts[0][:2]
                municipio_code = ref_parts[0][2:5]
            else:
                provincia_code = ''
                municipio_code = ''
            
            # Consulta_CPMRC requires RC to be exactly 14 characters without dashes
            # Format: 2 chars (province) + 3 chars (municipality) + 9 chars (parcel) = 14 total
            # Example: 16117B5-1300144 -> 16117B51300144 (15 chars) -> need to normalize to 14
            rc_normalized = cadastral_reference.replace('-', '')
            # Remove any non-alphanumeric characters
            rc_normalized = ''.join(c for c in rc_normalized if c.isalnum())
            
            # If longer than 14, try to preserve province (2) + municipality (3) = first 5 chars
            # Then take next 9 chars for parcel number
            if len(rc_normalized) > 14:
                # Take first 14 characters (province + municipality + first 9 of parcel)
                rc_normalized = rc_normalized[:14]
                logger.warning(f"RC too long ({len(cadastral_reference.replace('-', ''))}), truncated to 14: {rc_normalized}")
            elif len(rc_normalized) < 14:
                # Pad with zeros at the end (parcel number part)
                original_len = len(rc_normalized)
                rc_normalized = rc_normalized.ljust(14, '0')
                logger.warning(f"RC too short ({original_len}), padded to 14: {rc_normalized}")
            
            logger.info(f"Normalized RC: {rc_normalized} (length: {len(rc_normalized)}) from {cadastral_reference}")
            
            logger.debug(f"Calling Consulta_CPMRC with RC={rc_normalized} (normalized from {cadastral_reference}), Provincia={provincia_code}, Municipio={municipio_code}, SRS={srs_str}")
            
            result = self.client.service.Consulta_CPMRC(
                RC=rc_normalized,
                Provincia=provincia_code,
                Municipio=municipio_code,
                SRS=srs_str
            )
            
            # Parse XML response
            xml_elem = None
            if hasattr(result, 'content'):
                xml_elem = etree.fromstring(result.content)
            elif hasattr(result, 'text'):
                text = result.text
                xml_elem = etree.fromstring(text.encode('utf-8') if isinstance(text, str) else text)
            elif hasattr(result, 'find'):
                xml_elem = result
            else:
                logger.error(f"Unexpected result type: {type(result)}")
                return None
            
            # Log raw XML structure for debugging BEFORE parsing attempts
            xml_str = etree.tostring(xml_elem, encoding='unicode', pretty_print=True)
            logger.info(f"Consulta_CPMRC raw XML response (first 1000 chars):\n{xml_str[:1000]}")
            logger.debug(f"Consulta_CPMRC full XML response length: {len(xml_str)} chars")
            
            # Find coord element with geometry
            # Try different paths - Consulta_CPMRC structure may vary
            coord_elem = None
            
            # Method 1: Direct coord element
            coord_elem = xml_elem.find('.//{http://www.catastro.meh.es/}coord')
            if coord_elem is None:
                coord_elem = xml_elem.find('.//coord')
            
            # Method 2: Through coordenadas element
            if coord_elem is None:
                coordenadas_elem = xml_elem.find('.//{http://www.catastro.meh.es/}coordenadas')
                if coordenadas_elem is None:
                    coordenadas_elem = xml_elem.find('.//coordenadas')
                if coordenadas_elem is not None:
                    coord_elem = coordenadas_elem.find('.//{http://www.catastro.meh.es/}coord')
                    if coord_elem is None:
                        coord_elem = coordenadas_elem.find('.//coord')
            
            # Method 3: Try Consulta_CPMRCResponse structure
            if coord_elem is None:
                response_elem = xml_elem.find('.//{http://www.catastro.meh.es/}Consulta_CPMRCResponse')
                if response_elem is None:
                    response_elem = xml_elem.find('.//Consulta_CPMRCResponse')
                if response_elem is not None:
                    coord_elem = response_elem.find('.//{http://www.catastro.meh.es/}coord')
                    if coord_elem is None:
                        coord_elem = response_elem.find('.//coord')
            
            if coord_elem is None:
                logger.warning(f"Could not find coord element in Consulta_CPMRC response. XML structure: {xml_str[:1500]}")
                return None
            
            logger.debug(f"Found coord element: {etree.tostring(coord_elem, encoding='unicode')[:500]}")
            
            # Consulta_CPMRC only returns centroid (xcen/ycen), not full polygon
            # Extract centroid and create a buffer polygon
            geo_elem = coord_elem.find('.//{http://www.catastro.meh.es/}geo')
            if geo_elem is None:
                geo_elem = coord_elem.find('.//geo')
            
            # Also try direct geo element in coord
            if geo_elem is None:
                geo_elem = coord_elem.find('{http://www.catastro.meh.es/}geo')
            if geo_elem is None:
                geo_elem = coord_elem.find('geo')
            
            if geo_elem is None:
                logger.warning(f"Could not find geo element in Consulta_CPMRC response. Coord structure: {etree.tostring(coord_elem, encoding='unicode')[:500]}")
                return None
            
            # Get centroid coordinates
            xc = geo_elem.find('{http://www.catastro.meh.es/}xcen')
            if xc is None:
                xc = geo_elem.find('xcen')
            if xc is None:
                xc = geo_elem.find('{http://www.catastro.meh.es/}xc')
            if xc is None:
                xc = geo_elem.find('xc')
            
            yc = geo_elem.find('{http://www.catastro.meh.es/}ycen')
            if yc is None:
                yc = geo_elem.find('ycen')
            if yc is None:
                yc = geo_elem.find('{http://www.catastro.meh.es/}yc')
            if yc is None:
                yc = geo_elem.find('yc')
            
            if xc is not None and yc is not None and xc.text and yc.text:
                try:
                    center_lon = float(xc.text.strip())
                    center_lat = float(yc.text.strip())
                    
                    # Create a small square buffer polygon (about 20 meters)
                    # This is a fallback - ideally we'd get the full polygon from WFS
                    buffer = 0.0002  # ~20 meters in degrees at equator
                    coords = [
                        [center_lon - buffer, center_lat - buffer],
                        [center_lon + buffer, center_lat - buffer],
                        [center_lon + buffer, center_lat + buffer],
                        [center_lon - buffer, center_lat + buffer],
                        [center_lon - buffer, center_lat - buffer]  # Close polygon
                    ]
                    
                    logger.warning(f"Consulta_CPMRC only provides centroid. Created buffer polygon around ({center_lon}, {center_lat}) for {cadastral_reference}")
                    
                    return {
                        'type': 'Polygon',
                        'coordinates': [coords]
                    }
                except (ValueError, TypeError) as e:
                    logger.error(f"Error parsing centroid coordinates: {e}")
                    return None
            else:
                logger.warning("Could not extract centroid from Consulta_CPMRC response")
                return None
                
        except Fault as e:
            logger.warning(f"SOAP Fault getting geometry for {cadastral_reference}: {e}")
            return None
        except TransportError as e:
            logger.error(f"Transport error getting geometry: {e}")
            return None
        except Exception as e:
            logger.error(f"Error getting parcel geometry: {e}", exc_info=True)
            return None

    def query_by_coordinates(
        self,
        longitude: float,
        latitude: float,
        srs: str = "4326"
    ) -> Optional[Dict[str, Any]]:
        """
        Query cadastral reference by coordinates (reverse geocoding).
        
        Args:
            longitude: Longitude in decimal degrees (WGS84 if srs=4326)
            latitude: Latitude in decimal degrees (WGS84 if srs=4326)
            srs: Spatial Reference System code (default: 4326 = WGS84)
                 Valid codes: 4326 (WGS84), 4230 (ED50), 4258 (ETRS89),
                 25830 (UTM 30N ETRS89), 23030 (UTM 30N ED50), etc.
        
        Returns:
            Dictionary with cadastral data or None if not found/error
            {
                'cadastralReference': str,
                'municipality': str,
                'province': str,
                'address': str,
                'coordinates': {'lon': float, 'lat': float},
                'area': float (hectares, if available)
            }
        """
        if not self.client:
            logger.error("SOAP client not initialized")
            return None

        try:
            # Call the SOAP service method Consulta_RCCOOR
            # Method signature: Consulta_RCCOOR(SRS, Coordenada_X, Coordenada_Y)
            # The service expects:
            # - SRS: string (e.g., "EPSG:4326")
            # - Coordenada_X: longitude as string
            # - Coordenada_Y: latitude as string
            srs_str = f"EPSG:{srs}" if not srs.startswith("EPSG:") else srs
            result = self.client.service.Consulta_RCCOOR(
                SRS=srs_str,
                Coordenada_X=str(longitude),
                Coordenada_Y=str(latitude)
            )

            # With raw_response=True, result is a lxml.etree.Element
            # Parse the SOAP response XML directly
            logger.info(f"Calling Consulta_RCCOOR for coordinates ({longitude}, {latitude}), SRS={srs_str}")
            cadastral_data = self._parse_soap_xml_response(result, srs, longitude, latitude)
            
            if cadastral_data:
                # Enrichment: If geometry is missing (standard for SOAP), try to fetch it via WFS
                if not cadastral_data.get('geometry') and cadastral_data.get('cadastralReference'):
                    ref_cat = cadastral_data.get('cadastralReference')
                    logger.info(f"Fetching geometry for RefCat {ref_cat} via INSPIRE WFS")
                    geometry = self._fetch_geometry_from_wfs(ref_cat)
                    if geometry:
                        cadastral_data['geometry'] = geometry
                        logger.info(f"Successfully enriched RefCat {ref_cat} with geometry")
                    else:
                        logger.warning(f"Could not fetch geometry for RefCat {ref_cat}")

                logger.info(
                    f"Found cadastral reference {cadastral_data.get('cadastralReference')} "
                    f"for coordinates ({longitude}, {latitude}), municipality: {cadastral_data.get('municipality')}"
                )
            else:
                logger.warning(f"No cadastral data found for coordinates ({longitude}, {latitude})")
            
            return cadastral_data

        except Fault as e:
            logger.warning(f"SOAP Fault querying coordinates ({longitude}, {latitude}): {e}", exc_info=False)
            # Common fault codes:
            # - 11: No se encontró parcela (parcel not found)
            # - Other codes: various errors
            logger.warning(f"SOAP Fault code: {getattr(e, 'code', 'N/A')}, message: {getattr(e, 'message', str(e))}")
            return None
        except TransportError as e:
            logger.error(f"Transport error querying coordinates: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error querying coordinates ({longitude}, {latitude}): {e}", exc_info=True)
            return None

    def _fetch_geometry_from_wfs(self, ref_cat: str) -> Optional[Dict[str, Any]]:
        """
        Fetch geometry for a Cadastral Reference using the INSPIRE WFS service.
        URL: http://ovc.catastro.meh.es/INSPIRE/wfsCP.aspx
        """
        wfs_url = "http://ovc.catastro.meh.es/INSPIRE/wfsCP.aspx"
        try:
            # The REF_CAT sometimes comes with hiphens or codes that match localId
            # INSPIRE WFS expects localId or similar filter
            # Filter by CadastralParcel.inspireId.localId or similar
            
            # Simple GetFeature with stored query or filter
            # Using basic filter by REFCAT
            params = {
                'service': 'WFS',
                'version': '2.0.0',
                'request': 'GetFeature',
                'typeNames': 'CP.CadastralParcel',
                'srsName': 'EPSG:4326',
                'outputFormat': 'application/json',  # Some services support JSON
                # Filter is complex in WFS 2.0, try simple stored query if available
                # or build a filter XML
            }
            
            # Alternative: Use the "GetParcela" or similar via WFS filter
            # Let's try constructing a filter for the localId (RefCat)
            # RefCat in Spain is typically 14 or 20 chars
            
            # Constructing a standard OGC Filter
            # Reference: <Filter><PropertyIsEqualTo><PropertyName>...
            
            # Simplified approach: Try fetching by stored query if available, otherwise filter
            # Attempting simple filter on 'localId' which usually matches RefCat
            # Note: 20 digit refcat is typical structure
            
            # NOTE: The Spanish INSPIRE WFS might accept 'refcat' parameter directly in some custom profiles
            # checking docs... http://www.catastro.minhap.es/webinspire/documentos/WFS.pdf
            # It supports 'GetFeature' with 'StoreQuery_id=GetParcel' and 'refcat' param
            
            params = {
                'service': 'WFS',
                'version': '2.0.0',
                'request': 'GetFeature',
                'storedQuery_id': 'GetParcel',
                'refcat': ref_cat,
                'srsName': 'EPSG:4326',
                # 'outputFormat': 'application/json' # Try JSON, fallback to GML if needed
            }
            
            # Try JSON first
            import requests
            response = requests.get(wfs_url, params=params, timeout=10)
            
            if response.status_code == 200:
                # The service might return GML by default even if JSON requested if not supported
                # Check content type
                if 'json' in response.headers.get('Content-Type', '').lower():
                    data = response.json()
                    if 'features' in data and len(data['features']) > 0:
                        return data['features'][0].get('geometry')
                else:
                    # Parse GML/XML
                    # This is rigorous, simplified regex extraction for GML posList for Polygon
                    # <gml:posList>...</gml:posList>
                    import re
                    content = response.text
                    # Look for Polygon/Exterior/LinearRing/posList
                    # This is a very rough parser, replacing with proper library like xml.etree is better
                    # But for now, let's try to extract GML geometry and convert to GeoJSON structure
                    
                    # NOTE: Parsing GML is hard without libraries like gdal/fiona or specialized xml logic
                    # We will return None if not JSON for now to avoid complexity, 
                    # OR try a best effort extraction if it's a simple polygon
                    pass
            
            return None
            
        except Exception as e:
            logger.warning(f"Error fetching geometry from WFS for {ref_cat}: {e}")
            return None

    def _parse_soap_xml_response(self, result: Any, srs: str = "4326", longitude: float = None, latitude: float = None) -> Optional[Dict[str, Any]]:
        """
        Parse SOAP XML response (raw_response=True returns Response object or lxml.etree.Element).
        
        Args:
            result: SOAP response (lxml.etree.Element or Response object)
            srs: Spatial Reference System code
            longitude: Longitude for logging purposes
            latitude: Latitude for logging purposes
        """
        try:
            from lxml import etree
            
            result_dict = {}
            
            # With raw_response=True, result might be a requests.Response object
            # Extract the XML element from the response content
            xml_elem = None
            
            if hasattr(result, 'content'):
                # It's a requests.Response object, get the content (bytes)
                xml_elem = etree.fromstring(result.content)
                logger.debug("Extracted XML from Response.content")
            elif hasattr(result, 'text'):
                # It's a Response object with text attribute
                text = result.text
                xml_elem = etree.fromstring(text.encode('utf-8') if isinstance(text, str) else text)
                logger.debug("Extracted XML from Response.text")
            elif hasattr(result, 'find'):
                # Already an Element
                xml_elem = result
                logger.debug("Result is already an Element")
            else:
                # Unknown type, log and try to convert to string
                logger.warning(f"Unexpected result type: {type(result)}, attributes: {[a for a in dir(result) if not a.startswith('_')][:15]}")
                try:
                    # Try to get XML as string representation
                    xml_str = str(result)
                    xml_elem = etree.fromstring(xml_str.encode('utf-8'))
                except Exception as e:
                    logger.error(f"Could not parse result as XML: {e}")
                    return None
            
            if xml_elem is None:
                logger.error("Could not extract XML element from result")
                return None
            
            # xml_elem should now be an lxml.etree.Element
            # Find coordenadas/coord element
            # XML structure: <coordenadasDireccionesResponse><coordenadas><coord>...</coord></coordenadas></coordenadasDireccionesResponse>
            
            # Try different XPath patterns
            coord_elem = None
            
            # Method 1: Find coord element directly
            coord_elem = xml_elem.find('.//{http://www.catastro.meh.es/}coord')
            if coord_elem is None:
                coord_elem = xml_elem.find('.//coord')
            
            if coord_elem is None:
                # Method 2: Navigate through estructura
                coordenadas_elem = xml_elem.find('.//{http://www.catastro.meh.es/}coordenadas')
                if coordenadas_elem is None:
                    coordenadas_elem = xml_elem.find('.//coordenadas')
                if coordenadas_elem is not None:
                    coord_elem = coordenadas_elem.find('.//{http://www.catastro.meh.es/}coord')
                    if coord_elem is None:
                        coord_elem = coordenadas_elem.find('.//coord')
            
            if coord_elem is None:
                # Log the XML structure for debugging
                xml_str = etree.tostring(xml_elem, encoding='unicode', pretty_print=True)
                logger.error(f"Could not find 'coord' element in XML response. XML structure (first 2000 chars):\n{xml_str[:2000]}")
                
                # Try all possible element names
                all_tags = [elem.tag for elem in xml_elem.iter()]
                logger.warning(f"All tags in XML: {all_tags[:50]}")
                return None
            
            logger.debug(f"Found coord element: {etree.tostring(coord_elem, encoding='unicode')[:500]}")
            
            # Extract cadastral reference from pc element
            pc_elem = coord_elem.find('.//{http://www.catastro.meh.es/}pc')
            if pc_elem is None:
                pc_elem = coord_elem.find('.//pc')
            if pc_elem is None:
                pc_elem = coord_elem.find('{http://www.catastro.meh.es/}pc')
            if pc_elem is None:
                pc_elem = coord_elem.find('pc')
            
            if pc_elem is not None:
                logger.debug(f"Found pc element: {etree.tostring(pc_elem, encoding='unicode')[:300]}")
                pc_parts = []
                for pc_num in ['pc1', 'pc2', 'pc3', 'pc4', 'pc5', 'pc6', 'pc7']:
                    # Try with namespace first, then without
                    pc_val_elem = pc_elem.find(f'{{http://www.catastro.meh.es/}}{pc_num}')
                    if pc_val_elem is None:
                        pc_val_elem = pc_elem.find(pc_num)
                    if pc_val_elem is not None and pc_val_elem.text is not None:
                        pc_text = pc_val_elem.text.strip()
                        if pc_text:
                            # Apply padding
                            if pc_num == 'pc1':
                                pc_parts.append(pc_text.zfill(2))
                            elif pc_num == 'pc2':
                                pc_parts.append(pc_text.zfill(3))
                            elif pc_num == 'pc3':
                                pc_parts.append(pc_text)
                            elif pc_num == 'pc4':
                                pc_parts.append(pc_text.zfill(3))
                            elif pc_num == 'pc5':
                                pc_parts.append(pc_text.zfill(5))
                            elif pc_num == 'pc6':
                                pc_parts.append(pc_text.zfill(4))
                            elif pc_num == 'pc7':
                                pc_parts.append(pc_text)
                
                if pc_parts:
                    result_dict['cadastralReference'] = '-'.join(pc_parts)
                    logger.info(f"Extracted cadastral reference: {result_dict['cadastralReference']}")
            
            # Extract address, municipality, and province from ldt element
            # ldt structure can contain: ld (localización descriptiva) with nv (nombre vía), cm (código municipio), etc.
            ldt_elem = coord_elem.find('.//{http://www.catastro.meh.es/}ldt')
            if ldt_elem is None:
                ldt_elem = coord_elem.find('.//ldt')
            if ldt_elem is None:
                ldt_elem = coord_elem.find('{http://www.catastro.meh.es/}ldt')
            if ldt_elem is None:
                ldt_elem = coord_elem.find('ldt')
            
            if ldt_elem is not None:
                logger.info(f"Found ldt element: {etree.tostring(ldt_elem, encoding='unicode', pretty_print=True)[:1000]}")
                # Fallback: if we can't extract specific fields, use the whole text as address
                if ldt_elem.text and ldt_elem.text.strip():
                     result_dict['address'] = ldt_elem.text.strip()
                     logger.info(f"Using ldt text as address fallback: {result_dict['address']}")
            else:
                # Log the coord element structure to understand what we're getting
                logger.warning(f"ldt element not found. Coord element structure: {etree.tostring(coord_elem, encoding='unicode', pretty_print=True)[:1500]}")
                
            # Extract address from ld/nv
            if ldt_elem is not None:
                ld_elem = ldt_elem.find('{http://www.catastro.meh.es/}ld')
                if ld_elem is None:
                    ld_elem = ldt_elem.find('ld')
                if ld_elem is not None:
                    # Extract nombre vía (street name)
                    nv_elem = ld_elem.find('{http://www.catastro.meh.es/}nv')
                    if nv_elem is None:
                        nv_elem = ld_elem.find('nv')
                    if nv_elem is not None and nv_elem.text is not None:
                        # Prefer detailed address parsing if available, but append to fallback if needed
                        # Or just use this as primary
                        result_dict['address'] = nv_elem.text.strip()
                        logger.info(f"Extracted address from nv: {result_dict['address']}")
                    
                    # Extract municipality code (cm) and name (nm)
                    cm_elem = ld_elem.find('{http://www.catastro.meh.es/}cm')
                    if cm_elem is None:
                        cm_elem = ld_elem.find('cm')
                    
                    nm_elem = ld_elem.find('{http://www.catastro.meh.es/}nm')
                    if nm_elem is None:
                        nm_elem = ld_elem.find('nm')
                    
                    if nm_elem is not None and nm_elem.text is not None:
                        result_dict['municipality'] = nm_elem.text.strip()
                        logger.info(f"Extracted municipality from nm: {result_dict['municipality']}")
                
                # Also try to extract from other possible locations in ldt
                # Sometimes municipality is directly in ldt
                if 'municipality' not in result_dict:
                    nm_direct = ldt_elem.find('{http://www.catastro.meh.es/}nm')
                    if nm_direct is None:
                        nm_direct = ldt_elem.find('nm')
                    if nm_direct is not None and nm_direct.text is not None:
                        result_dict['municipality'] = nm_direct.text.strip()
                        logger.info(f"Extracted municipality directly from ldt/nm: {result_dict['municipality']}")
                
                # Extract province (provincia) - might be in ldt or elsewhere
                # Try to find provincia element
                prov_elem = ldt_elem.find('.//{http://www.catastro.meh.es/}provincia')
                if prov_elem is None:
                    prov_elem = ldt_elem.find('.//provincia')
                if prov_elem is not None and prov_elem.text is not None:
                    result_dict['province'] = prov_elem.text.strip()
                    logger.info(f"Extracted province from provincia: {result_dict['province']}")
            
            # Also try to extract municipality and province from pc (parcel code) structure
            # pc1 is province code, pc2 is municipality code
            # We can use these codes to look up names, but for now we'll try to get names from XML
            if 'municipality' not in result_dict and pc_elem is not None:
                # Try to find municipality name near pc structure
                nm_pc = coord_elem.find('.//{http://www.catastro.meh.es/}nm')
                if nm_pc is None:
                    nm_pc = coord_elem.find('.//nm')
                if nm_pc is not None and nm_pc.text is not None:
                    result_dict['municipality'] = nm_pc.text.strip()
                    logger.info(f"Extracted municipality from coord/nm: {result_dict['municipality']}")
            
            # Extract coordinates from geo element
            geo_elem = coord_elem.find('.//{http://www.catastro.meh.es/}geo')
            if geo_elem is None:
                geo_elem = coord_elem.find('.//geo')
            if geo_elem is None:
                geo_elem = coord_elem.find('{http://www.catastro.meh.es/}geo')
            if geo_elem is None:
                geo_elem = coord_elem.find('geo')
            
            if geo_elem is not None:
                xc_elem = geo_elem.find('{http://www.catastro.meh.es/}xc')
                if xc_elem is None:
                    xc_elem = geo_elem.find('xc')
                if xc_elem is None:
                    xc_elem = geo_elem.find('{http://www.catastro.meh.es/}xcen')
                if xc_elem is None:
                    xc_elem = geo_elem.find('xcen')
                
                yc_elem = geo_elem.find('{http://www.catastro.meh.es/}yc')
                if yc_elem is None:
                    yc_elem = geo_elem.find('yc')
                if yc_elem is None:
                    yc_elem = geo_elem.find('{http://www.catastro.meh.es/}ycen')
                if yc_elem is None:
                    yc_elem = geo_elem.find('ycen')
                
                if xc_elem is not None and yc_elem is not None and xc_elem.text is not None and yc_elem.text is not None:
                    try:
                        result_dict['coordinates'] = {
                            'lon': float(xc_elem.text.strip()),
                            'lat': float(yc_elem.text.strip())
                        }
                        logger.debug(f"Extracted coordinates: {result_dict['coordinates']}")
                    except (ValueError, TypeError):
                        pass
            
            # Extract municipality and province from address (only if not already extracted from XML)
            # Don't overwrite municipality/province if we already extracted them from XML (more reliable)
            if 'municipality' not in result_dict or not result_dict.get('municipality'):
                address = result_dict.get('address', '')
                municipality, province = self._extract_municipality_province(address)
                if municipality:
                    result_dict['municipality'] = municipality
                if province and ('province' not in result_dict or not result_dict.get('province')):
                    result_dict['province'] = province
            
            # Validate that we have at least cadastral reference
            if 'cadastralReference' not in result_dict or not result_dict['cadastralReference']:
                logger.warning("No cadastral reference found in response")
                # Log the coord element structure for debugging
                logger.warning(f"Coord element structure: {etree.tostring(coord_elem, encoding='unicode', pretty_print=True)[:1500]}")
                return None
            
            # Log extracted data for debugging
            logger.info(f"Extracted data - Reference: {result_dict.get('cadastralReference')}, "
                      f"Municipality: {result_dict.get('municipality')}, "
                      f"Province: {result_dict.get('province')}, "
                      f"Address: {result_dict.get('address')}")
            
            # Automatically download geometry if we have cadastral reference
            if result_dict.get('cadastralReference'):
                cadastral_ref = result_dict['cadastralReference']
                logger.info(f"Downloading geometry for cadastral reference: {cadastral_ref}")
                try:
                    # Use the srs parameter from the method signature
                    geometry = self.get_parcel_geometry(cadastral_ref, srs)
                    if geometry:
                        result_dict['geometry'] = geometry
                        coord_count = len(geometry.get('coordinates', [[[]]])[0]) if geometry.get('coordinates') else 0
                        logger.info(f"Successfully downloaded geometry with {coord_count} points for {cadastral_ref}")
                    else:
                        logger.warning(f"Could not download geometry for {cadastral_ref} - WFS and SOAP methods both returned None")
                        # Explicitly set geometry to None for consistent response
                        result_dict['geometry'] = None
                except Exception as e:
                    logger.error(f"Exception downloading geometry for {cadastral_ref}: {e}", exc_info=True)
                    # Set geometry to None on exception too
                    result_dict['geometry'] = None
            
            # Ensure geometry field exists (None if not found)
            if 'geometry' not in result_dict:
                result_dict['geometry'] = None
            
            return result_dict

        except Exception as e:
            logger.error(f"Error parsing SOAP XML response: {e}", exc_info=True)
            return None

    def _parse_soap_response(self, result: Any) -> Optional[Dict[str, Any]]:
        """
        Parse SOAP response into a dictionary.
        
        The SOAP response structure from OVCCoordenadas.asmx typically looks like:
        {
            'coordenadasDireccionesResponse': {
                'coord': {
                    'geo': {
                        'xcen': longitude,
                        'ycen': latitude,
                        'srs': 'EPSG:4326'
                    },
                    'pc': {
                        'pc1': 'province code',
                        'pc2': 'municipality code'
                    },
                    'ldt': 'full address description',
                    'refcat': 'cadastral reference'
                }
            }
        }
        
        Args:
            result: Zeep SOAP response object
            
        Returns:
            Parsed dictionary or None
        """
        try:
            result_dict = {}
            
            # Log the structure for debugging
            logger.debug(f"SOAP response type: {type(result)}")
            logger.debug(f"SOAP response attributes: {dir(result) if hasattr(result, '__dict__') else 'N/A'}")
            
            # Zeep returns structured objects, we need to navigate the response
            # Try different possible response structures
            
            # Method 1: Navigate through the response structure
            # Response structure: coordenadas.coord (coord can be a list or single object)
            coord_list = None
            if hasattr(result, 'coordenadas') and hasattr(result.coordenadas, 'coord'):
                coord_list = result.coordenadas.coord
                logger.debug("Found coord via result.coordenadas.coord")
            elif hasattr(result, 'coord'):
                coord_list = result.coord
                logger.debug("Found coord via result.coord")
            elif hasattr(result, 'coordenadasDireccionesResponse'):
                if hasattr(result.coordenadasDireccionesResponse, 'coordenadas') and hasattr(result.coordenadasDireccionesResponse.coordenadas, 'coord'):
                    coord_list = result.coordenadasDireccionesResponse.coordenadas.coord
                    logger.debug("Found coord via coordenadasDireccionesResponse.coordenadas.coord")
                elif hasattr(result.coordenadasDireccionesResponse, 'coord'):
                    coord_list = result.coordenadasDireccionesResponse.coord
                    logger.debug("Found coord via coordenadasDireccionesResponse.coord")
            else:
                # Try to use result directly
                coord_list = result
                logger.debug("Using result directly as coord")
            
            # Check if coord_list is None or empty
            if coord_list is None:
                logger.warning("Could not find 'coord' in SOAP response")
                logger.warning(f"Result structure type: {type(result)}, attributes: {[a for a in dir(result) if not a.startswith('_')][:10]}")
                return None
            
            # Handle case where coord is a list/iterable
            # Convert to list if it's iterable but not a string
            try:
                if hasattr(coord_list, '__iter__') and not isinstance(coord_list, str):
                    coord_list = list(coord_list)
                    if len(coord_list) == 0:
                        logger.warning("coord list is empty")
                        return None
                    # Use first coord if multiple exist
                    coord = coord_list[0]
                    logger.debug(f"Using first coord from list of {len(coord_list)}")
                else:
                    coord = coord_list
                    logger.debug("Using coord as single object")
            except (TypeError, AttributeError):
                coord = coord_list
                logger.debug("Using coord directly (not iterable)")
            
            logger.debug(f"Coord type: {type(coord)}, attributes: {[a for a in dir(coord) if not a.startswith('_')][:15]}")
            
            # Check if coord is an XML Element (from zeep raw response)
            # XML Elements have methods like 'find', 'iter', 'text', etc.
            if hasattr(coord, 'find') and hasattr(coord, 'iter'):
                # This is an XML Element, parse it using XML methods
                logger.debug("Coord is an XML Element, parsing using XML methods")
                
                # Extract cadastral reference from pc element
                pc_elem = coord.find('.//pc') or coord.find('pc')
                if pc_elem is not None:
                    pc_parts = []
                    for pc_num in ['pc1', 'pc2', 'pc3', 'pc4', 'pc5', 'pc6', 'pc7']:
                        pc_val = pc_elem.find(pc_num)
                        if pc_val is not None and pc_val.text:
                            pc_text = pc_val.text.strip()
                            if pc_text:
                                # Apply padding based on pc number
                                if pc_num == 'pc1':
                                    pc_parts.append(pc_text.zfill(2))
                                elif pc_num == 'pc2':
                                    pc_parts.append(pc_text.zfill(3))
                                elif pc_num == 'pc3':
                                    pc_parts.append(pc_text)
                                elif pc_num == 'pc4':
                                    pc_parts.append(pc_text.zfill(3))
                                elif pc_num == 'pc5':
                                    pc_parts.append(pc_text.zfill(5))
                                elif pc_num == 'pc6':
                                    pc_parts.append(pc_text.zfill(4))
                                elif pc_num == 'pc7':
                                    pc_parts.append(pc_text)
                    if pc_parts:
                        result_dict['cadastralReference'] = '-'.join(pc_parts)
                        logger.info(f"Extracted cadastral reference from XML pc: {result_dict['cadastralReference']}")
                
                # Extract address from ldt element
                ldt_elem = coord.find('.//ldt') or coord.find('ldt')
                if ldt_elem is not None:
                    ld_elem = ldt_elem.find('.//ld') or ldt_elem.find('ld')
                    if ld_elem is not None:
                        nv_elem = ld_elem.find('nv')
                        if nv_elem is not None and nv_elem.text:
                            result_dict['address'] = nv_elem.text.strip()
                            logger.debug(f"Extracted address from XML ldt/ld/nv: {result_dict['address']}")
                
                # Extract coordinates from geo element
                geo_elem = coord.find('.//geo') or coord.find('geo')
                if geo_elem is not None:
                    xc_elem = geo_elem.find('xc') or geo_elem.find('xcen')
                    yc_elem = geo_elem.find('yc') or geo_elem.find('ycen')
                    if xc_elem is not None and yc_elem is not None:
                        try:
                            result_dict['coordinates'] = {
                                'lon': float(xc_elem.text),
                                'lat': float(yc_elem.text)
                            }
                            logger.debug(f"Extracted coordinates from XML geo: {result_dict['coordinates']}")
                        except (ValueError, TypeError, AttributeError):
                            pass
            
            # Try Zeep structured object access
            elif hasattr(coord, 'pc'):
                pc = coord.pc
                logger.debug(f"Found pc structure: {type(pc)}, attributes: {[a for a in dir(pc) if not a.startswith('_')]}")
                pc_parts = []
                if hasattr(pc, 'pc1') and pc.pc1 is not None: 
                    pc_parts.append(str(pc.pc1).zfill(2))
                if hasattr(pc, 'pc2') and pc.pc2 is not None: 
                    pc_parts.append(str(pc.pc2).zfill(3))
                if hasattr(pc, 'pc3') and pc.pc3 is not None: 
                    pc_parts.append(str(pc.pc3))
                if hasattr(pc, 'pc4') and pc.pc4 is not None: 
                    pc_parts.append(str(pc.pc4).zfill(3))
                if hasattr(pc, 'pc5') and pc.pc5 is not None: 
                    pc_parts.append(str(pc.pc5).zfill(5))
                if hasattr(pc, 'pc6') and pc.pc6 is not None: 
                    pc_parts.append(str(pc.pc6).zfill(4))
                if hasattr(pc, 'pc7') and pc.pc7 is not None: 
                    pc_parts.append(str(pc.pc7))
                if pc_parts:
                    result_dict['cadastralReference'] = '-'.join(pc_parts)
                    logger.debug(f"Extracted cadastral reference from pc: {result_dict['cadastralReference']}")
            elif hasattr(coord, 'refcat'):
                result_dict['cadastralReference'] = str(coord.refcat)
                logger.debug(f"Extracted cadastral reference from refcat: {result_dict['cadastralReference']}")
            elif hasattr(coord, 'refCadastral'):
                result_dict['cadastralReference'] = str(coord.refCadastral)
                logger.debug(f"Extracted cadastral reference from refCadastral: {result_dict['cadastralReference']}")
            
            # If we still don't have cadastral reference, log warning
            if 'cadastralReference' not in result_dict:
                logger.warning(f"No cadastral reference found in coord. Type: {type(coord)}, Attributes: {[a for a in dir(coord) if not a.startswith('_')][:20]}")
            
            # Extract address (if not already extracted from XML)
            if 'address' not in result_dict:
                if hasattr(coord, 'ldt'):
                    ldt = coord.ldt
                    logger.debug(f"Found ldt: {type(ldt)}, attributes: {[a for a in dir(ldt) if not a.startswith('_')]}")
                    if hasattr(ldt, 'ld'):
                        ld = ldt.ld
                        logger.debug(f"Found ld: {type(ld)}, attributes: {[a for a in dir(ld) if not a.startswith('_')]}")
                        if hasattr(ld, 'nv'):
                            result_dict['address'] = str(ld.nv)
                            logger.debug(f"Extracted address from ldt.ld.nv: {result_dict['address']}")
                        elif hasattr(ld, 'cm') and hasattr(ld, 'nv'):
                            # Sometimes includes municipality code
                            result_dict['address'] = str(ld.nv)
                        else:
                            result_dict['address'] = str(ld) if ld else ''
                    else:
                        result_dict['address'] = str(ldt) if ldt else ''
                elif hasattr(coord, 'address'):
                    result_dict['address'] = str(coord.address)
                else:
                    result_dict['address'] = ''
            
            # Extract municipality and province from address (only if not already extracted from XML)
            # Don't overwrite municipality/province if we already extracted them from XML (more reliable)
            if 'municipality' not in result_dict or not result_dict.get('municipality'):
                address = result_dict.get('address', '')
                municipality, province = self._extract_municipality_province(address)
                if municipality:
                    result_dict['municipality'] = municipality
                if province and ('province' not in result_dict or not result_dict.get('province')):
                    result_dict['province'] = province
            
            # Try to extract from pc (province/municipality codes) if available
            if hasattr(coord, 'pc'):
                pc = coord.pc
                # pc.pc1 is province code, pc.pc2 is municipality code
                # We could use these to lookup names, but for now we use address parsing
                pass
            
            # Extract coordinates (if not already extracted from XML)
            if 'coordinates' not in result_dict:
                if hasattr(coord, 'geo'):
                    geo = coord.geo
                    if hasattr(geo, 'xc') and hasattr(geo, 'yc'):
                        try:
                            result_dict['coordinates'] = {
                                'lon': float(geo.xc),
                                'lat': float(geo.yc)
                            }
                        except (ValueError, TypeError):
                            pass
                    elif hasattr(geo, 'xcen') and hasattr(geo, 'ycen'):
                        try:
                            result_dict['coordinates'] = {
                                'lon': float(geo.xcen),
                                'lat': float(geo.ycen)
                            }
                        except (ValueError, TypeError):
                            pass
            
            # Validate that we have at least cadastral reference
            if 'cadastralReference' not in result_dict or not result_dict['cadastralReference']:
                logger.warning("No cadastral reference found in response")
                return None
            
            return result_dict

        except Exception as e:
            logger.error(f"Error parsing SOAP response: {e}", exc_info=True)
            logger.debug(f"Response object type: {type(result)}, attributes: {dir(result) if hasattr(result, '__dict__') else 'N/A'}")
            return None

    def _extract_municipality_province(self, address: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Extract municipality and province from address string using regex.
        
        The address format is typically:
        "DIRECCION MUNICIPIO (PROVINCIA)" or similar
        """
        if not address:
            return None, None

        try:
            # Pattern: matches "Municipality (Province)" at the end of string
            # Handles "POZUELO DE ALARCON (MADRID)"
            match = re.search(r'([^(]+)\s*\(([^)]+)\)$', address.strip())
            if match:
                return match.group(1).strip(), match.group(2).strip()
            
            # Alternative pattern: "C/ ALCALA, 1 - 28014 MADRID" (No province code, but maybe last word is province)
            # This is hard to guess accurately without a list of provinces.
            # Stick to standard format or leave None.
            return None, None

        except Exception as e:
            logger.warning(f"Error extracting municipality/province from address: {e}")
            return None, None


# Placeholder classes for future phases
class NavarraCatastroClient:
    """
    Client for Navarra Cadastre (WFS - IDENA).
    Uses WFS 2.0.0 GetFeature requests with dynamic feature type discovery.
    """
    
    WFS_BASE_URL = "https://idena.navarra.es/ogc/wfs"  # Updated to correct IDENA WFS URL
    FEATURE_TYPE = "CP:CadastralParcel"  # INSPIRE fallback
    
    # Hardcoded fallback feature types (used if GetCapabilities fails)
    FALLBACK_FEATURE_TYPES = [
        'CATAST_Pol_ParcelaUrba',  # Urban parcels
        'CATAST_Pol_ParcelaRusti',  # Rural parcels
        'CATAST_Pol_ParcelaMixta',  # Mixed parcels
        'CP:CadastralParcel'  # INSPIRE fallback
    ]
    
    def __init__(self):
        """Initialize the WFS client."""
        self.session = requests.Session()
        self._discovered_types = None
        logger.info("Navarra Catastro WFS client initialized")
    
    def _get_feature_types(self) -> List[str]:
        """
        Get feature types to try, using discovery if available.
        Returns discovered types (cached) or fallback hardcoded types.
        """
        if self._discovered_types is not None:
            return self._discovered_types
        
        # Try to discover feature types dynamically
        discovered = WFSCapabilitiesDiscovery.discover_feature_types(
            self.WFS_BASE_URL,
            fallback_types=self.FALLBACK_FEATURE_TYPES
        )
        
        # Filter to cadastral types
        cadastral_types = WFSCapabilitiesDiscovery.filter_cadastral_types(discovered)
        
        if cadastral_types and cadastral_types != self.FALLBACK_FEATURE_TYPES:
            logger.info(f"Using discovered Navarra feature types: {cadastral_types}")
            self._discovered_types = cadastral_types
        else:
            logger.info("Using fallback Navarra feature types")
            self._discovered_types = self.FALLBACK_FEATURE_TYPES
        
        return self._discovered_types
    
    def query_by_coordinates(
        self,
        longitude: float,
        latitude: float,
        srs: str = "4326"
    ) -> Optional[Dict[str, Any]]:
        """
        Query cadastral parcel by coordinates using WFS GetFeature with BBOX.
        
        Args:
            longitude: Longitude in decimal degrees
            latitude: Latitude in decimal degrees
            srs: Spatial Reference System (default: "4326")
            
        Returns:
            Dictionary with cadastral data including geometry, or None
        """
        try:
            # Convert to EPSG:25830 (UTM 30N) for Navarra if needed
            # For now, use WGS84 and let the service handle it
            srs_name = f"EPSG:{srs}"
            
            # Create a larger bounding box around the point (about 50 meters)
            # IDENA may need a larger buffer to find parcels
            buffer = 0.0005  # ~50 meters in degrees
            bbox = f"{longitude - buffer},{latitude - buffer},{longitude + buffer},{latitude + buffer},{srs_name}"
            
            # Get feature types (dynamically discovered or fallback)
            feature_types = self._get_feature_types()

            
            # Try each feature type until we find a parcel
            for feature_type in feature_types:
                try:
                    params = {
                        'service': 'WFS',
                        'version': '2.0.0',
                        'request': 'GetFeature',
                        'typeNames': feature_type,
                        'srsName': srs_name,
                        'bbox': bbox,
                        'outputFormat': 'application/json'  # Request GeoJSON
                    }
                    
                    logger.info(f"Trying Navarra WFS with feature type: {feature_type}, bbox={bbox}")
                    response = self.session.get(self.WFS_BASE_URL, params=params, timeout=15)
                    
                    logger.debug(f"Navarra WFS response status: {response.status_code}")
                    
                    # Log raw response for debugging
                    if response.status_code == 200:
                        try:
                            response_text = response.text if hasattr(response, 'text') else response.content.decode('utf-8', errors='ignore')
                            logger.info(f"Navarra WFS raw response (first 1000 chars):\n{response_text[:1000]}")
                        except Exception as e:
                            logger.warning(f"Could not log Navarra WFS raw response: {e}")
                    
                    if response.status_code == 200:
                        try:
                            data = response.json()
                            if 'features' in data and len(data['features']) > 0:
                                logger.info(f"Found {len(data['features'])} features in Navarra WFS with type {feature_type}")
                                # Found features, break and process
                                break
                            else:
                                logger.debug(f"No features in Navarra WFS response for type {feature_type}")
                        except ValueError as e:
                            # Not JSON, might be XML or error
                            logger.warning(f"Navarra WFS response is not JSON: {e}")
                            logger.debug(f"Response content (first 500 chars): {response.text[:500] if hasattr(response, 'text') else response.content[:500]}")
                    else:
                        logger.debug(f"Navarra WFS returned status {response.status_code}")
                except requests.exceptions.RequestException as e:
                    logger.debug(f"Request error with Navarra WFS, type {feature_type}: {e}")
                    continue
                except Exception as e:
                    logger.debug(f"Feature type {feature_type} failed: {e}")
                    continue
            else:
                # No features found with any type
                logger.debug("No features found in Navarra WFS response with any feature type")
                return None
            
            # Select the most relevant feature for the clicked point instead of relying on array order.
            feature = _select_best_feature(data['features'], longitude, latitude)
            if not feature:
                logger.warning("Navarra WFS returned features but no selectable feature was found")
                return None
            properties = feature.get('properties', {})
            geometry = feature.get('geometry')
            
            # Log feature structure for debugging (ALWAYS log, even if geometry is None)
            logger.info(f"Navarra WFS feature keys: {list(feature.keys())}")
            logger.info(f"Navarra WFS properties keys: {list(properties.keys())}")
            logger.info(f"Navarra WFS geometry present: {geometry is not None}, type: {type(geometry)}")
            if geometry:
                # Normalize MultiPolygon to Polygon for compatibility with frontend/platform
                if geometry.get('type') == 'MultiPolygon' and geometry.get('coordinates'):
                    logger.info("Normalizing Navarra MultiPolygon to Polygon (taking first polygon)")
                    # Take the first polygon from the multipolygon
                    # MultiPolygon coords: [[[[x,y],...]], ...]
                    # Polygon coords: [[[x,y],...]]
                    geometry['type'] = 'Polygon'
                    geometry['coordinates'] = geometry['coordinates'][0]

                logger.info(f"Navarra WFS geometry keys: {list(geometry.keys()) if isinstance(geometry, dict) else 'N/A'}")
                logger.info(f"Navarra WFS geometry type: {geometry.get('type') if isinstance(geometry, dict) else 'N/A'}")
            else:
                logger.warning(f"Navarra WFS: geometry is None or missing in feature. Feature structure: {json.dumps({k: str(type(v)) for k, v in feature.items()}, indent=2)[:500]}")
            
            # Extract cadastral reference from properties
            # IDENA uses different property names, try common ones
            cadastral_ref = (
                properties.get('localId') or
                properties.get('inspireId') or
                properties.get('cadastralReference') or
                properties.get('id') or
                properties.get('REFCAT') or  # IDENA specific
                feature.get('id', '').replace('ES.RRTN.CP.', '')
            )
            
            if not cadastral_ref:
                logger.warning("Could not extract cadastral reference from Navarra WFS response")
                logger.warning(f"Available properties: {list(properties.keys())}")
                logger.warning(f"Feature ID: {feature.get('id', 'N/A')}")
                return None
            
            # Try to extract municipality from various possible property names
            municipality = (
                properties.get('municipality') or
                properties.get('municipio') or
                properties.get('municipalityName') or
                properties.get('nombreMunicipio') or
                properties.get('MUNICIPIO') or
                properties.get('NOMBRE_MUNICIPIO') or
                properties.get('MUNICIPIO_NOMBRE') or
                properties.get('NOMBRE') or
                None
            )
            
            # Try to extract address
            address = (
                properties.get('address') or
                properties.get('direccion') or
                properties.get('addressText') or
                properties.get('DIRECCION') or
                None
            )
            
            # Validate geometry if present
            if geometry:
                # Ensure geometry is valid GeoJSON
                if isinstance(geometry, dict) and geometry.get('type') in ('Polygon', 'MultiPolygon'):
                    logger.info(f"Navarra parcel {cadastral_ref} has valid GeoJSON geometry: {geometry.get('type')}")
                else:
                    logger.warning(f"Navarra parcel {cadastral_ref} has geometry but invalid format: {type(geometry)}")
                    geometry = None  # Set to None if invalid
            else:
                logger.warning(f"Navarra parcel {cadastral_ref} found but geometry is None or missing")
            
            result = {
                'cadastralReference': str(cadastral_ref),
                'municipality': municipality,
                'province': 'Navarra',
                'address': address,
                'coordinates': {'lon': longitude, 'lat': latitude},
                'geometry': geometry,  # GeoJSON geometry (may be None)
                'region': 'navarra'
            }
            
            logger.info(f"Found Navarra parcel: {cadastral_ref}, municipality: {municipality}, geometry: {'present' if geometry else 'missing'}")
            return result
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Error querying Navarra WFS: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error querying Navarra cadastre: {e}", exc_info=True)
            return None

    def _parse_wfs_xml_response(self, content: bytes) -> Optional[Dict[str, Any]]:
        """
        Parse WFS GML/XML response when JSON is not available (Fallback for Navarra).
        """
        try:
            from lxml import etree
            root = etree.fromstring(content)
            
            # Common namespaces
            ns = {
                'wfs': 'http://www.opengis.net/wfs/2.0',
                'gml': 'http://www.opengis.net/gml/3.2',
                'cp': 'http://inspire.ec.europa.eu/schemas/cp/4.0',
                'ms': 'http://mapserver.gis.umn.edu/mapserver' # IDENA specific often uses 'ms'
            }
            
            # Find feature members
            # Try standard WFS/GML first
            features = []
            features.extend(root.xpath('//wfs:member', namespaces=ns))
            features.extend(root.xpath('//gml:featureMember', namespaces=ns))
            features.extend(root.xpath('//featureMember')) # No namespace
            
            if not features:
                # Try finding any element that looks like a parcel
                features = root.xpath('//*[local-name()="CadastralParcel"]')
                if not features:
                    features = root.xpath('//*[local-name()="CATAST_Pol_ParcelaUrba"]')
                if not features:
                    features = root.xpath('//*[local-name()="CATAST_Pol_ParcelaRusti"]')
            
            if not features:
                return None
            
            feature = features[0]
            
            # If feature is a wrapper (member), extract the actual content
            if feature.tag.endswith('member') or feature.tag.endswith('featureMember'):
                if len(feature) > 0:
                    feature = feature[0]
            
            # Extract ID/Reference
            ref_cat = None
            # IDENA often puts it in 'localId' or 'REFCAT' or 'id' property
            for path in ['.//*[local-name()="localId"]', './/*[local-name()="REFCAT"]', './/*[local-name()="id"]']:
                nodes = feature.xpath(path)
                if nodes and nodes[0].text:
                    ref_cat = nodes[0].text
                    break
            
            if not ref_cat:
                # Try gml:id
                gml_id = feature.get(f"{{{ns['gml']}}}id")
                if gml_id:
                    ref_cat = gml_id.split('.')[-1] if '.' in gml_id else gml_id
            
            if not ref_cat:
                return None

            # Extract Metadata
            municipality = None
            for tag in ['municipio', 'municipality', 'nombreMunicipio', 'MUNICIPIO']:
                nodes = feature.xpath(f'.//*[local-name()="{tag}"]')
                if nodes and nodes[0].text:
                    municipality = nodes[0].text
                    break
            
            address = None
            for tag in ['direccion', 'address', 'DIRECCION', 'clase']:
                nodes = feature.xpath(f'.//*[local-name()="{tag}"]')
                if nodes and nodes[0].text:
                    address = nodes[0].text
                    break
            
            # Extract Geometry (Basic GML Polygon support)
            geometry = None
            # Look for posList in LinearRing
            pos_lists = feature.xpath('.//*[local-name()="items"] | .//*[local-name()="posList"]')
            
            if pos_lists and pos_lists[0].text:
                coords_text = pos_lists[0].text.strip().split()
                coords = []
                
                # Simple parsing, assuming standard GML 2D
                if len(coords_text) >= 2:
                    try:
                        # Heuristic for Lat/Lon swap (same as Euskadi)
                        v1 = float(coords_text[0])
                        v2 = float(coords_text[1])
                        swap_xy = False
                        # Navarra is usually ETRS89 (approx Lat 42, Lon -1.6)
                        # If v1 is > 40, likely Lat. If v1 is small neg, likely Lon.
                        if (40 <= v1 <= 44) and (-3 <= v2 <= 0):
                            swap_xy = True # Input is [Lat, Lon], we want [Lon, Lat]
                        
                        for i in range(0, len(coords_text), 2):
                            if i+1 < len(coords_text):
                                val1 = float(coords_text[i])
                                val2 = float(coords_text[i+1])
                                if swap_xy:
                                    coords.append([val2, val1])
                                else:
                                    coords.append([val1, val2])
                    except ValueError:
                        pass
                
                if coords:
                    geometry = {
                        "type": "Polygon",
                        "coordinates": [coords]
                    }

            return {
                "type": "Feature",
                "geometry": geometry,
                "properties": {
                    "cadastralReference": ref_cat,
                    "municipality": municipality,
                    "province": "Navarra",
                    "address": address
                }
            }

        except Exception as e:
            logger.error(f"Error parsing Navarra WFS XML: {e}")
            return None


class EuskadiCatastroClient:
    """
    Client for Euskadi Cadastre (WFS - GeoEuskadi).
    Uses WFS 2.0.0 GetFeature requests with dynamic feature type discovery.
    """
    
    # Try different possible WFS URLs for Euskadi
    WFS_BASE_URLS = [
        "https://b5m.gipuzkoa.eus/inspire/wfs/gipuzkoa_wfs_cp",  # Gipuzkoa (Correct INSPIRE endpoint)
        "https://geo.araba.eus/WFS_INSPIRE_CP",           # Araba
        "https://geo.bizkaia.eus/arcgisserverinspire/services/LurraldeAntolamendua_PlanificacionTerritorial/Katastro_Catastro_WFS/MapServer/WFSServer", # Bizkaia (Verified 2026)
        "https://www.geo.euskadi.eus/wfs-katastro",       # General fallback
        "https://geo.euskadi.eus/wfs-katastro",
    ]
    FEATURE_TYPE = "CP:CadastralParcel"  # INSPIRE type
    
    # Hardcoded fallback feature types (used if GetCapabilities fails)
    # Added Katastro_Catastro_WFS:Parcelas for Bizkaia
    FALLBACK_FEATURE_TYPES = [
        "CP:CadastralParcel",
        "Katastro_Catastro_WFS:Parcelas",
        "katastro:parcela",
        "parcela_catastral",
        "CP.CadastralParcel",
        "CadastralParcel",
    ]
    
    def __init__(self):
        """Initialize the WFS client."""
        self.session = requests.Session()
        self._discovered_types = {}  # Per-URL cache
        logger.info("Euskadi Catastro WFS client initialized")
    
    def _get_feature_types_for_url(self, wfs_url: str) -> List[str]:
        """
        Get feature types to try for a specific URL, using discovery if available.
        Returns discovered types (cached) or fallback hardcoded types.
        """
        if wfs_url in self._discovered_types:
            return self._discovered_types[wfs_url]
        
        # Try to discover feature types dynamically
        discovered = WFSCapabilitiesDiscovery.discover_feature_types(
            wfs_url,
            fallback_types=self.FALLBACK_FEATURE_TYPES
        )
        
        # Filter to cadastral types
        cadastral_types = WFSCapabilitiesDiscovery.filter_cadastral_types(discovered)
        
        # Explicit check: If accessing Bizkaia WFS, ensure Katastro_Catastro_WFS:Parcelas is included
        # even if filter removed it (because it contains 'Parcelas' which might be filtered differently)
        if "bizkaia" in wfs_url.lower() and "Katastro_Catastro_WFS:Parcelas" not in cadastral_types:
             if "Katastro_Catastro_WFS:Parcelas" in discovered:
                 cadastral_types.insert(0, "Katastro_Catastro_WFS:Parcelas")
             else:
                 # Add it anyway as fallback for this URL
                 cadastral_types.append("Katastro_Catastro_WFS:Parcelas")

        if cadastral_types and cadastral_types != self.FALLBACK_FEATURE_TYPES:
            logger.info(f"Using discovered Euskadi feature types for {wfs_url}: {cadastral_types}")
            self._discovered_types[wfs_url] = cadastral_types
        else:
            logger.info(f"Using fallback Euskadi feature types for {wfs_url}")
            self._discovered_types[wfs_url] = self.FALLBACK_FEATURE_TYPES
        
        return self._discovered_types[wfs_url]

    
    def query_by_coordinates(
        self,
        longitude: float,
        latitude: float,
        srs: str = "4326"
    ) -> Optional[Dict[str, Any]]:
        """
        Query cadastral parcel by coordinates using WFS GetFeature with BBOX.
        
        Args:
            longitude: Longitude in decimal degrees
            latitude: Latitude in decimal degrees
            srs: Spatial Reference System (default: "4326")
            
        Returns:
            Dictionary with cadastral data including geometry, or None
        """
        try:
            # Convert to EPSG:25830 (UTM 30N) for Euskadi
            srs_name = f"EPSG:{srs}"
            
            # Create a larger bounding box around the point (about 50 meters)
            buffer = 0.0005  # ~50 meters in degrees
            # Create list of BBOX candidates to try (Standard Lon/Lat, and Swapped Lat/Lon)
            # WFS 2.0 with EPSG:4326 often expects Lat/Lon order
            bbox_candidates = [
                # Standard Lon/Lat (minx, miny, maxx, maxy) e.g. -2.2, 43.2, ...
                (f"{longitude - buffer},{latitude - buffer},{longitude + buffer},{latitude + buffer},{srs_name}", "Standard (Lon,Lat)"),
                 # Swapped Lat/Lon (miny, minx, maxy, maxx) e.g. 43.2, -2.2, ... (This is standard for urn:ogc:def:crs:EPSG::4326)
                (f"{latitude - buffer},{longitude - buffer},{latitude + buffer},{longitude + buffer},{srs_name}", "Swapped (Lat,Lon)")
            ]
            
            # Try each WFS URL and feature type combination
            data = None
            found_features = False
            
            for wfs_url in self.WFS_BASE_URLS:
                if found_features: break
                
                # Get feature types for this URL (dynamically discovered or fallback)
                feature_types = self._get_feature_types_for_url(wfs_url)
                
                for feature_type in feature_types:
                    if found_features: break
                    
                    # Try both BBOX formats
                    for bbox, bbox_desc in bbox_candidates:
                        # Try different WFS versions
                        # For Bizkaia, 2.0.0 is verified. For others, 2.0.0 or 1.1.0.
                        versions_to_try = ['2.0.0', '1.1.0'] if "bizkaia" in wfs_url else ['2.0.0', '1.1.0', '1.0.0']
                        
                        for wfs_version in versions_to_try: 
                            try:
                                params = {
                                    'service': 'WFS',
                                    'version': wfs_version,
                                    'request': 'GetFeature',
                                    'typeNames': feature_type, # WFS 2.0
                                    'typeName': feature_type,  # WFS 1.1/1.0
                                    'srsName': srs_name,
                                    'bbox': bbox,
                                    'outputFormat': 'application/json'
                                }
                                
                                # Adjust params based on version
                                if wfs_version != '2.0.0':
                                    if 'typeNames' in params: del params['typeNames']
                                else:
                                    if 'typeName' in params: del params['typeName']

                                logger.info(f"Trying Euskadi WFS ({bbox_desc}) v{wfs_version}: URL={wfs_url}, type={feature_type}")
                                # Use verify=False for Bizkaia to avoid SSL errors with intermediate certs
                                verify_ssl = "bizkaia" not in wfs_url
                                response = self.session.get(wfs_url, params=params, timeout=15, verify=verify_ssl)
                                
                                # Log response status for debugging
                                logger.debug(f"Euskadi WFS v{wfs_version} response status: {response.status_code}")
                                
                                if response.status_code == 200:
                                    try:
                                        # Check content type for JSON
                                        is_json = 'json' in response.headers.get('Content-Type', '').lower() or response.text.strip().startswith('{')
                                        
                                        if is_json:
                                            data = response.json()
                                            if 'features' in data and len(data['features']) > 0:
                                                logger.info(f"Found {len(data['features'])} features in Euskadi WFS with URL={wfs_url}")
                                                found_features = True
                                                break
                                        else:
                                             # Check for ExceptionReport disguised as 200 OK (common in ArcGIS WFS)
                                             if b"ExceptionReport" in response.content and b"outputFormat" in response.content:
                                                 logger.warning(f"Server returned 200 OK but with ExceptionReport for outputFormat at {wfs_url}. Retrying without outputFormat...")
                                                 if 'outputFormat' in params:
                                                     del params['outputFormat']
                                                 response = self.session.get(wfs_url, params=params, timeout=15, verify=verify_ssl)
                                                 if response.status_code == 200:
                                                     feature = self._parse_wfs_xml_response(response.content)
                                                     if feature:
                                                         data = {"type": "FeatureCollection", "features": [feature]}
                                                         found_features = True
                                                         break
                                             
                                             # Normal XML Parsing
                                             if not found_features:
                                                 logger.info(f"Euskadi WFS response is not JSON, attempting XML parsing")
                                                 feature = self._parse_wfs_xml_response(response.content)
                                                 if feature:
                                                     data = {"type": "FeatureCollection", "features": [feature]}
                                                     logger.info(f"Successfully parsed XML feature from {wfs_url}")
                                                     found_features = True
                                                     break

                                    except ValueError:
                                        # Fallback to XML
                                        logger.warning(f"Euskadi WFS response JSON parse error from {wfs_url}, trying XML")
                                        feature = self._parse_wfs_xml_response(response.content)
                                        if feature:
                                            data = {"type": "FeatureCollection", "features": [feature]}
                                            found_features = True
                                            break

                                elif response.status_code == 400 and 'outputFormat' in response.text:
                                    # Fallback: Retry without outputFormat=json if server rejects it
                                    logger.warning(f"Server rejected outputFormat=json at {wfs_url}, retrying with default (XML)...")
                                    if 'outputFormat' in params:
                                        del params['outputFormat']
                                    response = self.session.get(wfs_url, params=params, timeout=15, verify=verify_ssl)
                                    if response.status_code == 200:
                                         feature = self._parse_wfs_xml_response(response.content)
                                         if feature:
                                             data = {"type": "FeatureCollection", "features": [feature]}
                                             found_features = True
                                             break
                                else:
                                    logger.debug(f"Euskadi WFS returned status {response.status_code} from {wfs_url}")
                            except requests.exceptions.RequestException as e:
                                logger.debug(f"Request error with {wfs_url}, type {feature_type}: {e}")
                                continue
                            except Exception as e:
                                logger.debug(f"Error with {wfs_url}, type {feature_type}: {e}")
                                continue
                
                if found_features: break
            
            if not data or 'features' not in data or len(data['features']) == 0:
                logger.warning(f"No features found in Euskadi WFS for coordinates ({longitude}, {latitude}) after trying all URLs, feature types and BBOX formats")
                return None
            
            # Select the most relevant feature for the clicked point instead of relying on array order.
            feature = _select_best_feature(data['features'], longitude, latitude)
            if not feature:
                logger.warning("Euskadi WFS returned features but no selectable feature was found")
                return None
            properties = feature.get('properties', {})
            geometry = feature.get('geometry')
            
            # Extract Cadastral Reference with robust fallback
            cadastral_ref = (
                properties.get('cadastralReference') or # From our XML parser
                properties.get('localId') or
                properties.get('inspireId') or
                properties.get('id') or
                properties.get('REFCAT') or
                properties.get('nationalCadastralReference') or # INSPIRE standard
                feature.get('id', '')
            )
            
            if not cadastral_ref:
                logger.warning("Could not extract cadastral reference from Euskadi WFS response")
                return None
            
            # Extract Metadata
            municipality = (
                properties.get('municipality') or
                properties.get('municipio') or
                properties.get('nombreMunicipio') or # Navarra/Euskadi convention
                properties.get('MUNICIPIO') or
                properties.get('administrativeUnit') or # INSPIRE
                None
            )
            
            # Default Province
            province = properties.get('province') or 'País Vasco'
            if "bizkaia" in str(wfs_url).lower() or "48" in str(cadastral_ref)[:2]:
                province = "Bizkaia"
            elif "gipuzkoa" in str(wfs_url).lower() or "20" in str(cadastral_ref)[:2]: # 20 is SS, but province code is 20? No 20 is Gipuzkoa code.
                province = "Gipuzkoa"
            elif "araba" in str(wfs_url).lower():
                province = "Araba/Álava"
            
            # Try to extract address
            address = (
                properties.get('address') or
                properties.get('direccion') or
                properties.get('addressText') or
                properties.get('DIRECCION') or
                None
            )
                
            result = {
                'cadastralReference': str(cadastral_ref),
                'municipality': municipality,
                'province': province,
                'address': address,
                'coordinates': {'lon': longitude, 'lat': latitude},
                'geometry': geometry,
                'region': 'euskadi'
            }
            
            logger.debug(f"Euskadi parcel properties: {list(properties.keys())}")
            
            logger.info(f"Found Euskadi parcel: {cadastral_ref}")
            return result
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Error querying Euskadi WFS: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error querying Euskadi cadastre: {e}", exc_info=True)
            return None

    def _parse_wfs_xml_response(self, content: bytes) -> Optional[Dict[str, Any]]:
        """
        Parse WFS GML/XML response when JSON is not available.
        Robust implementation using lxml and handling various GML structures (Gipuzkoa, Bizkaia, etc.).
        """
        try:
            from lxml import etree
            root = etree.fromstring(content)
            
            # Common namespaces + Bizkaia specific
            ns = {
                'wfs': 'http://www.opengis.net/wfs/2.0',
                'gml': 'http://www.opengis.net/gml/3.2',
                'cp': 'http://inspire.ec.europa.eu/schemas/cp/4.0',
                'k': 'http://www.euskadi.eus/katastro',
                'bizkaia': 'https://p5mcargisb2.bizkaiko.aldundia/arcgisserverinspire/admin/services/Katastro_Catastro_WFS/MapServer/WFSServer',
                'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
            }
            
            # 1. Find the Parcel Element
            # --------------------------
            feature = None
            # Standard INSPIRE
            nodes = root.xpath('//cp:CadastralParcel', namespaces=ns)
            if not nodes:
                 # Bizkaia custom
                 nodes = root.xpath('//*[local-name()="Parcelas"]', namespaces=ns)
            if not nodes:
                 # Generic fallback
                 nodes = root.xpath('//*[contains(local-name(), "Parcel") or contains(local-name(), "finca")]')
            
            if not nodes:
                logger.warning("No parcel element found in WFS XML")
                logger.debug(f"XML Content: {content[:1000]}") # Log first 1000 chars
                return None
            
            parcel = nodes[0]
            logger.info(f"Parsing XML feature: {parcel.tag}")
            
            # 2. Extract Data (Properties)
            # ----------------------------
            properties = {}
            
            # Reference
            ref_cat = None
            # Check for generic property tags first
            for tag in ['nationalCadastralReference', 'REFCAT', 'CODCATAST', 'refcat', 'reference', 'label']:
                 vals = parcel.xpath(f'.//*[local-name()="{tag}"]')
                 if vals and vals[0].text:
                     ref_cat = vals[0].text.strip()
                     break
            
            # If not found, check localId
            if not ref_cat:
                vals = parcel.xpath('.//*[local-name()="localId"]')
                if vals and vals[0].text:
                    ref_cat = vals[0].text.strip()
            
            # If not found, check Bizkaia specific fields (Codigo_Mun, Codigo_Pol, Codigo_Par)
            if not ref_cat:
                mun = parcel.xpath('.//*[local-name()="Codigo_Mun"]')
                pol = parcel.xpath('.//*[local-name()="Codigo_Pol"]')
                par = parcel.xpath('.//*[local-name()="Codigo_Par"]')
                
                if mun and pol and par:
                    # Construct synthetic reference: BIZ-{Mun}-{Pol}-{Par}
                    # Or try to match standard format: 48 + Mun(3) + A + Pol(3) + Par(5)
                    m_val = mun[0].text.strip().zfill(3)
                    p_val = pol[0].text.strip().zfill(3)
                    pa_val = par[0].text.strip().zfill(5)
                    # Check province context if possible, but default to 48 (Bizkaia)
                    ref_cat = f"48{m_val}A{p_val}{pa_val}" # Approximation of rural ref
                    logger.info(f"Constructed Bizkaia RefCat: {ref_cat}")
            
            if not ref_cat:
                 # Last resort: gml:id
                 gml_id = parcel.get(f"{{{ns['gml']}}}id") or parcel.get('id')
                 if gml_id:
                     ref_cat = gml_id.split('.')[-1]
            
            if ref_cat:
                properties['cadastralReference'] = ref_cat
            
            # Address/Municipality
            for tag in ['municipality', 'municipio', 'nombreMunicipio', 'relatedAdministrativeUnit', 'administrativeUnit']:
                 vals = parcel.xpath(f'.//*[local-name()="{tag}"]')
                 if vals and vals[0].text:
                     properties['municipality'] = vals[0].text.strip()
                     break
            
            # Try to find address
            for tag in ['address', 'direccion', 'DIRECCION', 'domicilio']:
                 vals = parcel.xpath(f'.//*[local-name()="{tag}"]')
                 if vals and vals[0].text:
                     properties['address'] = vals[0].text.strip()
                     break

            # 3. Extract Geometry
            # -------------------
            geometry = None
            # Look for posList in any LinearRing or similar
            pos_lists = parcel.xpath('.//*[local-name()="posList"]')
            
            if pos_lists and pos_lists[0].text:
                coords_text = pos_lists[0].text.strip().split()
                coords = []
                
                if len(coords_text) >= 2:
                    try:
                        # Determine SRS order (Lat/Lon vs Lon/Lat)
                        # Bizkaia WFS 2.0 returns Lat/Lon (43.x, -2.x) in the test output
                        # We need [Lon, Lat] for GeoJSON
                        v1 = float(coords_text[0])
                        v2 = float(coords_text[1])
                        
                        swap = False
                        # If v1 is > 40 (Lat), and v2 is negative (Lon) [-10 to 5], we have Lat,Lon
                        if (35 <= v1 <= 45) and (-10 <= v2 <= 5):
                            swap = True
                        
                        for i in range(0, len(coords_text), 2):
                             if i+1 < len(coords_text):
                                 val1 = float(coords_text[i])
                                 val2 = float(coords_text[i+1])
                                 if swap:
                                     coords.append([val2, val1]) # Lon, Lat
                                 else:
                                     coords.append([val1, val2])
                    except ValueError:
                        pass
                
                if coords:
                    # Close polygon
                    if coords[0] != coords[-1]:
                        coords.append(coords[0])
                    
                    geometry = {
                        "type": "Polygon",
                        "coordinates": [coords]
                    }
            
            return {
                "type": "Feature",
                "properties": properties,
                "geometry": geometry
            }

        except Exception as e:
            logger.error(f"Error parsing WFS XML: {e}", exc_info=True)
            return None
