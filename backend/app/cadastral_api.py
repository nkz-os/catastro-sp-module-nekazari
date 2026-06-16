#!/usr/bin/env python3
# =============================================================================
# Cadastral Parcels API
# =============================================================================
# Backend API for managing cadastral parcels and polygon selection

import os
import sys
import logging
from flask import Flask, request, jsonify, g, Blueprint
from flask_cors import CORS
from typing import Dict, Any, List, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
import json
from datetime import datetime
import requests

# Add parent directories to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'common'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'task-queue'))
# Also add current directory and common/task-queue if running in Docker
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Use simple authentication middleware (trusts API Gateway validation)
from auth_middleware import require_auth, get_current_user, get_current_tenant

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
_cors_origins = [o.strip() for o in os.getenv('CORS_ORIGINS', 'http://localhost:3000,http://localhost:5173').split(',') if o.strip()]
CORS(app, origins=_cors_origins, supports_credentials=True)

# Configuration
POSTGRES_URL = os.getenv('POSTGRES_URL', 'postgresql://postgres:postgres@postgresql-service:5432/nekazari')
# Entity Manager URL (for NDVI job creation)
ENTITY_MANAGER_URL = os.getenv('ENTITY_MANAGER_URL', 'http://entity-manager-service:5000')

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'service': 'cadastral-api'}), 200

# Create Blueprint for API routes with prefix
api_bp = Blueprint('api', __name__, url_prefix='/api/cadastral-api')

# Import cadastral clients and region router
try:
    from region_router import get_region
    from catastro_clients import (
        SpanishStateCatastroClient,
        NavarraCatastroClient,
        EuskadiCatastroClient
    )
except ImportError:
    logger.error("Failed to import region_router or catastro_clients")
    def get_region(lat, lon): return 'spain'  # Fallback
    SpanishStateCatastroClient = None
    NavarraCatastroClient = None
    EuskadiCatastroClient = None

# Import cache service for Redis caching
try:
    from cache_service import get_cache
    _cache = get_cache()
    logger.info(f"Cache service initialized: available={_cache.is_available}")
except ImportError:
    logger.warning("Cache service not available, caching disabled")
    _cache = None

@api_bp.route('/parcels', methods=['GET'])
@require_auth
def list_parcels():
    """List all parcels for current tenant"""
    try:
        # Try to get tenant_id from Flask g (Keycloak auth) or request.environ (fallback)
        from flask import g
        tenant_id = getattr(g, 'tenant_id', None) or getattr(g, 'tenant', None) or request.environ.get('tenant_id')
        
        # Connect to database
        conn = psycopg2.connect(POSTGRES_URL)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get parcels for tenant
        cur.execute("""
            SELECT 
                id,
                cadastral_reference,
                municipality,
                province,
                crop_type,
                area_hectares,
                ST_AsGeoJSON(geometry) as geometry,
                ndvi_enabled,
                analytics_enabled,
                is_active,
                created_at
            FROM cadastral_parcels
            WHERE tenant_id = %s AND is_active = true
            ORDER BY created_at DESC
        """, (tenant_id,))
        
        parcels = cur.fetchall()
        cur.close()
        conn.close()
        
        # Convert to JSON
        result = []
        for p in parcels:
            parcel_dict = dict(p)
            # Parse geometry JSON
            if parcel_dict.get('geometry'):
                parcel_dict['geometry'] = json.loads(parcel_dict['geometry'])
            result.append(parcel_dict)
        
        return jsonify({'parcels': result}), 200
        
    except Exception as e:
        logger.error(f"Error listing parcels: {e}")
        return jsonify({'error': 'Internal server error'}), 500

# Parcel creation is owned by entity-manager (the SOLE writer of AgriParcel).
# The cadastral frontend posts to entity-manager POST /api/entities/parcels;
# cadastral_parcels is a read-model projected by subscription. The former direct
# INSERT endpoint was removed to preserve the single-source-of-truth invariant.


@api_bp.route('/parcels/<parcel_id>', methods=['GET'])
@require_auth
def get_parcel(parcel_id):
    """Get a specific parcel by ID"""
    try:
        # Try to get tenant_id from Flask g (Keycloak auth) or request.environ (fallback)
        tenant_id = getattr(g, 'tenant_id', None) or getattr(g, 'tenant', None) or request.environ.get('tenant_id')
        
        # Connect to database
        conn = psycopg2.connect(POSTGRES_URL)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Set tenant context
        cur.execute("SELECT set_config('app.current_tenant', %s, false)", (tenant_id,))
        
        # Get parcel
        cur.execute("""
            SELECT 
                id,
                cadastral_reference,
                municipality,
                province,
                crop_type,
                area_hectares,
                ST_AsGeoJSON(geometry) as geometry,
                ST_AsGeoJSON(centroid) as centroid,
                ndvi_enabled,
                analytics_enabled,
                notes,
                tags,
                is_active,
                created_at,
                updated_at
            FROM cadastral_parcels
            WHERE id = %s
        """, (parcel_id,))
        
        parcel = cur.fetchone()
        cur.close()
        conn.close()
        
        if not parcel:
            return jsonify({'error': 'Parcel not found'}), 404
        
        # Convert to dict and parse geometry
        result = dict(parcel)
        if result.get('geometry'):
            result['geometry'] = json.loads(result['geometry'])
        if result.get('centroid'):
            result['centroid'] = json.loads(result['centroid'])
        
        return jsonify(result), 200
        
    except Exception as e:
        logger.error(f"Error getting parcel: {e}")
        return jsonify({'error': 'Internal server error'}), 500

# Parcel update and delete are owned by entity-manager (PATCH/DELETE
# /api/entities/parcels/{id}); the former direct read-model writes were removed
# to preserve the single-source-of-truth invariant.


@api_bp.route('/parcels/summary', methods=['GET'])
@require_auth
def get_summary():
    """Get summary statistics for tenant parcels"""
    try:
        # Try to get tenant_id from Flask g (Keycloak auth) or request.environ (fallback)
        tenant_id = getattr(g, 'tenant_id', None) or getattr(g, 'tenant', None) or request.environ.get('tenant_id')
        
        # Connect to database
        conn = psycopg2.connect(POSTGRES_URL)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Set tenant context
        cur.execute("SELECT set_config('app.current_tenant', %s, false)", (tenant_id,))
        
        # Get summary
        cur.execute("""
            SELECT * FROM get_tenant_parcels_summary(%s)
        """, (tenant_id,))
        
        summary = cur.fetchone()
        cur.close()
        conn.close()
        
        if not summary:
            summary = {
                'total_parcels': 0,
                'total_area_ha': 0,
                'ndvi_enabled_parcels': 0,
                'ndvi_enabled_area_ha': 0,
                'crop_types': []
            }
        
        return jsonify(dict(summary)), 200
        
    except Exception as e:
        logger.error(f"Error getting summary: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@api_bp.route('/parcels/check-cadastral', methods=['POST'])
@require_auth
def check_cadastral_reference():
    """Check if cadastral reference exists for tenant"""
    try:
        # Try to get tenant_id from Flask g (Keycloak auth) or request.environ (fallback)
        tenant_id = getattr(g, 'tenant_id', None) or getattr(g, 'tenant', None) or request.environ.get('tenant_id')
        data = request.json
        
        cadastral_ref = data.get('cadastral_reference')
        if not cadastral_ref:
            return jsonify({'error': 'Missing cadastral_reference'}), 400
        
        # Connect to database
        conn = psycopg2.connect(POSTGRES_URL)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Set tenant context
        cur.execute("SELECT set_config('app.current_tenant', %s, false)", (tenant_id,))
        
        # Check if exists
        cur.execute("""
            SELECT EXISTS(
                SELECT 1 FROM cadastral_parcels 
                WHERE tenant_id = %s 
                AND cadastral_reference = %s
                AND is_active = true
            ) as exists
        """, (tenant_id, cadastral_ref))
        
        exists = cur.fetchone()['exists']
        cur.close()
        conn.close()
        
        return jsonify({'exists': exists}), 200
        
    except Exception as e:
        logger.error(f"Error checking cadastral: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@api_bp.route('/parcels/<parcel_id>/request-ndvi', methods=['POST'])
@require_auth
def request_ndvi_processing(parcel_id):
    """Request NDVI processing for a parcel"""
    try:
        # Try to get tenant_id from Flask g (Keycloak auth) or request.environ (fallback)
        tenant_id = getattr(g, 'tenant_id', None) or getattr(g, 'tenant', None) or request.environ.get('tenant_id')
        data = request.json or {}
        acquisition_date = data.get('date')  # Optional date override
        
        # Connect to database
        conn = psycopg2.connect(POSTGRES_URL)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Set tenant context
        cur.execute("SELECT set_config('app.current_tenant', %s, false)", (tenant_id,))
        
        # Verify parcel exists and get geometry
        cur.execute("""
            SELECT 
                id,
                ST_AsGeoJSON(geometry) as geometry,
                ndvi_enabled
            FROM cadastral_parcels
            WHERE id = %s
        """, (parcel_id,))
        
        parcel = cur.fetchone()
        if not parcel:
            return jsonify({'error': 'Parcel not found'}), 404
        
        # Check if NDVI processing is enabled for this parcel
        if not parcel.get('ndvi_enabled', True):
            return jsonify({'error': 'NDVI processing is disabled for this parcel'}), 400
        
        # Get geometry for the entity-manager request
        geometry_json = json.loads(parcel.get('geometry', '{}')) if parcel.get('geometry') else None
        
        # Get Orion entity ID if available
        cur.execute("""
            SELECT orion_entity_id
            FROM cadastral_parcels
            WHERE id = %s
        """, (parcel_id,))
        orion_result = cur.fetchone()
        orion_entity_id = orion_result.get('orion_entity_id') if orion_result else None
        
        cur.close()
        conn.close()
        
        # Forward request to entity-manager
        # Entity-manager acts as orchestrator and will handle the NDVI job creation
        try:
            # Get authorization token from request
            auth_header = request.headers.get('Authorization', '')
            
            # Prepare request to entity-manager
            entity_manager_payload = {
                'parcelId': orion_entity_id if orion_entity_id else str(parcel_id),
                'geometry': geometry_json,
            }
            
            # Add optional parameters
            if acquisition_date:
                entity_manager_payload['date'] = acquisition_date
            if data.get('timeRange'):
                entity_manager_payload['timeRange'] = data.get('timeRange')
            if data.get('resolution'):
                entity_manager_payload['resolution'] = data.get('resolution')
            if data.get('satellite'):
                entity_manager_payload['satellite'] = data.get('satellite')
            if data.get('maxCloudCoverage'):
                entity_manager_payload['maxCloudCoverage'] = data.get('maxCloudCoverage')
            
            # Call entity-manager to create NDVI job
            entity_manager_headers = {
                'Authorization': auth_header,
                'X-Source-Module': 'catastro-spain',
                'Content-Type': 'application/json'
            }
            
            entity_manager_response = requests.post(
                f'{ENTITY_MANAGER_URL}/ndvi/jobs',
                json=entity_manager_payload,
                headers=entity_manager_headers,
                timeout=10
            )
            
            if entity_manager_response.status_code in [200, 202]:
                entity_manager_data = entity_manager_response.json()
                logger.info(f"NDVI job created via entity-manager for parcel {parcel_id} (tenant: {tenant_id}, job: {entity_manager_data.get('job', {}).get('id')})")
                
                return jsonify({
                    'message': 'NDVI processing request submitted',
                    'parcel_id': parcel_id,
                    'job_id': entity_manager_data.get('job', {}).get('id'),
                    'status': entity_manager_data.get('job', {}).get('status', 'queued'),
                    'task_type': 'ndvi_processing'
                }), 202
            else:
                error_msg = entity_manager_response.text or 'Unknown error'
                logger.error(f"Entity-manager returned error {entity_manager_response.status_code}: {error_msg}")
                return jsonify({
                    'error': 'Failed to create NDVI job',
                    'details': error_msg
                }), entity_manager_response.status_code
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Error calling entity-manager for NDVI job: {e}")
            return jsonify({
                'error': 'Failed to communicate with entity-manager',
                'details': str(e)
            }), 503
        
    except Exception as e:
        logger.error(f"Error requesting NDVI processing: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@api_bp.route('/parcels/batch-request-ndvi', methods=['POST'])
@require_auth
def batch_request_ndvi():
    """Request NDVI processing for multiple parcels"""
    try:
        # Try to get tenant_id from Flask g (Keycloak auth) or request.environ (fallback)
        tenant_id = getattr(g, 'tenant_id', None) or getattr(g, 'tenant', None) or request.environ.get('tenant_id')
        data = request.json
        
        parcel_ids = data.get('parcel_ids', [])
        if not parcel_ids:
            return jsonify({'error': 'No parcel_ids provided'}), 400
        
        # Connect to database
        conn = psycopg2.connect(POSTGRES_URL)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Set tenant context
        cur.execute("SELECT set_config('app.current_tenant', %s, false)", (tenant_id,))
        
        # Get parcel IDs that exist and have NDVI enabled
        cur.execute("""
            SELECT id
            FROM cadastral_parcels
            WHERE id = ANY(%s) AND ndvi_enabled = true
        """, (parcel_ids,))
        
        # Get full parcel data for valid parcels
        cur.execute("""
            SELECT id, ST_AsGeoJSON(geometry) as geometry, orion_entity_id
            FROM cadastral_parcels
            WHERE id = ANY(%s) AND ndvi_enabled = true
        """, (parcel_ids,))
        
        valid_parcels_data = cur.fetchall()
        cur.close()
        conn.close()
        
        # Get authorization token from request
        auth_header = request.headers.get('Authorization', '')
        
        # Forward each valid parcel to entity-manager
        successful_jobs = []
        failed_jobs = []
        
        for parcel_data in valid_parcels_data:
            parcel_id = parcel_data['id']
            geometry_json = json.loads(parcel_data.get('geometry', '{}')) if parcel_data.get('geometry') else None
            orion_entity_id = parcel_data.get('orion_entity_id')
            
            try:
                entity_manager_payload = {
                    'parcelId': orion_entity_id if orion_entity_id else str(parcel_id),
                    'geometry': geometry_json,
                }
                
                # Add optional parameters from request
                if data.get('timeRange'):
                    entity_manager_payload['timeRange'] = data.get('timeRange')
                if data.get('resolution'):
                    entity_manager_payload['resolution'] = data.get('resolution')
                if data.get('satellite'):
                    entity_manager_payload['satellite'] = data.get('satellite')
                if data.get('maxCloudCoverage'):
                    entity_manager_payload['maxCloudCoverage'] = data.get('maxCloudCoverage')
                
                entity_manager_headers = {
                    'Authorization': auth_header,
                    'X-Source-Module': 'catastro-spain',
                    'Content-Type': 'application/json'
                }
                
                entity_manager_response = requests.post(
                    f'{ENTITY_MANAGER_URL}/ndvi/jobs',
                    json=entity_manager_payload,
                    headers=entity_manager_headers,
                    timeout=10
                )
                
                if entity_manager_response.status_code in [200, 202]:
                    entity_manager_data = entity_manager_response.json()
                    successful_jobs.append({
                        'parcel_id': parcel_id,
                        'job_id': entity_manager_data.get('job', {}).get('id'),
                        'status': entity_manager_data.get('job', {}).get('status', 'queued')
                    })
                else:
                    failed_jobs.append({
                        'parcel_id': parcel_id,
                        'error': entity_manager_response.text or f"HTTP {entity_manager_response.status_code}"
                    })
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Error calling entity-manager for parcel {parcel_id}: {e}")
                failed_jobs.append({
                    'parcel_id': parcel_id,
                    'error': str(e)
                })
        
        logger.info(f"Batch NDVI processing: {len(successful_jobs)} successful, {len(failed_jobs)} failed (tenant: {tenant_id})")
        
        return jsonify({
            'message': 'Batch NDVI processing request completed',
            'requested': len(parcel_ids),
            'valid': len(valid_parcels_data),
            'successful': len(successful_jobs),
            'failed': len(failed_jobs),
            'jobs': successful_jobs,
            'errors': failed_jobs
        }), 202 if successful_jobs else 207  # 207 Multi-Status if some failed
        
    except Exception as e:
        logger.error(f"Error requesting batch NDVI: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@api_bp.route('/parcels/query-by-coordinates', methods=['POST'])
@require_auth
def query_by_coordinates():
    """
    Query cadastral parcel by coordinates (reverse geocoding).
    
    This endpoint:
    1. Determines the geographic region (Navarra/Euskadi/Spain)
    2. Routes to the appropriate cadastral service
    3. Returns cadastral information if found
    
    Request body:
    {
        "longitude": float,
        "latitude": float,
        "srs": "4326" (optional, default WGS84)
    }
    
    Response (200):
    {
        "cadastralReference": str,
        "municipality": str,
        "province": str,
        "address": str,
        "coordinates": {"lon": float, "lat": float},
        "region": "spain" | "navarra" | "euskadi"
    }
    
    Response (501):
    {
        "error": "Not implemented",
        "region": "navarra" | "euskadi",
        "message": "Cadastral service for this region not yet implemented"
    }
    
    Response (404):
    {
        "error": "Parcel not found",
        "message": "No cadastral parcel found at the given coordinates"
    }
    """
    try:
        data = request.get_json(silent=True)
        if not data:
            return jsonify({'error': 'Request body required'}), 400
        
        longitude = data.get('longitude')
        latitude = data.get('latitude')
        srs = data.get('srs', '4326')  # Default to WGS84
        
        if longitude is None or latitude is None:
            return jsonify({'error': 'longitude and latitude are required'}), 400
        
        try:
            longitude = float(longitude)
            latitude = float(latitude)
        except (ValueError, TypeError):
            return jsonify({'error': 'longitude and latitude must be valid numbers'}), 400
        
        # Validate coordinate ranges (rough bounds for Spain)
        if not (-10 <= longitude <= 5) or not (35 <= latitude <= 45):
            return jsonify({
                'error': 'Coordinates out of valid range',
                'message': 'Coordinates must be within Spain bounds'
            }), 400
        
        # Check cache first (if available)
        if _cache and _cache.is_available:
            cached_data = _cache.get_by_coordinates(latitude, longitude)
            if cached_data:
                logger.info(f"Cache HIT for ({longitude}, {latitude})")
                return jsonify(cached_data), 200
            logger.debug(f"Cache MISS for ({longitude}, {latitude})")
        
        # Determine region
        region = get_region(latitude, longitude)
        logger.info(f"Query coordinates ({longitude}, {latitude}) -> region: {region}")
        
        # Route to appropriate service
        if region == 'spain':
            # Spanish State Catastre (DGC)
            if not SpanishStateCatastroClient:
                return jsonify({
                    'error': 'Service unavailable',
                    'message': 'Spanish State Catastro client not initialized'
                }), 503
            
            client = SpanishStateCatastroClient()
            cadastral_data = client.query_by_coordinates(longitude, latitude, srs)
            
            # Ensure consistent response structure
            if cadastral_data:
                # Ensure all required fields exist (even if None)
                cadastral_data.setdefault('cadastralReference', None)
                cadastral_data.setdefault('municipality', None)
                cadastral_data.setdefault('province', None)
                cadastral_data.setdefault('address', None)
                cadastral_data.setdefault('coordinates', {'lon': longitude, 'lat': latitude})
                cadastral_data.setdefault('geometry', None)  # Explicitly set to None if missing
                cadastral_data['region'] = region
                
                # Cache successful response
                if _cache and _cache.is_available:
                    _cache.set_by_coordinates(latitude, longitude, cadastral_data)
                
                return jsonify(cadastral_data), 200
            else:
                # Return consistent structure even when not found (for graceful frontend degradation)
                return jsonify({
                    'cadastralReference': None,
                    'municipality': None,
                    'province': None,
                    'address': None,
                    'coordinates': {'lon': longitude, 'lat': latitude},
                    'geometry': None,
                    'region': region,
                    'error': 'Parcel not found',
                    'message': 'No cadastral parcel found at the given coordinates'
                }), 404
        
        elif region == 'navarra':
            # Use Navarra WFS client
            if not NavarraCatastroClient:
                return jsonify({
                    'error': 'Service unavailable',
                    'region': region,
                    'message': 'Navarra cadastral client not available'
                }), 503
            
            client = NavarraCatastroClient()
            cadastral_data = client.query_by_coordinates(longitude, latitude, srs)
            
            # Ensure consistent response structure
            if cadastral_data:
                # Ensure all required fields exist (even if None)
                cadastral_data.setdefault('cadastralReference', None)
                cadastral_data.setdefault('municipality', None)
                cadastral_data.setdefault('province', None)
                cadastral_data.setdefault('address', None)
                cadastral_data.setdefault('coordinates', {'lon': longitude, 'lat': latitude})
                cadastral_data.setdefault('geometry', None)  # Explicitly set to None if missing
                cadastral_data['region'] = region
                
                # Cache successful response
                if _cache and _cache.is_available:
                    _cache.set_by_coordinates(latitude, longitude, cadastral_data)
                
                return jsonify(cadastral_data), 200
            else:
                # Return consistent structure even when not found (for graceful frontend degradation)
                return jsonify({
                    'cadastralReference': None,
                    'municipality': None,
                    'province': None,
                    'address': None,
                    'coordinates': {'lon': longitude, 'lat': latitude},
                    'geometry': None,
                    'region': region,
                    'error': 'Parcel not found',
                    'message': 'No cadastral parcel found at the given coordinates'
                }), 404
        
        elif region == 'euskadi':
            # Use Euskadi WFS client
            if not EuskadiCatastroClient:
                return jsonify({
                    'error': 'Service unavailable',
                    'region': region,
                    'message': 'Euskadi cadastral client not available'
                }), 503
            
            client = EuskadiCatastroClient()
            cadastral_data = client.query_by_coordinates(longitude, latitude, srs)
            
            # Ensure consistent response structure
            if cadastral_data:
                # Ensure all required fields exist (even if None)
                cadastral_data.setdefault('cadastralReference', None)
                cadastral_data.setdefault('municipality', None)
                cadastral_data.setdefault('province', None)
                cadastral_data.setdefault('address', None)
                cadastral_data.setdefault('coordinates', {'lon': longitude, 'lat': latitude})
                cadastral_data.setdefault('geometry', None)  # Explicitly set to None if missing
                cadastral_data['region'] = region
                
                # Cache successful response
                if _cache and _cache.is_available:
                    _cache.set_by_coordinates(latitude, longitude, cadastral_data)
                
                return jsonify(cadastral_data), 200
            else:
                # Return consistent structure even when not found (for graceful frontend degradation)
                return jsonify({
                    'cadastralReference': None,
                    'municipality': None,
                    'province': None,
                    'address': None,
                    'coordinates': {'lon': longitude, 'lat': latitude},
                    'geometry': None,
                    'region': region,
                    'error': 'Parcel not found',
                    'message': 'No cadastral parcel found at the given coordinates'
                }), 404
        
        else:
            return jsonify({
                'error': 'Unknown region',
                'region': region,
                'message': f'Unexpected region: {region}'
            }), 500
        
    except Exception as e:
        logger.error(f"Error in query_by_coordinates: {e}", exc_info=True)
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500

if __name__ == '__main__':
    # Register blueprint
    app.register_blueprint(api_bp)
    app.run(host='0.0.0.0', port=5000, debug=True)

