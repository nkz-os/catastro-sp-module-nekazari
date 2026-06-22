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
from psycopg2.extras import RealDictCursor, Json
import json
from datetime import datetime
import requests

# Orion-LD client wrapper (nkz-platform-sdk SyncOrionClient)
from app.orion_client import get_orion_client

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

# =============================================================================
# Orion-LD Synchronization Endpoint
# =============================================================================

# Entity-manager client — sole write path for AgriParcel
try:
    from entity_client import create_parcel as em_create_parcel
    from entity_client import update_parcel as em_update_parcel
    from entity_client import delete_parcel as em_delete_parcel
except ImportError:
    logger.warning("entity_client not available — entity-manager writes disabled")
    def em_create_parcel(*args, **kwargs): return None
    def em_update_parcel(*args, **kwargs): return False
    def em_delete_parcel(*args, **kwargs): return False

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

@api_bp.route('/orion/notify', methods=['POST'])
def orion_notification():
    """Receive NGSI-LD subscription notifications about AgriParcel changes.

    Called by Orion-LD when an AgriParcel is created/updated/deleted
    via the entity-manager. Maintains the local read-model in PostGIS.
    """
    data = request.json
    if not data or 'data' not in data:
        return jsonify({'error': 'Invalid notification'}), 400

    tenant_id = request.headers.get('NGSILD-Tenant', 'default')
    logger.info("Received Orion notification for tenant %s (%d entities)",
                tenant_id, len(data['data']))

    for entity in data['data']:
        # NGSI-LD entity with potentially expanded attribute names
        entity_id = entity.get('id', '')
        parcel_id = entity_id.split(':')[-1] if entity_id else None
        if not parcel_id:
            continue

        # Extract location — may be short name or expanded URI
        location = entity.get('location') or entity.get('https://uri.etsi.org/ngsi-ld/location', {})
        if isinstance(location, dict):
            geo_value = location.get('value') or location
        else:
            geo_value = None

        # Check for deletion notification
        deleted = entity.get('deleted', False) or entity.get('https://uri.etsi.org/ngsi-ld/deleted', False)

        if deleted:
            _delete_parcel_from_cache(tenant_id, parcel_id)
            logger.info("Deleted parcel %s from cache", parcel_id)
        else:
            _cache_parcel(tenant_id, parcel_id, geo_value, entity)
            logger.info("Cached parcel %s", parcel_id)

    return jsonify({'received': len(data.get('data', []))}), 200


def _ngsi_val(attr: Any) -> Any:
    """Extract the value from an NGSI-LD attribute (short or expanded URI form)."""
    if isinstance(attr, dict):
        return attr.get('value')
    return attr


def _cache_parcel(tenant_id: str, parcel_id: str, geometry: Any, entity: dict) -> None:
    """Store a parcel in the local cache/read-model."""
    if not geometry or not isinstance(geometry, dict):
        logger.warning("Cannot cache parcel %s: invalid or missing geometry", parcel_id)
        return

    # Extract attributes with @context-expanded name fallback
    orion_entity_id = entity.get('id') or f'urn:ngsi-ld:AgriParcel:{parcel_id}'

    def _get_attr(*names):
        for name in names:
            val = entity.get(name)
            if val is not None:
                return _ngsi_val(val)
        return None

    category = _get_attr('category', 'https://uri.etsi.org/ngsi-ld/category') or 'cadastral'
    cadastral_ref = _get_attr('cadastralReference', 'https://uri.etsi.org/ngsi-ld/cadastralReference')
    municipality = _get_attr('municipality', 'https://uri.etsi.org/ngsi-ld/municipality') or 'unknown'
    province = _get_attr('province', 'https://uri.etsi.org/ngsi-ld/province') or 'unknown'
    crop_type = _get_attr('cropType', 'https://uri.etsi.org/ngsi-ld/cropType') or 'unknown'

    try:
        conn = psycopg2.connect(POSTGRES_URL)
        cur = conn.cursor()
        geometry_json = json.dumps(geometry)
        cur.execute("""
            INSERT INTO cadastral_parcels (
                orion_entity_id, tenant_id, geometry,
                category, cadastral_reference,
                municipality, province, crop_type,
                updated_at
            )
            VALUES (%s, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326),
                    %s, %s,
                    %s, %s, %s,
                    NOW())
            ON CONFLICT (orion_entity_id)
            DO UPDATE SET
                geometry = ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326),
                category = %s,
                cadastral_reference = %s,
                municipality = %s,
                province = %s,
                crop_type = %s,
                updated_at = NOW()
        """, (
            orion_entity_id, tenant_id, geometry_json,
            category, cadastral_ref,
            municipality, province, crop_type,
            geometry_json,
            category, cadastral_ref,
            municipality, province, crop_type,
        ))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as exc:
        logger.error("Failed to cache parcel %s: %s", parcel_id, exc)


def _delete_parcel_from_cache(tenant_id: str, parcel_id: str) -> None:
    """Remove a parcel from the local cache."""
    try:
        conn = psycopg2.connect(POSTGRES_URL)
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM cadastral_parcels WHERE tenant_id = %s AND orion_entity_id = %s",
            (tenant_id, f'urn:ngsi-ld:AgriParcel:{parcel_id}')
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as exc:
        logger.error("Failed to delete parcel %s: %s", parcel_id, exc)


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


@api_bp.route('/parcels/parquet', methods=['GET'])
@require_auth
def get_parcels_parquet():
    """Get tenant parcels as GeoParquet (fast, compressed).
    
    Replaces the GeoJSON endpoint for large datasets.
    Frontend can read Parquet directly via @nekazari/geo-utils.
    """
    from flask import Response as _FlaskResponse
    try:
        tenant_id = getattr(g, 'tenant_id', None) or getattr(g, 'tenant', None) or request.environ.get('tenant_id')
        
        conn = psycopg2.connect(POSTGRES_URL)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
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
        
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        if not rows:
            return jsonify({'error': 'No parcels found'}), 404
        
        # Build GeoJSON FeatureCollection
        features = []
        for row in rows:
            geom = json.loads(row['geometry']) if row.get('geometry') else None
            props = {}
            for key in ('id', 'cadastral_reference', 'municipality', 'province',
                        'crop_type', 'area_hectares', 'ndvi_enabled',
                        'analytics_enabled', 'is_active', 'created_at'):
                if key in row:
                    val = row[key]
                    if hasattr(val, 'isoformat'):
                        val = val.isoformat()
                    props[key] = val
            features.append({
                'type': 'Feature',
                'geometry': geom,
                'properties': props,
            })
        
        feature_collection = {
            'type': 'FeatureCollection',
            'features': features,
        }
        
        from app.parquet_service import geojson_to_parquet
        parquet_bytes = geojson_to_parquet(feature_collection)
        if not parquet_bytes:
            return jsonify({'error': 'Conversion failed'}), 500
        
        return _FlaskResponse(
            parquet_bytes,
            mimetype='application/vnd.apache.parquet',
            headers={'Content-Disposition': f'attachment; filename=parcels_{tenant_id}.parquet'},
        )
        
    except Exception as e:
        logger.error(f"Error generating parquet: {e}")
        return jsonify({'error': 'Internal server error'}), 500


@api_bp.route('/parcels', methods=['POST'])
@require_auth
def create_parcel():
    """Create a new cadastral parcel"""
    try:
        # Try to get tenant_id from Flask g (Keycloak auth) or request.environ (fallback)
        from flask import g
        tenant_id = getattr(g, 'tenant_id', None) or getattr(g, 'tenant', None) or request.environ.get('tenant_id')
        user_id = getattr(g, 'user_id', None) or request.environ.get('user_id')
        data = request.json
        
        # Validate required fields
        required_fields = ['municipality', 'province', 'crop_type', 'geometry']
        for field in required_fields:
            if field not in data:
                return jsonify({'error': f'Missing required field: {field}'}), 400
        
        # Validate geometry
        geometry = data.get('geometry')
        if not geometry:
            return jsonify({'error': 'Missing required field: geometry'}), 400
        
        if not isinstance(geometry, dict):
            return jsonify({'error': 'Invalid geometry format. Expected object'}), 400
        
        if geometry.get('type') != 'Polygon':
            return jsonify({'error': f'Invalid geometry type. Expected Polygon, got {geometry.get("type")}'}), 400
        
        if 'coordinates' not in geometry:
            return jsonify({'error': 'Missing coordinates in geometry'}), 400
        
        coordinates = geometry.get('coordinates')
        if not isinstance(coordinates, list) or len(coordinates) == 0:
            return jsonify({'error': 'Invalid coordinates format. Expected non-empty array'}), 400
        
        if not isinstance(coordinates[0], list) or len(coordinates[0]) < 3:
            return jsonify({'error': 'Invalid polygon coordinates. Need at least 3 points'}), 400
        
        # Build NGSI-LD entity for entity-manager (primary write path)
        import time as _time
        cadastral_ref = data.get('cadastral_reference') or data.get('name') or (
            'MANUAL-' + str(int(_time.time()))
        )
        ngsi_ld_entity = {
            'id': f'urn:ngsi-ld:AgriParcel:{cadastral_ref}',
            'type': 'AgriParcel',
            'location': {
                'type': 'GeoProperty',
                'value': geometry,
            },
            'cadastralReference': {
                'type': 'Property',
                'value': cadastral_ref,
            },
            'municipality': {
                'type': 'Property',
                'value': data['municipality'],
            },
            'province': {
                'type': 'Property',
                'value': data['province'],
            },
            'cropType': {
                'type': 'Property',
                'value': data['crop_type'],
            },
            'description': {
                'type': 'Property',
                'value': data.get('notes', ''),
            },
            'category': {
                'type': 'Property',
                'value': 'cadastral',
            },
            '@context': 'https://nekazari.robotika.cloud/ngsi-ld-context.json',
        }

        # ---- Primary write: entity-manager (SoT) ----
        em_id = em_create_parcel(tenant_id, ngsi_ld_entity, user_id or 'anonymous')
        if not em_id:
            logger.error('entity-manager create failed — falling back to PostGIS-only')
            # Fall through to PostGIS (best-effort for dev/staging)

        # ---- Secondary write: local PostGIS read-model ----
        conn = None
        cur = None
        try:
            conn = psycopg2.connect(POSTGRES_URL)
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            # Set tenant context for RLS
            cur.execute("SELECT set_config('app.current_tenant', %s, false)", (tenant_id,))
            
            geometry_json = json.dumps(geometry)
            cur.execute("""
            INSERT INTO cadastral_parcels (
                tenant_id,
                cadastral_reference,
                municipality,
                province,
                crop_type,
                geometry,
                area_hectares,
                selected_by_user_id,
                notes,
                orion_entity_id
            ) VALUES (
                %s, %s, %s, %s, %s,
                ST_GeomFromGeoJSON(%s),
                ST_Area(ST_GeomFromGeoJSON(%s)::geography) / 10000,
                %s,
                %s,
                %s
            )
            RETURNING id, area_hectares
        """, (
            tenant_id,
            cadastral_ref,
            data['municipality'],
            data['province'],
            data['crop_type'],
            geometry_json,
            geometry_json,  # Calculate area from geometry
            user_id,
            data.get('notes'),
            em_id,  # orion_entity_id from entity-manager
        ))
            
            result = cur.fetchone()
            parcel_id = result['id'] if result else em_id or cadastral_ref
            area_hectares = float(result['area_hectares']) if result and result.get('area_hectares') else None
            conn.commit()
            cur.close()
            conn.close()
            
            logger.info(f"Created parcel {parcel_id} for tenant {tenant_id} (area: {area_hectares} ha)")
            return jsonify({
                'id': parcel_id,
                'area_hectares': area_hectares,
                'orion_entity_id': em_id,
                'message': 'Parcel created successfully'
            }), 201
            
        except psycopg2.IntegrityError as e:
            logger.error(f"Integrity error creating parcel: {e}")
            if conn:
                conn.rollback()
            return jsonify({'error': 'Parcel already exists for this tenant'}), 409
        except psycopg2.Error as e:
            logger.error(f"PostgreSQL error creating parcel: {e}", exc_info=True)
            if conn:
                conn.rollback()
            error_msg = str(e)
            if 'geometry' in error_msg.lower() or 'st_geomfromgeojson' in error_msg.lower():
                return jsonify({'error': f'Invalid geometry format: {error_msg}'}), 400
            return jsonify({'error': f'Database error: {error_msg}'}), 500
        except Exception as e:
            logger.error(f"Error creating parcel: {e}", exc_info=True)
            if conn:
                conn.rollback()
            return jsonify({'error': f'Failed to create parcel: {str(e)}'}), 500
    except Exception as e:
        logger.error(f"Unexpected error in create_parcel: {e}", exc_info=True)
        return jsonify({'error': f'Unexpected error: {str(e)}'}), 500

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

@api_bp.route('/parcels/<parcel_id>', methods=['PUT'])
@require_auth
def update_parcel(parcel_id):
    """Update a parcel"""
    try:
        # Try to get tenant_id from Flask g (Keycloak auth) or request.environ (fallback)
        tenant_id = getattr(g, 'tenant_id', None) or getattr(g, 'tenant', None) or request.environ.get('tenant_id')
        user_id = getattr(g, 'user_id', None) or request.environ.get('user_id')
        data = request.json
        
        # Connect to database
        conn = psycopg2.connect(POSTGRES_URL)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Set tenant context
        cur.execute("SELECT set_config('app.current_tenant', %s, false)", (tenant_id,))
        
        # ---- Primary write: entity-manager (SoT) ----
        em_payload = {}
        if 'crop_type' in data:
            em_payload['cropType'] = {'type': 'Property', 'value': data['crop_type']}
        if 'notes' in data:
            em_payload['description'] = {'type': 'Property', 'value': data['notes']}
        if 'cadastral_reference' in data:
            em_payload['cadastralReference'] = {'type': 'Property', 'value': data['cadastral_reference']}
        if 'geometry' in data:
            geometry = data['geometry']
            if geometry.get('type') != 'Polygon':
                return jsonify({'error': 'Invalid geometry type'}), 400
            em_payload['location'] = {'type': 'GeoProperty', 'value': geometry}

        if em_payload:
            em_success = em_update_parcel(tenant_id, parcel_id, em_payload, user_id or 'anonymous')
            if not em_success:
                logger.warning('entity-manager update failed — updating PostGIS only')

        # ---- Secondary write: PostGIS read-model ----
        updates = []
        values = []
        
        allowed_fields = ['crop_type', 'notes', 'ndvi_enabled', 'analytics_enabled', 'tags', 'cadastral_reference']
        for field in allowed_fields:
            if field in data:
                updates.append(f"{field} = %s")
                values.append(data[field])
        
        # Handle geometry update
        if 'geometry' in data:
            geometry = data['geometry']
            if geometry.get('type') != 'Polygon':
                return jsonify({'error': 'Invalid geometry type'}), 400
            updates.append("geometry = ST_GeomFromGeoJSON(%s)")
            values.append(json.dumps(geometry))
        
        if not updates and not em_payload:
            return jsonify({'error': 'No fields to update'}), 400

        if updates:
            # Add parcel_id to values
            values.append(parcel_id)
            
            # Execute update
            query = f"""
                UPDATE cadastral_parcels
                SET {', '.join(updates)}
                WHERE id = %s
                RETURNING id
            """
            cur.execute(query, values)
            
            updated = cur.fetchone()
            conn.commit()
            cur.close()
            conn.close()
            
            if not updated:
                return jsonify({'error': 'Parcel not found'}), 404
        else:
            # Only entity-manager update happened
            cur.close()
            conn.close()
        
        logger.info(f"Updated parcel {parcel_id} for tenant {tenant_id}")
        return jsonify({'message': 'Parcel updated successfully'}), 200
        
    except Exception as e:
        logger.error(f"Error updating parcel: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@api_bp.route('/parcels/<parcel_id>', methods=['DELETE'])
@require_auth
def delete_parcel(parcel_id):
    """Soft delete a parcel (set is_active = false)"""
    try:
        # Try to get tenant_id from Flask g (Keycloak auth) or request.environ (fallback)
        tenant_id = getattr(g, 'tenant_id', None) or getattr(g, 'tenant', None) or request.environ.get('tenant_id')
        user_id = getattr(g, 'user_id', None) or request.environ.get('user_id')
        
        # Connect to database
        conn = psycopg2.connect(POSTGRES_URL)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # ---- Primary delete: entity-manager (SoT) ----
        em_success = em_delete_parcel(tenant_id, parcel_id, user_id or 'anonymous')
        if not em_success:
            logger.warning('entity-manager delete failed — proceeding with PostGIS soft-delete')

        # Set tenant context
        cur.execute("SELECT set_config('app.current_tenant', %s, false)", (tenant_id,))
        
        # Soft delete
        cur.execute("""
            UPDATE cadastral_parcels
            SET is_active = false
            WHERE id = %s
            RETURNING id
        """, (parcel_id,))
        
        deleted = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        
        if not deleted:
            return jsonify({'error': 'Parcel not found'}), 404
        
        logger.info(f"Deleted parcel {parcel_id} for tenant {tenant_id}")
        return jsonify({'message': 'Parcel deleted successfully'}), 200
        
    except Exception as e:
        logger.error(f"Error deleting parcel: {e}")
        return jsonify({'error': 'Internal server error'}), 500

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

