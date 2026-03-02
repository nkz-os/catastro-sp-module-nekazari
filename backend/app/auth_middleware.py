#!/usr/bin/env python3
# =============================================================================
# Simple Authentication Middleware for Catastro Spain Module
# =============================================================================
# Trusts API Gateway validation - only decodes token and extracts tenant
# No need for complex Keycloak JWKs validation as API Gateway handles it

import os
import logging
from functools import wraps
from flask import request, jsonify, g
import jwt

logger = logging.getLogger(__name__)

# Configuration
TRUST_API_GATEWAY = os.getenv('TRUST_API_GATEWAY', 'true').lower() == 'true'


def get_request_token():
    """Extract JWT token from Authorization header or httpOnly cookie (fallback)."""
    auth_header = request.headers.get('Authorization')
    if auth_header and auth_header.startswith('Bearer '):
        return auth_header.split(' ')[1]
    return request.cookies.get('nkz_token')


def require_auth(f):
    """
    Simple authentication decorator for Flask routes.

    Trusts API Gateway validation:
    - If X-Tenant-ID header is present, uses it (API Gateway already validated)
    - Only decodes token to extract user info (no signature verification)
    - Stores user info in Flask g for access in route handlers
    - Reads token from Authorization header or httpOnly cookie (fallback)
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Get token from Authorization header or httpOnly cookie
        token = get_request_token()
        if not token:
            return jsonify({'error': 'Missing or invalid authorization header'}), 401
        
        # Get tenant from X-Tenant-ID header (set by API Gateway)
        tenant_id = request.headers.get('X-Tenant-ID')
        
        try:
            # Decode token without verification (API Gateway already validated it)
            # Only check expiration
            payload = jwt.decode(token, options={"verify_signature": False, "verify_exp": True})
            
            # Use tenant from header if available, otherwise try to extract from token
            if not tenant_id:
                tenant_id = payload.get('tenant-id') or payload.get('tenant_id') or payload.get('tenant')
            
            if not tenant_id:
                logger.warning("No tenant_id found in token or X-Tenant-ID header")
                return jsonify({'error': 'Tenant ID not found'}), 401
            
            # Store in Flask g for access in route handlers
            g.current_user = payload
            g.tenant = tenant_id
            g.tenant_id = tenant_id
            g.user_id = payload.get('sub')
            g.username = payload.get('preferred_username')
            g.email = payload.get('email')
            g.roles = payload.get('realm_access', {}).get('roles', [])
            
            return f(*args, **kwargs)
            
        except jwt.ExpiredSignatureError:
            logger.warning("Token expired")
            return jsonify({'error': 'Token has expired'}), 401
        except Exception as e:
            logger.error(f"Error in auth decorator: {e}")
            return jsonify({'error': 'Authentication error'}), 500
    
    return decorated_function


def get_current_user():
    """Get current user from Flask request context"""
    return getattr(g, 'current_user', None)


def get_current_tenant():
    """Get current tenant from Flask request context"""
    return getattr(g, 'tenant', None) or getattr(g, 'tenant_id', None)


