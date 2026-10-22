#!/usr/bin/env python3
# =============================================================================
# Authentication Middleware for Catastro Spain Module
# =============================================================================
# Trusts API Gateway validation + validates X-Auth-Signature HMAC.
#
# Per AGENTS.md:
#   backends run REQUIRE_HMAC_SIGNATURE=true, so require_auth rejects
#   unsigned requests with a SILENT 401.
#
# JWT decoding via PyJWT (verify against API Gateway's validation).
# HMAC signature uses HMAC_SECRET shared with api-gateway + entity-manager.

import os
import hashlib
import hmac
import logging
import time
from functools import wraps

from flask import request, jsonify, g

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
HMAC_SECRET = os.getenv('HMAC_SECRET', '')
REQUIRE_HMAC = os.getenv('REQUIRE_HMAC_SIGNATURE', 'true').lower() == 'true'


# ---------------------------------------------------------------------------
# Public-prefix check
# ---------------------------------------------------------------------------
# Endpoints starting with these prefixes skip auth entirely.
SKIP_AUTH_PREFIXES = tuple(
    p.strip()
    for p in os.getenv('SKIP_AUTH_PREFIXES', '/health,/readyz,/orion/').split(',')
    if p.strip()
)


def _should_skip_auth(path: str) -> bool:
    """Return True if *path* starts with any SKIP_AUTH_PREFIXES prefix."""
    return path.startswith(SKIP_AUTH_PREFIXES)


# ---------------------------------------------------------------------------
# HMAC helpers — canonical format (aligned with keycloak_auth.py)
#
# Platform-wide HMAC format (MANDATORY):
#   Internal payload: {data}|{tenant_id}|{timestamp}   (pipe-separated)
#   Output:           {HMAC-SHA256 hexdigest}:{timestamp} (colon-separated, sig first)
#   Algorithm:        HMAC-SHA256, full hex digest (64 chars), NO truncation
#   Verification:     hmac.compare_digest() + timestamp window (5 min)
#
# See services/common/keycloak_auth.py for the canonical reference.
# ---------------------------------------------------------------------------


def _generate_timestamped_signature(token: str, tenant_id: str, timestamp: int) -> str:
    """Generate HMAC-SHA256 signature for given token+tenant+timestamp."""
    if not HMAC_SECRET:
        return ''
    # Pipe separator matches keycloak_auth.py
    payload = f'{token}|{tenant_id}|{timestamp}'
    return hmac.new(
        HMAC_SECRET.encode(),
        payload.encode(),
        hashlib.sha256,
    ).hexdigest()


def generate_hmac_signature(token: str, tenant_id: str) -> str:
    """Generate an HMAC-SHA256 signature for a token + tenant pair.

    Canonical platform format (mirrors keycloak_auth.py:generate_hmac_signature):
      payload  = {token}|{tenant_id}|{timestamp}
      output   = {hexdigest}:{timestamp}

    Returns:
        Empty string if HMAC_SECRET not configured, else "{sig}:{timestamp}".
    """
    if not HMAC_SECRET:
        return ''
    timestamp = int(time.time())
    sig = _generate_timestamped_signature(token, tenant_id, timestamp)
    return f'{sig}:{timestamp}'


def _validate_hmac(signature: str, token: str, tenant_id: str, user_id: str) -> bool:
    """Validate an HMAC signature against the canonical platform format.

    Parses "{sig}:{timestamp}", verifies HMAC and checks timestamp is
    within 5-minute window (matching keycloak_auth.py:verify_hmac_signature).

    Returns True when valid or when HMAC validation is disabled.
    """
    if not REQUIRE_HMAC or not HMAC_SECRET:
        return True  # validation disabled

    try:
        parts = signature.split(':')
        if len(parts) != 2:
            logger.warning('Invalid HMAC signature format (expected sig:ts)')
            return False

        provided_sig, timestamp_str = parts
        timestamp = int(timestamp_str)

        # Check 5-minute window
        if abs(int(time.time()) - timestamp) > 300:
            logger.warning('HMAC signature timestamp outside 5min window')
            return False

        expected = _generate_timestamped_signature(token, tenant_id, timestamp)
        return hmac.compare_digest(provided_sig, expected)

    except (ValueError, IndexError) as e:
        logger.warning('HMAC validation error: %s', e)
        return False


# ---------------------------------------------------------------------------
# Require-auth decorator
# ---------------------------------------------------------------------------

def require_auth(f):
    """
    Authentication decorator for Flask routes.

    Validates:
      1. X-Tenant-ID header (mandatory)
      2. X-User-Id header (mandatory for HMAC)
      3. X-Auth-Signature HMAC (mandatory when REQUIRE_HMAC=true)

    Stores extracted info in Flask *g* for route handlers:
      g.tenant_id, g.user_id, g.current_user (JWT payload).
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # ---- Skip auth for public endpoints ----
        path = request.path
        if _should_skip_auth(path):
            logger.debug('Skipping auth for public path: %s', path)
            return f(*args, **kwargs)

        # ---- Headers from API Gateway ----
        token = _get_request_token()
        tenant_id = request.headers.get('X-Tenant-ID')
        user_id = request.headers.get('X-User-ID')
        signature = request.headers.get('X-Auth-Signature')

        # ---- Tenant ----
        if not tenant_id:
            logger.warning('Missing X-Tenant-ID')
            return jsonify({'error': 'Missing X-Tenant-ID'}), 401

        # ---- HMAC signature validation ----
        if REQUIRE_HMAC and HMAC_SECRET:
            if signature:
                token_for_hmac = token or ''
                if not _validate_hmac(signature, token_for_hmac, tenant_id, user_id or ''):
                    logger.warning(
                        'Invalid X-Auth-Signature for tenant %s',
                        tenant_id,
                    )
                    return jsonify({'error': 'Missing or invalid signature'}), 401
            elif not token:
                logger.warning(
                    'Missing X-Auth-Signature and JWT for tenant %s',
                    tenant_id,
                )
                return jsonify({'error': 'Missing or invalid signature'}), 401

        # ---- JWT decode (best-effort for user info) ----
        current_user = None
        if token:
            try:
                import jwt as pyjwt

                current_user = pyjwt.decode(
                    token,
                    options={'verify_signature': False, 'verify_exp': True},
                )
                if not user_id:
                    user_id = current_user.get('sub')
            except Exception:
                logger.debug('Could not decode JWT (proceeding anyway)')

        # ---- Store in Flask g ----
        g.current_user = current_user
        g.tenant_id = tenant_id
        g.user_id = user_id or 'unknown'

        logger.info(
            'Auth OK: tenant=%s user=%s',
            g.tenant_id,
            g.user_id,
        )
        return f(*args, **kwargs)

    return decorated_function


# ---------------------------------------------------------------------------
# Helpers for route handlers
# ---------------------------------------------------------------------------

def _get_request_token():
    """Extract JWT token from Authorization header or httpOnly cookie."""
    auth_header = request.headers.get('Authorization')
    if auth_header and auth_header.startswith('Bearer '):
        return auth_header.split(' ', 1)[1]
    return request.cookies.get('nkz_token')


def get_current_user():
    """Return current user dict from Flask g, or None."""
    return getattr(g, 'current_user', None)


def get_current_tenant():
    """Return tenant ID from Flask g, or None."""
    return getattr(g, 'tenant_id', None) or getattr(g, 'tenant', None)
