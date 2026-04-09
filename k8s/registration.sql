-- =============================================================================
-- Catastro Spain Module Registration
-- =============================================================================
-- Register Catastro Spain module in the marketplace_modules table
-- =============================================================================

INSERT INTO marketplace_modules (
    id,
    name,
    display_name,
    description,
    remote_entry_url,
    scope,
    exposed_module,
    version,
    author,
    category,
    route_path,
    label,
    module_type,
    required_plan_type,
    pricing_tier,
    is_local,
    is_active,
    required_roles,
    metadata
) VALUES (
    'catastro-spain',
    'catastro-spain',
    'Catastro Spain',
    'Spanish Cadastre integration for reverse geocoding and parcel lookup. Click-to-add parcels functionality.',
    'https://nekazari.robotika.cloud/modules/catastro-spain/nkz-module.js',
    'catastro_spain_module',
    './App',
    '1.0.0',
    'Nekazari Team',
    'geospatial',
    '/catastro',
    'Catastro Spain',
    'ADDON_FREE',
    'basic',
    'FREE',
    false,
    true,
    ARRAY['Farmer', 'TenantAdmin', 'PlatformAdmin'],
    '{"icon": "🗺️", "color": "#3B82F6"}'::jsonb
) ON CONFLICT (id) DO UPDATE SET
    display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    remote_entry_url = EXCLUDED.remote_entry_url,
    scope = EXCLUDED.scope,
    exposed_module = EXCLUDED.exposed_module,
    is_active = true,
    updated_at = NOW();

