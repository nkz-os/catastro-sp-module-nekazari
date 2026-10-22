-- =============================================================================
-- Initial Schema: cadastral_parcels Table with Orion-LD Sync Support
-- =============================================================================
-- Creates cadastral_parcels table with PostGIS geometry and Orion-LD sync columns
-- Part of hybrid architecture: Orion as source of truth, PostGIS as spatial cache

-- Enable PostGIS extension if not already enabled
CREATE EXTENSION IF NOT EXISTS postgis;

-- Create cadastral_parcels table
CREATE TABLE IF NOT EXISTS cadastral_parcels (
    -- Primary key
    id SERIAL PRIMARY KEY,
    
    -- Orion-LD sync columns
    orion_entity_id VARCHAR(255) UNIQUE NOT NULL,  -- NGSI-LD entity ID
    
    -- Multi-tenancy
    tenant_id VARCHAR(255) NOT NULL,
    
    -- Hierarchical structure for zonification
    category VARCHAR(50) NOT NULL DEFAULT 'cadastral',  -- 'cadastral' or 'managementZone'
    ref_parent VARCHAR(255),  -- Reference to parent parcel (Orion entity ID)
    
    -- Cadastral information
    cadastral_reference VARCHAR(255),  -- Official cadastral reference
    municipality VARCHAR(255) NOT NULL,
    province VARCHAR(255) NOT NULL,
    
    -- Agricultural data
    crop_type VARCHAR(255) NOT NULL,
    
    -- Spatial data (PostGIS)
    geometry GEOMETRY(Polygon, 4326) NOT NULL,  -- WGS84 coordinates
    centroid GEOMETRY(Point, 4326) GENERATED ALWAYS AS (ST_Centroid(geometry)) STORED,
    area_hectares DECIMAL(10, 4) GENERATED ALWAYS AS (ST_Area(geometry::geography) / 10000) STORED,
    
    -- NDVI and analytics flags
    ndvi_enabled BOOLEAN DEFAULT true,
    analytics_enabled BOOLEAN DEFAULT true,
    
    -- Additional metadata
    notes TEXT,
    tags JSONB DEFAULT '[]'::jsonb,
    
    -- User tracking
    selected_by_user_id VARCHAR(255),
    
    -- Soft delete
    is_active BOOLEAN DEFAULT true,
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    
    -- Constraints
    CONSTRAINT check_category_valid CHECK (category IN ('cadastral', 'managementZone')),
    CONSTRAINT check_management_zone_has_parent CHECK (
        (category = 'cadastral') OR 
        (category = 'managementZone' AND ref_parent IS NOT NULL)
    )
);

-- Create indices for performance
CREATE INDEX idx_cadastral_parcels_tenant ON cadastral_parcels(tenant_id);
CREATE INDEX idx_cadastral_parcels_orion_entity ON cadastral_parcels(orion_entity_id);
CREATE INDEX idx_cadastral_parcels_ref_parent ON cadastral_parcels(ref_parent);
CREATE INDEX idx_cadastral_parcels_category ON cadastral_parcels(category);
CREATE INDEX idx_cadastral_parcels_tenant_category ON cadastral_parcels(tenant_id, category);
CREATE INDEX idx_cadastral_parcels_active ON cadastral_parcels(is_active) WHERE is_active = true;

-- Spatial index for geometry queries
CREATE INDEX idx_cadastral_parcels_geometry ON cadastral_parcels USING GIST(geometry);
CREATE INDEX idx_cadastral_parcels_centroid ON cadastral_parcels USING GIST(centroid);

-- Create trigger to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_cadastral_parcels_updated_at
    BEFORE UPDATE ON cadastral_parcels
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Create view for hierarchical queries
CREATE OR REPLACE VIEW parcel_hierarchy AS
SELECT 
    p.id,
    p.orion_entity_id,
    p.tenant_id,
    p.category,
    p.cadastral_reference,
    p.municipality,
    p.province,
    p.crop_type,
    p.area_hectares,
    p.ref_parent,
    parent.orion_entity_id as parent_entity_id,
    parent.cadastral_reference as parent_cadastral_ref,
    parent.area_hectares as parent_area_hectares,
    ST_AsGeoJSON(p.geometry) as geometry,
    ST_AsGeoJSON(p.centroid) as centroid,
    p.ndvi_enabled,
    p.analytics_enabled,
    p.created_at,
    p.updated_at
FROM cadastral_parcels p
LEFT JOIN cadastral_parcels parent ON p.ref_parent = parent.orion_entity_id
WHERE p.is_active = true;

-- Create function to get tenant parcels summary
CREATE OR REPLACE FUNCTION get_tenant_parcels_summary(p_tenant_id VARCHAR)
RETURNS TABLE (
    total_parcels BIGINT,
    total_area_ha DECIMAL,
    cadastral_parcels BIGINT,
    management_zones BIGINT,
    ndvi_enabled_parcels BIGINT,
    ndvi_enabled_area_ha DECIMAL,
    crop_types JSONB
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        COUNT(*)::BIGINT as total_parcels,
        COALESCE(SUM(area_hectares), 0)::DECIMAL as total_area_ha,
        COUNT(*) FILTER (WHERE category = 'cadastral')::BIGINT as cadastral_parcels,
        COUNT(*) FILTER (WHERE category = 'managementZone')::BIGINT as management_zones,
        COUNT(*) FILTER (WHERE ndvi_enabled = true)::BIGINT as ndvi_enabled_parcels,
        COALESCE(SUM(area_hectares) FILTER (WHERE ndvi_enabled = true), 0)::DECIMAL as ndvi_enabled_area_ha,
        COALESCE(
            jsonb_agg(DISTINCT jsonb_build_object('crop_type', crop_type, 'count', crop_count))
            FILTER (WHERE crop_type IS NOT NULL),
            '[]'::jsonb
        ) as crop_types
    FROM cadastral_parcels
    CROSS JOIN LATERAL (
        SELECT COUNT(*)::INT as crop_count
        FROM cadastral_parcels cp2
        WHERE cp2.tenant_id = p_tenant_id
        AND cp2.crop_type = cadastral_parcels.crop_type
        AND cp2.is_active = true
    ) crop_counts
    WHERE tenant_id = p_tenant_id
    AND is_active = true;
END;
$$ LANGUAGE plpgsql;

-- Add comments for documentation
COMMENT ON TABLE cadastral_parcels IS 'Cadastral parcels synced from Orion-LD AgriParcel entities. Supports hierarchical zonification.';
COMMENT ON COLUMN cadastral_parcels.orion_entity_id IS 'NGSI-LD entity ID from Orion-LD (e.g., urn:ngsi-ld:AgriParcel:001)';
COMMENT ON COLUMN cadastral_parcels.category IS 'Parcel category: cadastral (parent) or managementZone (subdivision)';
COMMENT ON COLUMN cadastral_parcels.ref_parent IS 'Reference to parent parcel for subdivisions (Orion entity ID)';
COMMENT ON COLUMN cadastral_parcels.geometry IS 'Parcel boundary polygon in WGS84 (EPSG:4326)';
COMMENT ON COLUMN cadastral_parcels.centroid IS 'Automatically calculated centroid of the parcel';
COMMENT ON COLUMN cadastral_parcels.area_hectares IS 'Automatically calculated area in hectares';
COMMENT ON VIEW parcel_hierarchy IS 'View showing parcels with their parent relationships for easy hierarchical queries';
COMMENT ON FUNCTION get_tenant_parcels_summary IS 'Returns summary statistics for a tenant parcels including zonification breakdown';

-- Grant permissions (adjust as needed for your setup)
-- GRANT SELECT, INSERT, UPDATE, DELETE ON cadastral_parcels TO your_app_user;
-- GRANT USAGE, SELECT ON SEQUENCE cadastral_parcels_id_seq TO your_app_user;
