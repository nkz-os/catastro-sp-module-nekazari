-- =============================================================================
-- Migration: Add Orion-LD Sync Columns to cadastral_parcels
-- =============================================================================
-- Adds columns needed for Orion-LD to PostGIS synchronization pattern
-- Part of hybrid architecture: Orion as source of truth, PostGIS as spatial cache

-- Add sync columns
ALTER TABLE cadastral_parcels 
ADD COLUMN IF NOT EXISTS orion_entity_id VARCHAR(255) UNIQUE,
ADD COLUMN IF NOT EXISTS category VARCHAR(50) DEFAULT 'cadastral',
ADD COLUMN IF NOT EXISTS ref_parent VARCHAR(255);

-- Add comments for documentation
COMMENT ON COLUMN cadastral_parcels.orion_entity_id IS 'NGSI-LD entity ID from Orion-LD (e.g., urn:ngsi-ld:AgriParcel:001)';
COMMENT ON COLUMN cadastral_parcels.category IS 'Parcel category: cadastral (parent) or managementZone (subdivision)';
COMMENT ON COLUMN cadastral_parcels.ref_parent IS 'Reference to parent parcel for subdivisions (Orion entity ID)';

-- Create indices for performance
CREATE INDEX IF NOT EXISTS idx_cadastral_parcels_orion_entity 
ON cadastral_parcels(orion_entity_id);

CREATE INDEX IF NOT EXISTS idx_cadastral_parcels_ref_parent 
ON cadastral_parcels(ref_parent);

CREATE INDEX IF NOT EXISTS idx_cadastral_parcels_category 
ON cadastral_parcels(category);

-- Create index for tenant + category queries (common pattern)
CREATE INDEX IF NOT EXISTS idx_cadastral_parcels_tenant_category 
ON cadastral_parcels(tenant_id, category);

-- Update existing rows to have default category
UPDATE cadastral_parcels 
SET category = 'cadastral' 
WHERE category IS NULL;

-- Make category NOT NULL after setting defaults
ALTER TABLE cadastral_parcels 
ALTER COLUMN category SET NOT NULL;

-- Add constraint to ensure valid categories
ALTER TABLE cadastral_parcels
ADD CONSTRAINT check_category_valid 
CHECK (category IN ('cadastral', 'managementZone'));

-- Add constraint: managementZone must have ref_parent
ALTER TABLE cadastral_parcels
ADD CONSTRAINT check_management_zone_has_parent
CHECK (
  (category = 'cadastral') OR 
  (category = 'managementZone' AND ref_parent IS NOT NULL)
);

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
  ST_AsGeoJSON(p.geometry) as geometry,
  p.created_at,
  p.updated_at
FROM cadastral_parcels p
LEFT JOIN cadastral_parcels parent ON p.ref_parent = parent.orion_entity_id
WHERE p.is_active = true;

COMMENT ON VIEW parcel_hierarchy IS 'View showing parcels with their parent relationships for easy hierarchical queries';
