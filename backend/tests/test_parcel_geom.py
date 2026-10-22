"""Tests for parcel geometry resolution (PostGIS + Orion fallback)."""

from unittest.mock import MagicMock, patch

import pytest

from app.cadastral_api import (
    _geometry_from_entity,
    _get_parcel_geom,
    _get_parcel_geom_from_orion,
    _parcel_lookup_keys,
)


class TestParcelLookupKeys:
    def test_uuid_builds_orion_urn(self):
        parcel_uuid = "da36ccd2-85d2-4c76-b552-c5c835a987c1"
        orion_ids, keys = _parcel_lookup_keys(parcel_uuid)
        assert orion_ids == [f"urn:ngsi-ld:AgriParcel:{parcel_uuid}"]
        assert parcel_uuid in keys

    def test_full_urn_preserved(self):
        urn = "urn:ngsi-ld:AgriParcel:da36ccd2-85d2-4c76-b552-c5c835a987c1"
        orion_ids, keys = _parcel_lookup_keys(urn)
        assert orion_ids[0] == urn
        assert urn in keys


class TestGeometryFromEntity:
    def test_short_location_attribute(self):
        entity = {
            "location": {
                "type": "GeoProperty",
                "value": {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]]},
            }
        }
        geo = _geometry_from_entity(entity)
        assert geo["type"] == "Polygon"

    def test_expanded_location_attribute(self):
        entity = {
            "https://uri.etsi.org/ngsi-ld/location": {
                "type": "GeoProperty",
                "value": {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]]},
            }
        }
        geo = _geometry_from_entity(entity)
        assert geo["type"] == "Polygon"


class TestGetParcelGeomFromOrion:
    @patch("app.cadastral_api.get_entity")
    def test_returns_geometry_from_first_matching_entity(self, mock_get_entity):
        mock_get_entity.return_value = {
            "id": "urn:ngsi-ld:AgriParcel:abc",
            "location": {
                "type": "GeoProperty",
                "value": {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]]},
            },
        }
        geo = _get_parcel_geom_from_orion("tenant-a", ["urn:ngsi-ld:AgriParcel:abc"])
        assert geo["type"] == "Polygon"
        mock_get_entity.assert_called_once_with(
            "tenant-a", "urn:ngsi-ld:AgriParcel:abc", entity_type="AgriParcel"
        )


class TestGetParcelGeom:
    @patch("app.cadastral_api._get_parcel_geom_from_orion")
    @patch("app.cadastral_api.psycopg2.connect")
    def test_orion_fallback_when_not_in_postgis(self, mock_connect, mock_orion_fallback):
        from app.cadastral_api import app

        parcel_uuid = "da36ccd2-85d2-4c76-b552-c5c835a987c1"
        mock_orion_fallback.return_value = {
            "type": "Polygon",
            "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]],
        }

        mock_cur = MagicMock()
        mock_cur.fetchone.return_value = None
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cur
        mock_connect.return_value = mock_conn

        with app.app_context():
            from flask import g

            g.tenant_id = "tenant-a"
            geo = _get_parcel_geom(parcel_uuid)

        assert geo["type"] == "Polygon"
        mock_orion_fallback.assert_called_once()
        called_urns = mock_orion_fallback.call_args[0][1]
        assert f"urn:ngsi-ld:AgriParcel:{parcel_uuid}" in called_urns

    @patch("app.cadastral_api._get_parcel_geom_from_orion")
    @patch("app.cadastral_api.psycopg2.connect")
    def test_postgis_hit_skips_orion(self, mock_connect, mock_orion_fallback):
        from app.cadastral_api import app

        parcel_uuid = "da36ccd2-85d2-4c76-b552-c5c835a987c1"
        polygon = '{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,0]]]}'

        mock_cur = MagicMock()
        mock_cur.fetchone.return_value = {"geometry": polygon}
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cur
        mock_connect.return_value = mock_conn

        with app.app_context():
            from flask import g

            g.tenant_id = "tenant-a"
            geo = _get_parcel_geom(parcel_uuid)

        assert geo["type"] == "Polygon"
        mock_orion_fallback.assert_not_called()
