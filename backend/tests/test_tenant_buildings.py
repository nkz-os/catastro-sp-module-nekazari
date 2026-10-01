"""Viewport building queries return only buildings inside the tenant's parcels."""

from unittest.mock import MagicMock, patch

import pytest

from app import cadastral_api as api


def _parcel(pid, x0, y0, x1, y1):
    return {
        "id": f"urn:ngsi-ld:AgriParcel:{pid}",
        "type": "AgriParcel",
        "location": {
            "type": "GeoProperty",
            "value": {"type": "Polygon", "coordinates": [[
                [x0, y0], [x1, y0], [x1, y1], [x0, y1], [x0, y0],
            ]]},
        },
    }


def _building(fid, x0, y0, x1, y1):
    return {
        "type": "Feature",
        "id": fid,
        "geometry": {"type": "Polygon", "coordinates": [[
            [x0, y0], [x1, y0], [x1, y1], [x0, y1], [x0, y0],
        ]]},
        "properties": {"height": 6.0},
    }


def _orion(pages):
    client = MagicMock()
    client.query_entities.side_effect = pages
    return patch.object(api, "get_orion_client", return_value=client), client


class TestTenantParcelsInBbox:
    def test_keeps_only_parcels_in_view_and_paginates(self):
        page1 = [_parcel(f"p{i}", 10, 10, 11, 11) for i in range(api._ORION_PAGE - 1)]
        page1.append(_parcel("in-view", 0, 0, 1, 1))
        page2 = [_parcel("also-in-view", 1.5, 1.5, 2.5, 2.5), {"id": "urn:x", "type": "AgriParcel"}]
        ctx, client = _orion([page1, page2])
        with ctx:
            parcels = api._tenant_parcels_in_bbox("t1", (0, 0, 2, 2))
        assert [pid for pid, _ in parcels] == [
            "urn:ngsi-ld:AgriParcel:in-view", "urn:ngsi-ld:AgriParcel:also-in-view",
        ]
        assert client.query_entities.call_count == 2
        assert client.query_entities.call_args_list[1].kwargs["offset"] == api._ORION_PAGE

    def test_broker_errors_propagate(self):
        ctx, _ = _orion([RuntimeError("orion down")])
        with ctx, pytest.raises(RuntimeError):
            api._tenant_parcels_in_bbox("t1", (0, 0, 2, 2))


class TestBuildingsForTenantView:
    def _run(self, parcels, buildings_by_bbox):
        def fake_bbox(bbox):
            return {"type": "FeatureCollection", "features": buildings_by_bbox(bbox)}, 200

        with patch.object(api, "_tenant_parcels_in_bbox", return_value=parcels), \
             patch.object(api, "_get_buildings_for_bbox", side_effect=fake_bbox) as wfs, \
             patch.object(api, "_cache", None):
            fc = api._buildings_for_tenant_view("t1", (0, 0, 10, 10))
        return fc, wfs

    def test_only_buildings_touching_a_parcel_are_returned(self):
        from shapely.geometry import box

        parcels = [("urn:ngsi-ld:AgriParcel:a", box(0, 0, 2, 2))]
        # The WFS returns everything in the parcel bbox: one inside, one just outside.
        fc, _ = self._run(parcels, lambda bbox: [
            _building("in", 0.5, 0.5, 1, 1),
            _building("out", 2.5, 2.5, 3, 3),
        ])
        assert [f["id"] for f in fc["features"]] == ["in"]

    def test_buildings_shared_by_adjacent_parcels_are_not_duplicated(self):
        from shapely.geometry import box

        parcels = [
            ("urn:ngsi-ld:AgriParcel:a", box(0, 0, 2, 2)),
            ("urn:ngsi-ld:AgriParcel:b", box(2, 0, 4, 2)),
        ]
        shared = _building("shared", 1.8, 0.5, 2.2, 1)
        fc, wfs = self._run(parcels, lambda bbox: [shared])
        assert [f["id"] for f in fc["features"]] == ["shared"]
        assert wfs.call_count == 2   # one WFS query per parcel, never the whole view

    def test_no_parcels_in_view_means_no_wfs_query(self):
        fc, wfs = self._run([], lambda bbox: [_building("x", 0, 0, 1, 1)])
        assert fc == {"type": "FeatureCollection", "features": []}
        wfs.assert_not_called()

    def test_a_failing_parcel_does_not_drop_the_others(self):
        from shapely.geometry import box

        parcels = [
            ("urn:ngsi-ld:AgriParcel:a", box(0, 0, 2, 2)),
            ("urn:ngsi-ld:AgriParcel:b", box(5, 5, 7, 7)),
        ]

        def fake_bbox(bbox):
            if bbox[0] == 0:
                return {"error": "WFS down"}, 503
            return {"type": "FeatureCollection", "features": [_building("b1", 5.5, 5.5, 6, 6)]}, 200

        with patch.object(api, "_tenant_parcels_in_bbox", return_value=parcels), \
             patch.object(api, "_get_buildings_for_bbox", side_effect=fake_bbox), \
             patch.object(api, "_cache", None):
            fc = api._buildings_for_tenant_view("t1", (0, 0, 10, 10))
        assert [f["id"] for f in fc["features"]] == ["b1"]


class TestParcelBuildingsCache:
    def test_cache_hit_skips_the_wfs(self):
        from shapely.geometry import box

        cache = MagicMock()
        cache.is_available = True
        cache.get_parcel_buildings.return_value = [_building("cached", 0, 0, 1, 1)]
        with patch.object(api, "_cache", cache), \
             patch.object(api, "_get_buildings_for_bbox") as wfs:
            feats = api._buildings_in_parcel("t1", "urn:ngsi-ld:AgriParcel:a", box(0, 0, 2, 2))
        assert [f["id"] for f in feats] == ["cached"]
        wfs.assert_not_called()

    def test_cache_key_changes_with_geometry(self):
        from shapely.geometry import box

        k1 = api._parcel_buildings_cache_key("t1", "p", box(0, 0, 2, 2))
        k2 = api._parcel_buildings_cache_key("t1", "p", box(0, 0, 3, 2))
        assert k1 != k2
        assert k1 == api._parcel_buildings_cache_key("t1", "p", box(0, 0, 2, 2))
