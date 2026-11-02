"""SIGPAC enclosures: FEGA fixtures recorded near Estella; no network."""

import json
import pathlib
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
import requests
from shapely.geometry import box, mapping

from app import sigpac_service as sp

FIX = pathlib.Path(__file__).parent / 'fixtures'
OGC = json.loads((FIX / 'fega_ogc_items.json').read_text())
POINTS = json.loads((FIX / 'fega_recinfobypoint.json').read_text())
# Rectangle over four recintos: 228 (43.36 %), 90020 (29.41 %), 417 (14.46 %), 229 (10.81 %).
PARCEL = mapping(box(-2.0335, 42.6684, -2.0315, 42.6696))
NOW = datetime(2026, 11, 2, 12, 0, tzinfo=timezone.utc)


class FakeClient:
    def __init__(self, features=None, error=None):
        self.features = OGC['features'] if features is None else features
        self.error = error
        self.point_calls = []

    def candidates_in_bbox(self, bbox):
        if self.error:
            raise self.error
        return self.features

    def enclosure_at(self, lon, lat):
        self.point_calls.append((lon, lat))
        # resolve by geometry like the real service would
        from shapely.geometry import Point, shape
        for f in self.features:
            if shape(f['geometry']).contains(Point(lon, lat)):
                return POINTS[str(f['properties']['parcela'])][0]
        return None


def resp(status=200, body=None):
    r = MagicMock()
    r.status_code = status
    r.json.return_value = body
    return r


class TestOverlaps:
    def test_four_enclosures_with_pct_over_parcel_area(self):
        out = sp.compute_enclosures(PARCEL, FakeClient(), NOW)
        assert [e['reference'] for e in out] == [
            '31:97:0:0:4:228:1', '31:97:0:0:4:90020:1',
            '31:97:0:0:4:417:1', '31:97:0:0:4:229:1']
        assert [e['overlapPct'] for e in out] == [43.36, 29.41, 14.46, 10.81]
        first = out[0]
        assert first['overlapAreaHa'] == pytest.approx(0.947, abs=0.002)
        assert first['enclosureAreaHa'] == 1.0313
        assert first['sigpacUse'] == 'ZU'
        assert first['slopePct'] == 2.6
        assert first['irrigationCoef'] == 0
        assert first['retrievedAt'] == '2026-11-02T12:00:00Z'

    def test_sliver_below_threshold_dropped(self):
        # shrink the parcel so 229 (10.8 %) stays but cut one under 1 %
        assert sp.SIGPAC_MIN_OVERLAP_PCT == 1.0
        parcel = mapping(box(-2.0335, 42.6684, -2.0315, 42.66921))  # just 3 m into 229
        out = sp.find_overlaps(parcel, OGC['features'])
        refs = [o['feature']['properties']['parcela'] for o in out]
        assert 229 not in refs

    def test_point_lookup_only_for_kept_overlaps(self):
        c = FakeClient()
        sp.compute_enclosures(mapping(box(-2.0335, 42.6684, -2.0315, 42.66921)), c, NOW)
        assert len(c.point_calls) == 3

    def test_no_candidates_gives_empty_list(self):
        assert sp.compute_enclosures(PARCEL, FakeClient(features=[]), NOW) == []

    def test_multipolygon_parcel(self):
        multi = {'type': 'MultiPolygon', 'coordinates': [
            mapping(box(-2.0335, 42.6684, -2.0325, 42.6696))['coordinates'],
            mapping(box(-2.0325, 42.6684, -2.0315, 42.6696))['coordinates']]}
        out = sp.compute_enclosures(multi, FakeClient(), NOW)
        assert len(out) == 4

    def test_invalid_bowtie_still_computed(self):
        bow = {'type': 'Polygon', 'coordinates': [[
            [-2.0335, 42.6684], [-2.0315, 42.6696], [-2.0315, 42.6684],
            [-2.0335, 42.6696], [-2.0335, 42.6684]]]}
        assert sp.compute_enclosures(bow, FakeClient(), NOW) is not None

    def test_mismatched_point_lookup_leaves_attrs_empty(self):
        c = FakeClient()
        c.enclosure_at = lambda lon, lat: POINTS['229'][0]
        out = sp.compute_enclosures(PARCEL, c, NOW)
        wrong = [e for e in out if e['reference'] != '31:97:0:0:4:229:1']
        assert all(e['enclosureAreaHa'] is None for e in wrong)

    def test_utm_zone_by_longitude(self):
        assert sp._metric_epsg(-16.0) == 25828
        assert sp._metric_epsg(-8.0) == 25829
        assert sp._metric_epsg(-2.0) == 25830
        assert sp._metric_epsg(2.5) == 25831


class TestSpain:
    def test_inside_and_outside(self):
        assert sp.is_in_spain(PARCEL)
        paris = mapping(box(2.30, 48.85, 2.31, 48.86))
        assert not sp.is_in_spain(paris)

    @pytest.mark.parametrize('lon,lat', [(-5.31, 35.89), (-2.94, 35.29), (2.65, 39.57),
                                         (-16.5, 28.3), (-2.03, 42.67)])
    def test_spanish_places_inside(self, lon, lat):
        assert sp.is_in_spain(mapping(box(lon, lat, lon + 0.001, lat + 0.001)))

    @pytest.mark.parametrize('lon,lat', [(-9.14, 38.72), (-8.61, 41.15), (2.9, 42.7), (1.52, 42.5),
                                         (-7.6, 33.6)])
    def test_neighbours_outside(self, lon, lat):
        assert not sp.is_in_spain(mapping(box(lon, lat, lon + 0.001, lat + 0.001)))


class TestClient:
    def test_retry_then_success(self):
        s = MagicMock()
        s.get.side_effect = [resp(503), requests.Timeout(), resp(200, {'type': 'FeatureCollection', 'features': [1], 'links': []})]
        sleeps = []
        c = sp.FegaClient(session=s, sleep=sleeps.append)
        assert c.candidates_in_bbox((0, 0, 1, 1)) == [1]
        assert sleeps == [0.5, 1.0]

    def test_gives_up_with_fega_error(self):
        s = MagicMock()
        s.get.return_value = resp(500)
        c = sp.FegaClient(session=s, sleep=lambda _: None)
        with pytest.raises(sp.FegaError):
            c.candidates_in_bbox((0, 0, 1, 1))
        assert s.get.call_count == sp.MAX_RETRIES

    def test_client_error_not_retried(self):
        s = MagicMock()
        s.get.return_value = resp(400)
        with pytest.raises(sp.FegaError):
            sp.FegaClient(session=s, sleep=lambda _: None).enclosure_at(0, 0)
        assert s.get.call_count == 1

    def test_pagination_follows_next(self):
        s = MagicMock()
        s.get.side_effect = [
            resp(200, {'type': 'FeatureCollection', 'features': [1], 'links': [{'rel': 'next', 'href': 'https://x/next'}]}),
            resp(200, {'type': 'FeatureCollection', 'features': [2], 'links': []})]
        c = sp.FegaClient(session=s, sleep=lambda _: None)
        assert c.candidates_in_bbox((0, 0, 1, 1)) == [1, 2]
        assert s.get.call_args_list[1].args[0] == 'https://x/next'

    @pytest.mark.parametrize('body', [{'code': 'InvalidParameterValue'}, [], {'type': 'FeatureCollection'},
                                      {'type': 'FeatureCollection', 'features': 'x'}, None])
    def test_non_feature_collection_is_an_error(self, body):
        s = MagicMock()
        s.get.return_value = resp(200, body)
        with pytest.raises(sp.FegaError):
            sp.FegaClient(session=s, sleep=lambda _: None).candidates_in_bbox((0, 0, 1, 1))

    def test_valid_empty_collection_is_empty(self):
        s = MagicMock()
        s.get.return_value = resp(200, {'type': 'FeatureCollection', 'features': [], 'links': []})
        assert sp.FegaClient(session=s).candidates_in_bbox((0, 0, 1, 1)) == []

    def test_deadline_stops_requests(self):
        s = MagicMock()
        c = sp.FegaClient(session=s, deadline_s=-1)
        with pytest.raises(sp.FegaError):
            c.enclosure_at(0, 0)
        s.get.assert_not_called()

    def test_each_thread_gets_its_own_session(self):
        import threading
        c = sp.FegaClient()
        seen = []
        t = threading.Thread(target=lambda: seen.append(c._session()))
        t.start(); t.join()
        assert seen[0] is not c._session()

    def test_enclosure_at_empty_is_none(self):
        s = MagicMock()
        s.get.return_value = resp(200, [])
        assert sp.FegaClient(session=s).enclosure_at(0, 0) is None


class TestReadTolerance:
    @pytest.mark.parametrize('name', list(sp.ATTR_ALIASES))
    def test_reads_any_attribute_name(self, name):
        ent = {name: {'type': 'Property', 'value': [{'reference': 'a'}]}}
        assert sp.read_enclosures(ent) == [{'reference': 'a'}]

    @pytest.mark.parametrize('name', list(sp.ATTR_ALIASES))
    def test_single_enclosure_compacted_to_object(self, name):
        # Orion-LD compacts a one-element array into the bare element
        ent = {name: {'type': 'Property', 'value': {'reference': 'a'}}}
        assert sp.read_enclosures(ent) == [{'reference': 'a'}]

    def test_absent_is_none(self):
        assert sp.read_enclosures({'id': 'x'}) is None


class TestRefresh:
    URN = 'urn:ngsi-ld:AgriParcel:acme-1'

    def test_writes_with_provenance(self):
        orion = MagicMock()
        r = sp.refresh_parcel('acme', self.URN, geometry=PARCEL, client=FakeClient(),
                              orion=orion, now=NOW)
        assert r['status'] == 'updated'
        urn, attrs = orion.append_entity_attrs.call_args.args
        assert urn == self.URN
        assert attrs['sigpacEnclosures']['source']['value'] == 'fega_sigpac'
        assert len(attrs['sigpacEnclosures']['value']) == 4
        assert orion.append_entity_attrs.call_args.kwargs == {'overwrite': True}

    def test_fega_failure_does_not_write(self):
        orion = MagicMock()
        r = sp.refresh_parcel('acme', self.URN, geometry=PARCEL,
                              client=FakeClient(error=sp.FegaError('down')), orion=orion)
        assert r['status'] == 'error'
        orion.append_entity_attrs.assert_not_called()
        orion.delete_entity_attr.assert_not_called()

    def test_outside_spain_no_http_no_write(self):
        orion = MagicMock()
        c = MagicMock()
        r = sp.refresh_parcel('acme', self.URN, geometry=mapping(box(2.3, 48.85, 2.31, 48.86)),
                              client=c, orion=orion)
        assert r == {'status': 'skipped', 'reason': 'outside_spain'}
        c.candidates_in_bbox.assert_not_called()
        orion.append_entity_attrs.assert_not_called()

    def test_empty_result_overwrites_with_empty_list(self):
        orion = MagicMock()
        sp.refresh_parcel('acme', self.URN, geometry=PARCEL, client=FakeClient(features=[]),
                          orion=orion)
        assert orion.append_entity_attrs.call_args.args[1]['sigpacEnclosures']['value'] == []

    def test_reads_geometry_from_entity(self):
        orion = MagicMock()
        orion.get_entity.return_value = {'location': {'type': 'GeoProperty', 'value': PARCEL}}
        r = sp.refresh_parcel('acme', self.URN, client=FakeClient(), orion=orion, now=NOW)
        assert r['status'] == 'updated'

    def test_only_if_missing_skips_present(self):
        orion = MagicMock()
        orion.get_entity.return_value = {
            'https://nkz-os.org/ns/sigpacEnclosures': {'type': 'Property', 'value': []}}
        r = sp.refresh_parcel('acme', self.URN, client=FakeClient(), orion=orion,
                              only_if_missing=True)
        assert r['reason'] == 'already_present'
        orion.append_entity_attrs.assert_not_called()

    def test_invalid_fega_body_never_overwrites(self):
        orion = MagicMock()
        sess = MagicMock()
        sess.get.return_value = resp(200, {'code': 'InvalidParameterValue'})
        client = sp.FegaClient(session=sess, sleep=lambda _: None)
        r = sp.refresh_parcel('acme', self.URN, geometry=PARCEL, client=client, orion=orion)
        assert r['status'] == 'error'
        orion.append_entity_attrs.assert_not_called()

    def test_write_error_reported_not_raised(self):
        orion = MagicMock()
        orion.append_entity_attrs.side_effect = RuntimeError('orion down')
        r = sp.refresh_parcel('acme', self.URN, geometry=PARCEL, client=FakeClient(), orion=orion)
        assert r['status'] == 'error'
