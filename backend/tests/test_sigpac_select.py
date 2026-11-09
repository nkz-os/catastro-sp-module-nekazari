"""SIGPAC enclosure selection helpers. Recorded FEGA answers; synthetic tenants only."""

import json
import os
from unittest.mock import MagicMock

import pytest
from pyproj import Transformer
from shapely.geometry import box, mapping, shape
from shapely.ops import transform

from app import sigpac_select as sel
from app import sigpac_service as sp

FIXTURE = os.path.join(os.path.dirname(__file__), 'fixtures', 'fega_recinfo_pair.json')
with open(FIXTURE, encoding='utf-8') as fh:
    RECINFO = json.load(fh)
A, B, FAR = '31:104:0:0:18:398:1', '31:104:0:0:18:389:1', '31:255:0:0:3:44:1'

_fwd = Transformer.from_crs(4326, 25830, always_xy=True).transform
_inv = Transformer.from_crs(25830, 4326, always_xy=True).transform


def metric_box(x0, y0, x1, y1):
    """Box given in metres (EPSG:25830), returned as lon/lat GeoJSON."""
    return mapping(transform(_inv, box(x0, y0, x1, y1)))


ORIGIN_X, ORIGIN_Y = 600000.0, 4700000.0


def test_parse_reference_ok():
    assert sel.parse_reference('31:104:0:0:18:398:1') == (31, 104, 0, 0, 18, 398, 1)


@pytest.mark.parametrize('ref', ['', '31:104:0:0:18:398', 'a:1:0:0:1:1:1',
                                 '31-104-0-0-18-398-1', '31:104:0:0:18:398:1:2', ' 31:104:0:0:18:398:1'])
def test_parse_reference_rejects(ref):
    with pytest.raises(ValueError):
        sel.parse_reference(ref)


def test_summary_maps_keys():
    attrs = RECINFO[A][0]
    s = sel.enclosure_summary(attrs)
    assert s['reference'] == A
    assert s['sigpacUse'] == attrs['uso_sigpac']
    assert s['areaHa'] == attrs['superficie']
    assert s['slopePct'] == attrs['pendiente_media']
    assert s['irrigationCoef'] == attrs['coef_regadio']
    assert s['geometry']['type'] == 'Polygon'


def test_union_adjacent_is_polygon():
    ga = sel.enclosure_summary(RECINFO[A][0])['geometry']
    gb = sel.enclosure_summary(RECINFO[B][0])['geometry']
    union = sel.union_enclosures([ga, gb])
    assert union['type'] == 'Polygon'
    total = sel.area_ha(ga) + sel.area_ha(gb)
    assert sel.area_ha(union) == pytest.approx(total, rel=0.01)


def test_union_closes_small_gap():
    left = metric_box(ORIGIN_X, ORIGIN_Y, ORIGIN_X + 100, ORIGIN_Y + 100)
    right = metric_box(ORIGIN_X + 100.3, ORIGIN_Y, ORIGIN_X + 200, ORIGIN_Y + 100)
    assert sel.union_enclosures([left, right])['type'] == 'Polygon'


def test_union_far_apart_raises():
    left = metric_box(ORIGIN_X, ORIGIN_Y, ORIGIN_X + 100, ORIGIN_Y + 100)
    right = metric_box(ORIGIN_X + 105, ORIGIN_Y, ORIGIN_X + 200, ORIGIN_Y + 100)
    with pytest.raises(sel.NotContiguous):
        sel.union_enclosures([left, right])


def test_union_of_fixture_far_pair_raises():
    with pytest.raises(sel.NotContiguous):
        sel.union_enclosures([sel.enclosure_summary(RECINFO[A][0])['geometry'],
                              sel.enclosure_summary(RECINFO[FAR][0])['geometry']])


def test_union_single_multipolygon_within_tolerance():
    a = shape(metric_box(ORIGIN_X, ORIGIN_Y, ORIGIN_X + 50, ORIGIN_Y + 50))
    b = shape(metric_box(ORIGIN_X + 50.2, ORIGIN_Y, ORIGIN_X + 100, ORIGIN_Y + 50))
    multi = {'type': 'MultiPolygon', 'coordinates': [mapping(a)['coordinates'], mapping(b)['coordinates']]}
    assert sel.union_enclosures([multi])['type'] == 'Polygon'


def test_union_area_not_inflated_by_closing():
    one = metric_box(ORIGIN_X, ORIGIN_Y, ORIGIN_X + 100, ORIGIN_Y + 100)   # 1 ha
    assert sel.area_ha(sel.union_enclosures([one])) == pytest.approx(1.0, rel=0.001)


def _parcel(urn, refs_pcts, name=sp.ATTR_NAME, compact=False):
    encl = [{'reference': r, 'overlapPct': p} for r, p in refs_pcts]
    value = encl[0] if compact and len(encl) == 1 else encl
    return {'id': urn, 'type': 'AgriParcel', name: {'type': 'Property', 'value': value}}


def test_parcels_by_enclosure_threshold():
    parcels = [
        _parcel('urn:ngsi-ld:AgriParcel:p1', [(A, 49.0), (B, 50.0)],
                name='https://nkz-os.org/ns/sigpacEnclosures'),
        _parcel('urn:ngsi-ld:AgriParcel:p2', [(FAR, 100.0)], compact=True),
        {'id': 'urn:ngsi-ld:AgriParcel:p3', 'type': 'AgriParcel'},
    ]
    assert sel.parcels_by_enclosure(parcels) == {
        B: 'urn:ngsi-ld:AgriParcel:p1',
        FAR: 'urn:ngsi-ld:AgriParcel:p2',
    }


def test_enclosure_by_reference_url():
    session = MagicMock()
    resp = MagicMock(status_code=200)
    resp.json.return_value = RECINFO[A]
    session.get.return_value = resp
    out = sp.FegaClient(session=session).enclosure_by_reference(A)
    assert out == RECINFO[A][0]
    url = session.get.call_args.args[0]
    assert url.endswith('/servicioconsultassigpac/query/recinfo/31/104/0/0/18/398/1.json')


def test_enclosure_by_reference_empty_is_none():
    session = MagicMock()
    resp = MagicMock(status_code=200)
    resp.json.return_value = []
    session.get.return_value = resp
    assert sp.FegaClient(session=session).enclosure_by_reference(A) is None


def test_parcels_by_enclosure_uses_share_of_the_enclosure():
    # overlapPct is the share of the PARCEL; "taken" is about the share of the RECINTO.
    parcels = [{'id': 'urn:ngsi-ld:AgriParcel:big', 'type': 'AgriParcel', sp.ATTR_NAME: {'type': 'Property', 'value': [
        {'reference': A, 'overlapPct': 33.0, 'overlapAreaHa': 2.0, 'enclosureAreaHa': 2.0},     # whole recinto
        {'reference': B, 'overlapPct': 100.0, 'overlapAreaHa': 1.0, 'enclosureAreaHa': 10.0},   # 10 % of it
    ]}}]
    assert sel.parcels_by_enclosure(parcels) == {A: 'urn:ngsi-ld:AgriParcel:big'}


def test_normalize_reference_drops_leading_zeros():
    assert sel.normalize_reference('01:001:0:0:018:00398:01') == '1:1:0:0:18:398:1'
    with pytest.raises(ValueError):
        sel.normalize_reference('1:2:3')
