"""SIGPAC click routes: lookup at a point and parcel creation. Synthetic tenants only."""

import json
import os

import pytest
from shapely.geometry import shape

from app import sigpac_routes as rt
from app import sigpac_select as sel
from app.cadastral_api import app

FIXTURE = os.path.join(os.path.dirname(__file__), 'fixtures', 'fega_recinfo_pair.json')
with open(FIXTURE, encoding='utf-8') as fh:
    RECINFO = json.load(fh)
A, B, FAR = '31:104:0:0:18:398:1', '31:104:0:0:18:389:1', '31:255:0:0:3:44:1'
HEADERS = {'X-Tenant-ID': 'tenant-a', 'X-User-ID': 'user-1'}
AT = '/api/cadastral-api/sigpac/enclosure-at'
CREATE = '/api/cadastral-api/sigpac/parcels'
POINT = {'longitude': -1.7300, 'latitude': 42.4100}


class FakeFega:
    """Answers from the recorded fixture; `down` makes every call fail."""
    down = False
    missing: set = set()
    at_point = A

    instances: list = []

    def __init__(self, *a, **kw):
        FakeFega.instances.append(kw)

    def enclosure_at(self, lon, lat):
        if FakeFega.down:
            raise rt.svc.FegaError('down')
        return RECINFO[FakeFega.at_point][0] if FakeFega.at_point else None

    def enclosure_by_reference(self, ref):
        if FakeFega.down or ref in FakeFega.missing:
            raise rt.svc.FegaError('down')
        return RECINFO[ref][0] if ref in RECINFO else None


class FakeOrion:
    def __init__(self):
        self.parcels = []

    def query_entities(self, type=None, limit=100, offset=0, **kw):
        return self.parcels[offset:offset + limit]


@pytest.fixture
def env(monkeypatch):
    app.config['TESTING'] = True
    FakeFega.down, FakeFega.missing, FakeFega.at_point = False, set(), A
    FakeFega.instances = []
    orion = FakeOrion()
    created = []

    def fake_create(tenant_id, payload, user_id='x'):
        created.append(payload)
        return f'urn:ngsi-ld:AgriParcel:new{len(created)}'

    refreshes = []

    def fake_refresh(tenant_id, urn, geometry, **kw):
        refreshes.append((tenant_id, urn))
        # Same semantics as compute_enclosures: overlapPct is the share of the PARCEL.
        union = shape(geometry)
        parcel_ha = sel.area_ha(geometry)
        value = []
        for ref, rec in RECINFO.items():
            enc = sel.enclosure_summary(rec[0])
            if union.contains(shape(enc['geometry']).representative_point()):
                ha = sel.area_ha(enc['geometry'])
                value.append({'reference': ref, 'overlapAreaHa': ha, 'enclosureAreaHa': rec[0]['superficie'],
                              'overlapPct': ha / parcel_ha * 100})
        orion.parcels.append({'id': urn, 'type': 'AgriParcel',
                              'sigpacEnclosures': {'type': 'Property', 'value': value}})
        return {'status': 'updated', 'enclosures': []}

    monkeypatch.setattr(rt.svc, 'FegaClient', FakeFega)
    monkeypatch.setattr(rt, '_orion_for', lambda tenant: orion)
    monkeypatch.setattr(rt, '_place_names', lambda geometry: {})
    monkeypatch.setattr(rt, 'em_create_parcel', fake_create)
    monkeypatch.setattr(rt.svc, 'refresh_parcel', fake_refresh)
    return app.test_client(), orion, created, refreshes


def post(client, url, body):
    return client.post(url, json=body, headers=HEADERS)


# --- lookup -------------------------------------------------------------------

def test_enclosure_at_requires_auth(env):
    client = env[0]
    assert client.post(AT, json=POINT).status_code == 401


def test_enclosure_at_ok_marks_in_parcel(env):
    client, orion, _, _ = env
    orion.parcels.append({'id': 'urn:ngsi-ld:AgriParcel:old', 'type': 'AgriParcel',
                          'sigpacEnclosures': {'type': 'Property',
                                               'value': {'reference': A, 'overlapPct': 100.0}}})
    r = post(client, AT, POINT)
    assert r.status_code == 200
    body = r.get_json()
    assert body['reference'] == A and body['sigpacUse'] == 'TA'
    assert body['geometry']['type'] == 'Polygon'
    assert body['inParcel'] == 'urn:ngsi-ld:AgriParcel:old'


def test_enclosure_at_free_is_not_in_parcel(env):
    assert post(env[0], AT, POINT).get_json()['inParcel'] is None


def test_enclosure_at_gap_is_404(env):
    FakeFega.at_point = None
    assert post(env[0], AT, POINT).status_code == 404


def test_enclosure_at_outside_spain_is_422(env):
    r = post(env[0], AT, {'longitude': 2.35, 'latitude': 48.85})
    assert r.status_code == 422
    assert r.get_json()['error'] == 'outside_coverage'


def test_enclosure_at_fega_down_is_503(env):
    FakeFega.down = True
    r = post(env[0], AT, POINT)
    assert r.status_code == 503
    assert r.get_json()['source'] == 'fega'


@pytest.mark.parametrize('body', [{}, {'longitude': 'x', 'latitude': 1}, {'longitude': -1.7}])
def test_enclosure_at_bad_body_is_400(env, body):
    assert post(env[0], AT, body).status_code == 400


# --- creation -----------------------------------------------------------------

def test_create_one_enclosure(env):
    client, _, created, refreshes = env
    r = post(client, CREATE, {'references': [A]})
    assert r.status_code == 201
    assert r.get_json() == {'id': 'urn:ngsi-ld:AgriParcel:new1', 'sigpacPending': False}
    payload = created[0]
    assert payload['category'] == 'cadastral'
    assert 'cadastralReference' not in payload
    assert payload['name'] == A
    expected = shape(sel.enclosure_summary(RECINFO[A][0])['geometry'])
    assert shape(payload['geometry']).symmetric_difference(expected).area < expected.area * 0.001
    assert refreshes == [('tenant-a', 'urn:ngsi-ld:AgriParcel:new1')]


def test_create_two_adjacent(env):
    client, _, created, _ = env
    r = post(client, CREATE, {'references': [A, B]})
    assert r.status_code == 201
    assert created[0]['name'] == f'{A} +1'
    assert created[0]['geometry']['type'] == 'Polygon'


def test_create_uses_given_name(env):
    client, _, created, _ = env
    post(client, CREATE, {'references': [A], 'name': '  Campo norte '})
    assert created[0]['name'] == 'Campo norte'


def test_create_ignores_client_geometry(env):
    client, _, created, _ = env
    bogus = {'type': 'Polygon', 'coordinates': [[[0, 0], [0, 1], [1, 1], [0, 0]]]}
    post(client, CREATE, {'references': [A], 'geometry': bogus})
    assert created[0]['geometry'] != bogus
    assert shape(created[0]['geometry']).bounds[0] < -1


def test_create_taken_is_409_and_creates_nothing(env):
    client, orion, created, _ = env
    orion.parcels.append({'id': 'urn:ngsi-ld:AgriParcel:old', 'type': 'AgriParcel',
                          'sigpacEnclosures': {'type': 'Property',
                                               'value': [{'reference': B, 'overlapPct': 80.0}]}})
    r = post(client, CREATE, {'references': [A, B]})
    assert r.status_code == 409
    assert r.get_json() == {'error': 'enclosure_in_parcel', 'reference': B,
                            'parcel': 'urn:ngsi-ld:AgriParcel:old'}
    assert created == []


def test_create_twice_second_is_409(env):
    client, _, created, _ = env
    assert post(client, CREATE, {'references': [A]}).status_code == 201
    assert post(client, CREATE, {'references': [A]}).status_code == 409
    assert len(created) == 1


def test_create_partial_fega_failure_is_503_and_creates_nothing(env):
    client, _, created, _ = env
    FakeFega.missing = {B}
    r = post(client, CREATE, {'references': [A, B]})
    assert r.status_code == 503
    assert r.get_json()['source'] == 'fega'
    assert created == []


@pytest.mark.parametrize('refs,code', [
    ([], 'empty'),
    ('31:104:0:0:18:398:1', 'invalid_reference'),
    (['31-104'], 'invalid_reference'),
    (['31:1:0:0:1:1:999'], 'invalid_reference'),          # FEGA has no such recinto
    ([f'31:104:0:0:18:{n}:1' for n in range(21)], 'too_many'),
    ([A, FAR], 'not_contiguous'),
])
def test_create_rejections_are_422(env, refs, code):
    client, _, created, _ = env
    r = post(client, CREATE, {'references': refs})
    assert r.status_code == 422
    assert r.get_json()['error'] == code
    assert created == []


def test_create_duplicates_collapse(env):
    client, _, created, _ = env
    assert post(client, CREATE, {'references': [A, A]}).status_code == 201
    assert created[0]['name'] == A


def test_create_refresh_failure_still_201_pending(env, monkeypatch):
    client, _, created, _ = env
    monkeypatch.setattr(rt.svc, 'refresh_parcel', lambda *a, **k: {'status': 'error'})
    queued = []
    monkeypatch.setattr(rt, 'trigger_refresh', lambda *a, **k: queued.append(a) or True)
    r = post(client, CREATE, {'references': [A]})
    assert r.status_code == 201
    assert r.get_json()['sigpacPending'] is True
    assert queued


def test_create_entity_manager_failure_is_502(env, monkeypatch):
    client = env[0]
    monkeypatch.setattr(rt, 'em_create_parcel', lambda *a, **k: None)
    assert post(client, CREATE, {'references': [A]}).status_code == 502


def test_create_same_two_enclosures_twice_second_is_409(env):
    client, _, created, _ = env
    assert post(client, CREATE, {'references': [A, B]}).status_code == 201
    r = post(client, CREATE, {'references': [A]})       # the smaller recinto alone
    assert r.status_code == 409
    assert len(created) == 1


def test_create_with_leading_zeros_is_normalised(env):
    client, _, created, _ = env
    assert post(client, CREATE, {'references': [A]}).status_code == 201
    assert post(client, CREATE, {'references': ['31:104:00:0:018:398:01']}).status_code == 409
    assert len(created) == 1


def test_create_uses_one_fega_budget(env):
    client = env[0]
    post(client, CREATE, {'references': [A, B]})
    assert FakeFega.instances == [{'deadline_s': rt.SIGPAC_CREATE_BUDGET_S}]


def test_place_names_is_bounded(monkeypatch):
    import time
    monkeypatch.setattr(rt, '_lookup_place_names', lambda geometry: time.sleep(1) or {'municipality': 'x'})
    monkeypatch.setattr(rt, 'PLACE_NAMES_TIMEOUT_S', 0.1)
    start = time.monotonic()
    assert rt._place_names({'type': 'Polygon', 'coordinates': []}) == {}
    assert time.monotonic() - start < 0.5
