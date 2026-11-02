"""SIGPAC routes: auth, backfill, notifications, create hook. Synthetic tenants only."""

import json
from concurrent.futures import Future
from unittest.mock import MagicMock

import pytest
from shapely.geometry import box, mapping

from app import sigpac_routes as rt
from app.cadastral_api import app
from nkz_platform_sdk.notify_auth import notification_token

PARCEL = mapping(box(-2.0335, 42.6684, -2.0315, 42.6696))
SECRET = 'unit-test-secret'


class InlineExecutor:
    def submit(self, fn, *a, **kw):
        f = Future()
        try:
            f.set_result(fn(*a, **kw))
        except Exception as exc:
            f.set_exception(exc)
        return f


class NeverExecutor:
    def submit(self, fn, *a, **kw):
        return Future()


PARCEL_ENTITY = {'id': 'urn:ngsi-ld:AgriParcel:p1', 'type': 'AgriParcel',
                 'location': {'type': 'GeoProperty', 'value': PARCEL}}


@pytest.fixture
def client(monkeypatch):
    app.config['TESTING'] = True
    monkeypatch.setenv('INTERNAL_SERVICE_SECRET', SECRET)
    monkeypatch.setattr(rt, '_executor', InlineExecutor())
    monkeypatch.setattr(rt, '_backfill_executor', InlineExecutor())
    monkeypatch.setattr('app.orion_client.get_entity', lambda *a, **k: dict(PARCEL_ENTITY))
    rt._recent.clear()
    return app.test_client()


def test_refresh_requires_auth(client):
    assert client.post('/api/cadastral-api/parcels/p1/sigpac/refresh').status_code == 401


def test_refresh_ok_includes_attribution(client, monkeypatch):
    monkeypatch.setattr(rt.svc, 'refresh_parcel', MagicMock(
        return_value={'status': 'updated', 'enclosures': []}))
    r = client.post('/api/cadastral-api/parcels/p1/sigpac/refresh',
                    headers={'X-Tenant-ID': 'acme'}, json={'onlyIfMissing': True})
    assert r.status_code == 200
    assert r.get_json()['source']['license'] == 'CC BY 4.0'
    args = rt.svc.refresh_parcel.call_args
    assert args.args[:2] == ('acme', 'urn:ngsi-ld:AgriParcel:p1')
    assert args.kwargs['only_if_missing'] is True


def test_refresh_error_is_502(client, monkeypatch):
    monkeypatch.setattr(rt.svc, 'refresh_parcel', MagicMock(return_value={'status': 'error'}))
    r = client.post('/api/cadastral-api/parcels/p1/sigpac/refresh', headers={'X-Tenant-ID': 'acme'})
    assert r.status_code == 502


def test_refresh_404_when_parcel_missing(client, monkeypatch):
    monkeypatch.setattr('app.orion_client.get_entity', lambda *a, **k: None)
    m = MagicMock(); monkeypatch.setattr(rt.svc, 'refresh_parcel', m)
    r = client.post('/api/cadastral-api/parcels/p1/sigpac/refresh', headers={'X-Tenant-ID': 'acme'})
    assert r.status_code == 404
    m.assert_not_called()


def test_refresh_404_for_other_entity_types(client, monkeypatch):
    monkeypatch.setattr('app.orion_client.get_entity',
                        lambda *a, **k: {'id': 'urn:ngsi-ld:Device:x', 'type': 'Device'})
    r = client.post('/api/cadastral-api/parcels/x/sigpac/refresh', headers={'X-Tenant-ID': 'acme'})
    assert r.status_code == 404


def test_refresh_is_bounded_and_answers_202(client, monkeypatch):
    monkeypatch.setattr(rt, '_executor', NeverExecutor())
    monkeypatch.setattr(rt, 'REFRESH_WAIT_S', 0.01)
    r = client.post('/api/cadastral-api/parcels/p1/sigpac/refresh', headers={'X-Tenant-ID': 'acme'})
    assert r.status_code == 202 and r.get_json()['status'] == 'pending'


def test_get_reads_full_iri_attribute(client, monkeypatch):
    ent = {'https://nkz-os.org/ns/sigpacEnclosures': {'type': 'Property', 'value': [{'reference': 'r'}]}}
    monkeypatch.setattr('app.orion_client.get_entity', lambda *a, **k: ent)
    r = client.get('/api/cadastral-api/parcels/p1/sigpac', headers={'X-Tenant-ID': 'acme'})
    assert r.get_json()['enclosures'] == [{'reference': 'r'}]


class TestBackfill:
    def test_secret_required(self, client):
        assert client.post('/api/cadastral-api/internal/sigpac/backfill',
                           json={'tenant_id': 'acme'}).status_code == 401
        assert client.post('/api/cadastral-api/internal/sigpac/backfill',
                           headers={'X-Internal-Service-Secret': 'wrong'},
                           json={'tenant_id': 'acme'}).status_code == 401

    def test_tenant_required(self, client):
        r = client.post('/api/cadastral-api/internal/sigpac/backfill',
                        headers={'X-Internal-Service-Secret': SECRET}, json={})
        assert r.status_code == 400

    def test_accepted_runs_backfill(self, client, monkeypatch):
        m = MagicMock()
        monkeypatch.setattr(rt, 'backfill_job', m)
        r = client.post('/api/cadastral-api/internal/sigpac/backfill',
                        headers={'X-Internal-Service-Secret': SECRET},
                        json={'tenant_id': 'acme', 'force': True})
        assert r.status_code == 202
        m.assert_called_once_with('acme', True)

    def test_run_backfill_skips_present_unless_forced(self, monkeypatch):
        orion = MagicMock()
        loc = {'type': 'GeoProperty', 'value': PARCEL}
        orion.query_entities.return_value = [
            {'id': 'urn:ngsi-ld:AgriParcel:a', 'location': loc},
            {'id': 'urn:ngsi-ld:AgriParcel:b', 'location': loc,
             'sigpacEnclosures': {'type': 'Property', 'value': []}},
            {'id': 'urn:ngsi-ld:AgriParcel:c'},
        ]
        calls = []
        monkeypatch.setattr(rt.svc, 'refresh_parcel',
                            lambda t, u, g, **k: calls.append(u) or {'status': 'updated'})
        out = rt.run_backfill('acme', orion=orion)
        assert calls == ['urn:ngsi-ld:AgriParcel:a']
        assert out == {'updated': 1, 'skipped': 2, 'error': 0}
        calls.clear()
        rt.run_backfill('acme', force=True, orion=orion)
        assert calls == ['urn:ngsi-ld:AgriParcel:a', 'urn:ngsi-ld:AgriParcel:b']

    def test_backfill_job_logs_and_survives_errors(self, monkeypatch, caplog):
        monkeypatch.setattr(rt, 'run_backfill', MagicMock(side_effect=RuntimeError('orion down')))
        with caplog.at_level('ERROR'):
            rt.backfill_job('acme', False)
        assert 'acme' in caplog.text and 'orion down' in caplog.text

    def test_run_backfill_paginates(self, monkeypatch):
        orion = MagicMock()
        full = [{'id': f'urn:ngsi-ld:AgriParcel:{i}'} for i in range(rt._BACKFILL_PAGE)]
        orion.query_entities.side_effect = [full, []]
        rt.run_backfill('acme', orion=orion)
        assert orion.query_entities.call_count == 2


class TestNotify:
    URL = '/api/cadastral-api/orion/sigpac-notify'

    def body(self):
        return {'data': [{'id': 'urn:ngsi-ld:AgriParcel:n1',
                          'location': {'type': 'GeoProperty', 'value': PARCEL}}]}

    def headers(self, tenant='acme', token=True):
        h = {'NGSILD-Tenant': tenant, 'Host': 'recv.local'}
        if token:
            h['X-Notification-Token'] = notification_token(SECRET, tenant, 'recv.local')
        return h

    def test_missing_token_401(self, client, monkeypatch):
        m = MagicMock(); monkeypatch.setattr(rt.svc, 'refresh_parcel', m)
        assert client.post(self.URL, json=self.body(), headers=self.headers(token=False)).status_code == 401
        m.assert_not_called()

    def test_token_for_other_tenant_401(self, client, monkeypatch):
        h = self.headers()
        h['NGSILD-Tenant'] = 'other'
        assert client.post(self.URL, json=self.body(), headers=h).status_code == 401

    def test_valid_token_triggers_refresh_204(self, client, monkeypatch):
        m = MagicMock(); monkeypatch.setattr(rt.svc, 'refresh_parcel', m)
        r = client.post(self.URL, json=self.body(), headers=self.headers())
        assert r.status_code == 204 and r.data == b''
        assert m.call_args.args[:2] == ('acme', 'urn:ngsi-ld:AgriParcel:n1')

    def test_repeated_same_geometry_deduped(self, client, monkeypatch):
        m = MagicMock(); monkeypatch.setattr(rt.svc, 'refresh_parcel', m)
        for _ in range(2):
            client.post(self.URL, json=self.body(), headers=self.headers())
        assert m.call_count == 1

    def test_create_and_notify_share_one_dedupe_key(self, client, monkeypatch):
        m = MagicMock(); monkeypatch.setattr(rt.svc, 'refresh_parcel', m)
        urn = 'urn:ngsi-ld:AgriParcel:n1'
        # create path: tenant as received, geometry as sent by the user
        assert rt.trigger_refresh('ACME', urn, PARCEL) is True
        # notification: Orion round-trip changes float noise below 1e-7 deg
        noisy = json.loads(json.dumps(PARCEL).replace('42.6684', '42.66840000001'))
        client.post(self.URL, json={'data': [{'id': urn, 'location': {'type': 'GeoProperty', 'value': noisy}}]},
                    headers=self.headers())
        assert m.call_count == 1

    def test_deleted_ignored(self, client, monkeypatch):
        m = MagicMock(); monkeypatch.setattr(rt.svc, 'refresh_parcel', m)
        b = self.body(); b['data'][0]['deleted'] = True
        client.post(self.URL, json=b, headers=self.headers())
        m.assert_not_called()


def test_subscription_disabled_without_url(monkeypatch):
    from app import sigpac_subscription as sub
    monkeypatch.delenv('SIGPAC_NOTIFY_URL', raising=False)
    assert sub.start_background() is False


def test_subscription_declaration(monkeypatch):
    from app import sigpac_subscription as sub
    monkeypatch.setenv('SIGPAC_NOTIFY_URL', 'http://recv.local:5000/api/cadastral-api/orion/sigpac-notify')
    monkeypatch.setenv('INTERNAL_SERVICE_SECRET', SECRET)
    monkeypatch.delenv('NKZ_NOTIFY_DERIVED_TOKENS', raising=False)
    reg = sub.build_registrar()
    body = reg._body(reg._subs[0], 'acme')
    assert body['watchedAttributes'] == ['location']
    assert 'throttling' not in body      # Orion throttles per subscription: would drop events
    keys = [h['key'] for h in body['notification']['endpoint']['receiverInfo']]
    assert keys == ['X-Notification-Token']
