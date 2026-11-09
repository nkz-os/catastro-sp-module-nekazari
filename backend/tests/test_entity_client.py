"""entity-manager client: path and credentials of the parcel write. Synthetic tenants only."""

from unittest.mock import MagicMock

from app import entity_client as ec


def _ok():
    r = MagicMock(status_code=201)
    r.json.return_value = {'id': 'urn:ngsi-ld:AgriParcel:x'}
    return r


def test_create_parcel_posts_to_entity_manager_path(monkeypatch):
    post = MagicMock(return_value=_ok())
    monkeypatch.setattr(ec.requests, 'post', post)
    assert ec.create_parcel('tenant-a', {'name': 'p'}, 'user-1', bearer_token='tok') == 'urn:ngsi-ld:AgriParcel:x'
    url = post.call_args.args[0]
    assert url.endswith('/entities/parcels') and '/api/' not in url


def test_create_parcel_forwards_the_user_token(monkeypatch):
    post = MagicMock(return_value=_ok())
    monkeypatch.setattr(ec.requests, 'post', post)
    ec.create_parcel('tenant-a', {'name': 'p'}, 'user-1', bearer_token='tok')
    headers = post.call_args.kwargs['headers']
    assert headers['Authorization'] == 'Bearer tok'
    assert headers['X-Tenant-ID'] == 'tenant-a'
