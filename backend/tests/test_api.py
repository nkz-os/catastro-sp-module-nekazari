"""Smoke tests for the cadastral Flask app."""

import pytest

from app.cadastral_api import app


@pytest.fixture
def client():
    app.config["TESTING"] = True
    return app.test_client()


def test_health(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.get_json() == {"status": "healthy", "service": "cadastral-api"}


def test_buildings_requires_auth(client):
    response = client.get("/api/cadastral-api/buildings?bbox=0,0,1,1")
    assert response.status_code == 401
