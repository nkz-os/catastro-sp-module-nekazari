"""Tests for cadastral geometry simplification.

The Spanish Catastro WFS/INSPIRE service returns parcel boundaries with
hundreds of vertices (sub-metre detail). `_simplify_geometry` must reduce
that to a reasonable vertex count (1 m tolerance, via UTM reprojection)
without breaking the parcel shape or raising.
"""

import math

from app.catastro_clients import (
    CADASTRAL_GEOMETRY_SIMPLIFY_TOLERANCE_M,
    _simplify_geometry,
)


def _dense_polygon(n_points: int = 486, radius_deg: float = 0.001) -> dict:
    """A dense near-circle polygon around Navarra (EPSG:4326)."""
    cx, cy = -1.6, 42.6
    coords = []
    for i in range(n_points):
        a = 2 * math.pi * i / n_points
        coords.append([cx + radius_deg * math.cos(a), cy + radius_deg * math.sin(a)])
    coords.append(coords[0])  # close the ring
    return {"type": "Polygon", "coordinates": [coords]}


def test_dense_polygon_is_simplified():
    geom = _dense_polygon(n_points=486)
    out = _simplify_geometry(geom)
    assert out["type"] == "Polygon"
    n_in = len(geom["coordinates"][0])
    n_out = len(out["coordinates"][0])
    assert n_out < n_in
    # 1 m tolerance must collapse a sub-metre-detailed circle dramatically.
    assert n_out < n_in // 4


def test_simple_polygon_stays_a_polygon():
    geom = {
        "type": "Polygon",
        "coordinates": [[[0, 0], [0.0001, 0], [0.0001, 0.0001], [0, 0]]],
    }
    out = _simplify_geometry(geom)
    assert out["type"] == "Polygon"


def test_non_polygon_geometry_returned_unchanged():
    geom = {"type": "Point", "coordinates": [-1.6, 42.6]}
    assert _simplify_geometry(geom) is geom


def test_none_returned_unchanged():
    assert _simplify_geometry(None) is None


def test_tolerance_is_one_meter():
    assert CADASTRAL_GEOMETRY_SIMPLIFY_TOLERANCE_M == 1.0
