"""Region routing tests for cadastral building queries."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'app'))

from region_router import get_region, get_region_for_bbox


def test_allotarra_point_is_navarra():
    # Allo, Navarra — approximate centre
    assert get_region(42.57, -2.12) == 'navarra'


def test_pamplona_point_is_navarra():
    assert get_region(42.816, -1.644) == 'navarra'


def test_vitoria_point_is_euskadi():
    assert get_region(42.85, -2.67) == 'euskadi'


def test_navarra_bbox_not_misclassified_as_euskadi_when_centre_in_euskadi():
    # Bbox spanning Navarra south + north Euskadi: any Navarra corner wins.
    bbox = (-2.20, 42.50, -1.90, 43.00)  # west,south,east,north
    assert get_region_for_bbox(bbox) == 'navarra'
