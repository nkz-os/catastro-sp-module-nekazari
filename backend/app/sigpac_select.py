"""Select SIGPAC enclosures on the map and build one parcel from them.

The map click picks the recinto under the point; several adjacent recintos can be
joined into one parcel. Geometry always comes from FEGA, never from the client.
Constants are named so that a reviewer can find and question them.
"""

from __future__ import annotations

import re
from typing import Optional

from shapely import wkt
from shapely.geometry import mapping
from shapely.ops import transform, unary_union

from app import sigpac_service as svc

# Digitisation gaps between neighbouring recintos are closed up to this width.
GAP_TOLERANCE_M = 0.5
MAX_ENCLOSURES_PER_PARCEL = 20
# A recinto counts as already used when an existing parcel covers this share OF THE RECINTO.
# (Stored overlapPct is the share of the parcel, so it is only a fallback when areas are missing.)
IN_PARCEL_MIN_OVERLAP_PCT = 50.0

_REFERENCE = re.compile(r'^\d{1,2}:\d{1,3}:\d{1,3}:\d{1,3}:\d{1,3}:\d{1,5}:\d{1,5}$')


class NotContiguous(ValueError):
    """The selected recintos do not form one piece of land."""


def parse_reference(ref: str) -> tuple[int, int, int, int, int, int, int]:
    """'pp:mmm:a:z:pol:par:rec' -> seven integers; ValueError on anything else."""
    if not isinstance(ref, str) or not _REFERENCE.match(ref):
        raise ValueError(f'invalid SIGPAC reference: {ref!r}')
    parts = tuple(int(p) for p in ref.split(':'))
    return parts  # type: ignore[return-value]


def normalize_reference(ref: str) -> str:
    """Canonical form, without leading zeros, as stored in sigpacEnclosures."""
    return ':'.join(str(p) for p in parse_reference(ref))


def enclosure_summary(attrs: dict) -> dict:
    """FEGA recinto attributes -> the shape the API and the UI use."""
    return {
        'reference': svc.make_reference(attrs),
        'sigpacUse': attrs.get('uso_sigpac'),
        'areaHa': attrs.get('superficie'),
        'slopePct': attrs.get('pendiente_media'),
        'irrigationCoef': attrs.get('coef_regadio'),
        'geometry': mapping(wkt.loads(attrs['wkt'])),
    }


def _metric(geometries: list[dict]):
    shapes = [svc._to_shape(g) for g in geometries]
    lon = unary_union(shapes).centroid.x
    fwd, inv = svc._transformers(lon)
    return [transform(fwd, s) for s in shapes], inv


def union_enclosures(geometries: list[dict]) -> dict:
    """One Polygon from adjacent recintos, closing gaps up to GAP_TOLERANCE_M."""
    if not geometries:
        raise ValueError('no geometry to join')
    metric, inv = _metric(geometries)
    half = GAP_TOLERANCE_M / 2
    closed = unary_union(metric).buffer(half, join_style='mitre').buffer(-half, join_style='mitre')
    closed = svc._polygonal(closed)
    if closed.geom_type != 'Polygon' or closed.is_empty:
        raise NotContiguous('the selected recintos are not contiguous')
    return mapping(transform(inv, closed))


def area_ha(geometry: dict) -> float:
    metric, _ = _metric([geometry])
    return metric[0].area / 10_000


def parcels_by_enclosure(parcels: list[dict]) -> dict[str, str]:
    """Reference -> URN of an existing parcel that already covers that recinto."""
    taken: dict[str, str] = {}
    for entity in parcels:
        urn = entity.get('id') if isinstance(entity, dict) else None
        for enc in svc.read_enclosures(entity) or []:
            if not isinstance(enc, dict):
                continue
            ref = enc.get('reference')
            share = _enclosure_share_pct(enc)
            if ref and urn and share is not None and share >= IN_PARCEL_MIN_OVERLAP_PCT:
                taken.setdefault(ref, urn)
    return taken


def _enclosure_share_pct(enc: dict) -> Optional[float]:
    """Share of the recinto inside the parcel; falls back to overlapPct without areas."""
    overlap, total = enc.get('overlapAreaHa'), enc.get('enclosureAreaHa')
    if isinstance(overlap, (int, float)) and isinstance(total, (int, float)) and total > 0:
        return overlap / total * 100.0
    pct = enc.get('overlapPct')
    return float(pct) if isinstance(pct, (int, float)) else None


def default_name(references: list[str], name: Optional[str] = None) -> str:
    if name and name.strip():
        return name.strip()
    extra = len(references) - 1
    return references[0] + (f' +{extra}' if extra else '')
