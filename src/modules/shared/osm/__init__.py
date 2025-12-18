"""
Sistema OSM común para todos los portales
"""
from .overpass_client import OverpassClient, OSMChurch
from .osm_matcher import OSMMatcher, OSMMatchResult

__all__ = [
    'OverpassClient',
    'OSMChurch',
    'OSMMatcher',
    'OSMMatchResult'
]