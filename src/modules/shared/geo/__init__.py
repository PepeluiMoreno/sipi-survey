"""
Sistema de geocoding compartido
Usado por: portals, osmwikidata
"""
from .nominatim_client import NominatimClient

__all__ = ['NominatimClient']