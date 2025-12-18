"""
Fase EXTRACT: Scraping de portales
"""
from .base_scraper import BaseScraper, InmuebleData, GeoData, GeoType
from .base_client import BaseHTTPClient

__all__ = [
    'BaseScraper', 'BaseHTTPClient',
    'InmuebleData', 'GeoData', 'GeoType'
]