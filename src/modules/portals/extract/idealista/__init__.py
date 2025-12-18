"""
Módulo de extracción de Idealista
Contiene los scrapers y clientes para obtener datos de Idealista
"""

# Exponer el scraper principal
from .idealista_scrape_sync import IdealistaScraper
from .idealista_client import IdealistaClient

__all__ = ['IdealistaScraper','IdealistaClient']