"""
Factory para crear instancias de scrapers según el portal
"""
from typing import Dict, Type
from .base_scraper import BasePortalScraper, ScraperConfig
from ...core.etl_event_system import PortalType

# Registry de scrapers
_SCRAPER_REGISTRY: Dict[PortalType, Type[BasePortalScraper]] = {}


def register_scraper(portal: PortalType):
    """Decorator para registrar un scraper"""
    def decorator(scraper_class: Type[BasePortalScraper]):
        _SCRAPER_REGISTRY[portal] = scraper_class
        return scraper_class
    return decorator


def create_scraper(
    portal: PortalType,
    config: ScraperConfig = None
) -> BasePortalScraper:
    """
    Factory method para crear un scraper según el portal
    
    Args:
        portal: Tipo de portal (IDEALISTA, FOTOCASA, etc.)
        config: Configuración del scraper
        
    Returns:
        Instancia del scraper correspondiente
        
    Raises:
        ValueError: Si el portal no está registrado
    """
    if portal not in _SCRAPER_REGISTRY:
        raise ValueError(
            f"Portal '{portal.value}' no tiene scraper registrado. "
            f"Portales disponibles: {[p.value for p in _SCRAPER_REGISTRY.keys()]}"
        )
    
    scraper_class = _SCRAPER_REGISTRY[portal]
    return scraper_class(config=config)


def get_available_portals() -> list[PortalType]:
    """Retorna lista de portales con scraper implementado"""
    return list(_SCRAPER_REGISTRY.keys())


def is_portal_supported(portal: PortalType) -> bool:
    """Verifica si un portal tiene scraper implementado"""
    return portal in _SCRAPER_REGISTRY