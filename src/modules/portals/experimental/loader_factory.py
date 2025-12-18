"""
Factory para crear loaders de diferentes portales
"""
from typing import Optional
import asyncpg
from src.core.etl_event_system import PortalType
from src.modules.portals.idealista.load.loader import IdealistaDetectionLoader


async def create_loader(
    portal: PortalType,
    db_pool: asyncpg.Pool,
    batch_size: int = 100,
    enable_dedup: bool = True,
    enable_screenshots: bool = True
):
    """
    Crea un loader para el portal especificado
    
    Args:
        portal: Tipo de portal
        db_pool: Pool de conexiones a PostgreSQL
        batch_size: Tamaño de batch para inserción
        enable_dedup: Activar deduplicación con Redis
        enable_screenshots: Activar captura de screenshots
        
    Returns:
        Loader específico del portal
    """
    if portal == PortalType.IDEALISTA:
        return IdealistaDetectionLoader(
            db_pool=db_pool,
            batch_size=batch_size,
            enable_dedup=enable_dedup,
            enable_screenshots=enable_screenshots
        )
    
    elif portal == PortalType.FOTOCASA:
        raise NotImplementedError(f"Loader para {portal.value} no implementado aún")
    
    elif portal == PortalType.PISOS:
        raise NotImplementedError(f"Loader para {portal.value} no implementado aún")
    
    elif portal == PortalType.HABITACLIA:
        raise NotImplementedError(f"Loader para {portal.value} no implementado aún")
    
    else:
        raise ValueError(f"Portal desconocido: {portal}")