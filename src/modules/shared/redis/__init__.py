"""
Sistema de caché Redis compartido
Usado por: portals (deduplicación), orchestration
"""
from .cache_client import RedisCache

__all__ = ['RedisCache']