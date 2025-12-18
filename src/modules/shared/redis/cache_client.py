"""
Cliente Redis para caché y deduplicación
"""
import logging
from typing import Optional
import redis.asyncio as aioredis

logger = logging.getLogger(__name__)


class RedisCache:
    """Cliente Redis para caché de detecciones"""
    
    def __init__(self, host: str = 'localhost', port: int = 6379, db: int = 0):
        self.host = host
        self.port = port
        self.db = db
        self.redis: Optional[aioredis.Redis] = None
    
    async def connect(self):
        """Conecta a Redis"""
        if not self.redis:
            self.redis = await aioredis.from_url(
                f"redis://{self.host}:{self.port}/{self.db}",
                encoding="utf-8",
                decode_responses=True
            )
            logger.info(f"✓ Conectado a Redis: {self.host}:{self.port}/{self.db}")
    
    async def check_duplicate(self, portal: str, id_portal: str, ttl_hours: int = 24) -> bool:
        """
        Verifica si un inmueble ya fue procesado
        
        Args:
            portal: Nombre del portal ('idealista', 'fotocasa')
            id_portal: ID del inmueble en el portal
            ttl_hours: TTL del cache en horas
            
        Returns:
            True si es duplicado, False si es nuevo
        """
        await self.connect()
        
        key = f"portal:{portal}:id:{id_portal}"
        exists = await self.redis.exists(key)
        
        if not exists:
            # Marcar como procesado
            await self.redis.setex(key, ttl_hours * 3600, "1")
            return False
        
        return True
    
    async def close(self):
        """Cierra conexión Redis"""
        if self.redis:
            await self.redis.close()
            logger.info("✓ Redis cerrado")