"""
Clase base para loaders de portales inmobiliarios
Sin ORM, acceso directo a PostgreSQL con asyncpg
"""
from typing import Optional
from dataclasses import dataclass
import asyncpg
import os
import logging

logger = logging.getLogger(__name__)


@dataclass
class LoaderStats:
    """Estadísticas del loader"""
    total_processed: int = 0
    new_insertions: int = 0
    duplicates_skipped: int = 0
    errors: int = 0
    
    # Stats extendidas (para loaders específicos)
    evaluated: int = 0
    below_threshold: int = 0
    screenshots_captured: int = 0
    screenshots_failed: int = 0


class PostgresConnectionPool:
    """
    Singleton para gestionar el pool de conexiones PostgreSQL
    """
    _pool: Optional[asyncpg.Pool] = None
    
    @classmethod
    async def get_pool(cls) -> asyncpg.Pool:
        """Obtiene o crea el pool de conexiones"""
        if cls._pool is None:
            # Leer configuración de variables de entorno
            # Por defecto usa 'postgis' (nombre del servicio Docker)
            # Usar 'localhost' solo cuando se ejecuta fuera de Docker
            default_host = os.getenv('POSTGRES_HOST', 'postgis')
            default_port = os.getenv('POSTGRES_PORT', '5432')
            default_user = os.getenv('POSTGRES_USER', 'user')
            default_password = os.getenv('POSTGRES_PASSWORD', 'password')
            default_db = os.getenv('POSTGRES_DB', 'spatialdb')
            
            database_url = os.getenv(
                'DATABASE_URL',
                f'postgresql://{default_user}:{default_password}@{default_host}:{default_port}/{default_db}'
            )
            
            logger.info(f"Conectando a PostgreSQL: {default_host}:{default_port}/{default_db}")
            
            cls._pool = await asyncpg.create_pool(
                database_url,
                min_size=2,
                max_size=10,
                command_timeout=60
            )
            
            logger.info("✓ Pool de conexiones PostgreSQL creado")
        
        return cls._pool
    
    @classmethod
    async def close_pool(cls):
        """Cierra el pool de conexiones"""
        if cls._pool is not None:
            await cls._pool.close()
            cls._pool = None
            logger.info("✓ Pool de conexiones PostgreSQL cerrado")


class BaseLoader:
    """
    Clase base para loaders de diferentes portales
    Acceso directo a PostgreSQL sin ORM
    """
    
    def __init__(
        self,
        db_pool: asyncpg.Pool,
        portal: str,
        batch_size: int = 100,
        enable_dedup: bool = True
    ):
        self.db_pool = db_pool
        self.portal = portal
        self.batch_size = batch_size
        self.enable_dedup = enable_dedup
        
        # Stats
        self.stats = LoaderStats()
        
        # Redis para deduplicación (se inicializa lazy)
        self.redis_cache = None
        self.dedup_ttl_hours = 24
    
    async def _ensure_redis(self):
        """Inicializa Redis si es necesario"""
        if self.redis_cache is None and self.enable_dedup:
            try:
                from shared.redis import RedisCache
                self.redis_cache = RedisCache()
                await self.redis_cache.connect()
                logger.info("✓ Redis conectado para deduplicación")
            except ImportError:
                logger.warning("⚠️  Redis no disponible, deduplicación deshabilitada")
                self.enable_dedup = False
            except Exception as e:
                logger.warning(f"⚠️  Error conectando Redis: {e}, deduplicación deshabilitada")
                self.enable_dedup = False
    
    async def load(self, inmueble):
        """
        Método a implementar por cada portal específico
        
        Args:
            inmueble: Datos del inmueble a cargar
            
        Returns:
            bool: True si se guardó, False si se omitió
        """
        raise NotImplementedError("Subclases deben implementar load()")
    
    async def close(self):
        """Cierra conexiones"""
        if self.redis_cache:
            await self.redis_cache.close()
        
        # No cerramos el pool aquí, se gestiona globalmente
        
        # Log de estadísticas
        logger.info("=" * 60)
        logger.info("ESTADÍSTICAS DE CARGA")
        logger.info("=" * 60)
        logger.info(f"Total procesados: {self.stats.total_processed}")
        logger.info(f"Evaluados: {self.stats.evaluated}")
        logger.info(f"Nuevas inserciones: {self.stats.new_insertions}")
        logger.info(f"Duplicados: {self.stats.duplicates_skipped}")
        logger.info(f"Bajo threshold: {self.stats.below_threshold}")
        logger.info(f"Errores: {self.stats.errors}")
        
        if self.stats.screenshots_captured > 0 or self.stats.screenshots_failed > 0:
            logger.info(f"Screenshots capturados: {self.stats.screenshots_captured}")
            logger.info(f"Screenshots fallidos: {self.stats.screenshots_failed}")
        
        logger.info("=" * 60)