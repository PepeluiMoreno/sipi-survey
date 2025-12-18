"""
Loader específico para Idealista
Usa asyncpg directamente sin ORM
"""
import logging
from typing import List
from datetime import datetime

from portals.load import BaseLoader, LoaderStats
from portals.extract import InmuebleData
from portals.config import DETECTION_THRESHOLD

logger = logging.getLogger(__name__)


class IdealistaLoader(BaseLoader):
    """
    Loader para Idealista
    Pipeline: Dedup → Filter by score → Save to PostgreSQL
    """
    
    def __init__(self, db_pool, batch_size: int = 100, enable_dedup: bool = True):
        super().__init__(
            db_pool=db_pool,
            portal='idealista',
            batch_size=batch_size,
            enable_dedup=enable_dedup
        )
    
    async def load(
        self,
        inmueble: InmuebleData,
        score: float,
        evidences: List[str]
    ) -> bool:
        """
        Carga inmueble:
        1. Dedup check
        2. Filter by threshold
        3. Save to PostgreSQL (sin ORM)
        """
        self.stats.total_processed += 1
        self.stats.evaluated += 1
        
        # 1. Dedup check
        if self.enable_dedup:
            await self._ensure_redis()
            
            if self.redis_cache:
                is_duplicate = await self.redis_cache.check_duplicate(
                    self.portal,
                    inmueble.id_portal,
                    self.dedup_ttl_hours
                )
                
                if is_duplicate:
                    self.stats.duplicates_skipped += 1
                    logger.debug(f"Duplicado: {inmueble.id_portal}")
                    return False
        
        # 2. Filter by threshold
        if score < DETECTION_THRESHOLD:
            self.stats.below_threshold += 1
            logger.debug(f"Bajo threshold: {inmueble.titulo[:50]} (score: {score:.1f})")
            return False
        
        # 3. Save to PostgreSQL (INSERT directo con asyncpg)
        try:
            await self._save_to_db(inmueble, score, evidences)
            self.stats.new_insertions += 1
            logger.info(f"✓ Guardado: {inmueble.titulo[:50]} (score: {score:.1f})")
            return True
        
        except Exception as e:
            self.stats.errors += 1
            logger.error(f"Error guardando {inmueble.id_portal}: {e}")
            return False
    
    async def _save_to_db(
        self,
        inmueble: InmuebleData,
        score: float,
        evidences: List[str]
    ):
        """
        Guarda en PostgreSQL usando asyncpg directo (sin ORM)
        """
        query = """
        INSERT INTO detections_idealista (
            id_portal,
            url,
            titulo,
            descripcion,
            tipo,
            precio,
            superficie,
            lat,
            lon,
            geo_type,
            geo_uncertainty_radius_m,
            ciudad,
            provincia,
            caracteristicas,
            imagenes,
            score,
            evidences,
            fecha_scraping,
            fecha_deteccion
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            $11, $12, $13, $14, $15, $16, $17, $18, $19
        )
        ON CONFLICT (id_portal) DO UPDATE SET
            score = EXCLUDED.score,
            evidences = EXCLUDED.evidences,
            fecha_deteccion = EXCLUDED.fecha_deteccion
        """
        
        async with self.db_pool.acquire() as conn:
            await conn.execute(
                query,
                inmueble.id_portal,
                inmueble.url,
                inmueble.titulo,
                inmueble.descripcion,
                inmueble.tipo,
                inmueble.precio,
                inmueble.superficie,
                inmueble.geo.lat,
                inmueble.geo.lon,
                inmueble.geo.type.value,
                inmueble.geo.uncertainty_radius_m,
                inmueble.geo.ciudad,
                inmueble.geo.provincia,
                inmueble.caracteristicas,  # array de strings
                inmueble.imagenes,  # array de strings
                score,
                evidences,  # array de strings
                inmueble.fecha_scraping,
                datetime.now()
            )
        
        logger.debug(f"INSERT/UPDATE en detections_idealista: {inmueble.id_portal}")