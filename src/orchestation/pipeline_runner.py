"""
Orquestador de pipelines usando el sistema de eventos
"""
import asyncio
import logging
from typing import List, Optional

from core.etl_event_system import ETLEventBus, PortalType, ETLPhase
from portals.load import PostgresConnectionPool
from portals.config import DETECTION_THRESHOLD
from portals.transform import ReligiousPropertyScorer

logger = logging.getLogger(__name__)


class PipelineRunner:
    """
    Ejecuta pipelines ETL con sistema de eventos para observabilidad
    """
    
    def __init__(self, event_bus: ETLEventBus):
        self.event_bus = event_bus
    
    async def run_pipeline(
        self,
        portal: PortalType,
        provincia: str,
        max_pages: int = 5,
        use_db: bool = True
    ):
        """
        Ejecuta pipeline completo con eventos granulares
        
        Emite eventos:
        - phase_start/complete para cada fase ETL
        - scraping_progress durante extract
        - scoring_result durante transform
        - load_result durante load
        - error en caso de fallo
        """
        
        # ================================================================
        # INICIALIZACIÓN
        # ================================================================
        portal_name = portal.value if hasattr(portal, 'value') else str(portal)
        
        logger.info(f"🚀 Iniciando pipeline: {portal_name} - {provincia}")
        
        # Pool PostgreSQL
        db_pool = None
        if use_db:
            db_pool = await PostgresConnectionPool.get_pool()
        
        # Crear componentes
        scraper = self._create_scraper(portal)
        scorer = ReligiousPropertyScorer()
        loader = self._create_loader(portal, db_pool) if db_pool else None
        
        # Compartir driver si existe (para screenshots)
        if loader and hasattr(scraper, 'driver'):
            loader.driver = scraper.driver
        
        total_scrapeados = 0
        total_detectados = 0
        
        try:
            # ================================================================
            # FASE: EXTRACT
            # ================================================================
            await self.event_bus.emit_phase_start(portal, ETLPhase.EXTRACT)
            
            page = 1
            async for inmueble in scraper.scrape_provincia(provincia, max_pages):
                total_scrapeados += 1
                
                # Evento de progreso cada 10 inmuebles
                if total_scrapeados % 10 == 0:
                    await self.event_bus.emit_scraping_progress(
                        portal=portal,
                        provincia=provincia,
                        page=page,
                        items_extracted=total_scrapeados
                    )
                
                # ================================================================
                # FASE: TRANSFORM
                # ================================================================
                await self.event_bus.emit_phase_start(portal, ETLPhase.TRANSFORM)
                
                try:
                    inmueble_dict = {
                        'titulo': inmueble.titulo,
                        'descripcion': inmueble.descripcion,
                        'tipo': inmueble.tipo,
                        'superficie': inmueble.superficie,
                        'lat': inmueble.geo.lat,
                        'lon': inmueble.geo.lon,
                        'caracteristicas_basicas': inmueble.caracteristicas,
                        'caracteristicas_extras': []
                    }
                    
                    score, evidences = scorer.score(inmueble_dict)
                    
                    # Evento de scoring
                    await self.event_bus.emit_scoring_result(
                        portal=portal,
                        inmueble_id=inmueble.id_portal,
                        score=score,
                        evidences=evidences
                    )
                    
                    await self.event_bus.emit_phase_complete(portal, ETLPhase.TRANSFORM)
                
                except Exception as e:
                    await self.event_bus.emit_error(
                        portal=portal,
                        phase=ETLPhase.TRANSFORM,
                        error=str(e),
                        context={'inmueble_id': inmueble.id_portal}
                    )
                    continue
                
                # ================================================================
                # FASE: LOAD
                # ================================================================
                if score >= DETECTION_THRESHOLD:
                    total_detectados += 1
                    
                    if loader:
                        await self.event_bus.emit_phase_start(portal, ETLPhase.LOAD)
                        
                        try:
                            success = await loader.load(inmueble, score, evidences)
                            
                            # Evento de load result
                            await self.event_bus.emit_load_result(
                                portal=portal,
                                inmueble_id=inmueble.id_portal,
                                success=success,
                                reason='saved' if success else 'duplicate or error'
                            )
                            
                            await self.event_bus.emit_phase_complete(portal, ETLPhase.LOAD)
                        
                        except Exception as e:
                            await self.event_bus.emit_error(
                                portal=portal,
                                phase=ETLPhase.LOAD,
                                error=str(e),
                                context={'inmueble_id': inmueble.id_portal}
                            )
            
            await self.event_bus.emit_phase_complete(portal, ETLPhase.EXTRACT)
            
            # Evento de resumen final
            await self.event_bus.emit_pipeline_complete(
                portal=portal,
                total_extracted=total_scrapeados,
                total_detected=total_detectados,
                total_loaded=loader.stats.new_insertions if loader else 0
            )
        
        except Exception as e:
            await self.event_bus.emit_error(
                portal=portal,
                phase=ETLPhase.EXTRACT,
                error=str(e)
            )
            raise
        
        finally:
            await scraper.close()
            if loader:
                await loader.close()
        
        logger.info(f"✅ Pipeline completado: {total_scrapeados} scrapeados, {total_detectados} detectados")
    
    def _create_scraper(self, portal: PortalType):
        """Crea scraper según portal"""
        portal_name = portal.value if hasattr(portal, 'value') else str(portal).lower()
        
        if portal_name == 'idealista':
            from portals.extract.idealista import IdealistaScraper
            return IdealistaScraper(use_selenium=False)
        elif portal_name == 'fotocasa':
            from portals.extract.fotocasa import FotocasaScraper
            return FotocasaScraper(use_selenium=False)
        else:
            raise ValueError(f"Portal desconocido: {portal_name}")
    
    def _create_loader(self, portal: PortalType, db_pool):
        """Crea loader según portal"""
        portal_name = portal.value if hasattr(portal, 'value') else str(portal).lower()
        
        if portal_name == 'idealista':
            from portals.load.idealista import IdealistaLoader
            return IdealistaLoader(db_pool=db_pool)
        elif portal_name == 'fotocasa':
            from portals.load.fotocasa import FotocasaLoader
            return FotocasaLoader(db_pool=db_pool)
        else:
            raise ValueError(f"