"""
Pipeline de orquestación con PostgreSQL pool
"""
import asyncio
import logging
from typing import List, Optional

from portals.load import PostgresConnectionPool
from portals.config import ALL_PROVINCES, DETECTION_THRESHOLD
from portals.transform import ReligiousPropertyScorer

logger = logging.getLogger(__name__)


async def run_portal_pipeline(
    portal: str,
    provincias: List[str] = None,
    max_pages: int = -1,
    use_db: bool = True  # ← Nuevo parámetro
):
    """
    Ejecuta pipeline ETL para un portal
    
    Args:
        portal: 'idealista', 'fotocasa'
        provincias: Lista de provincias (None = todas)
        max_pages: Máximo de páginas por provincia
        use_db: Si False, solo scrapea sin guardar en BD
    """
    provincias = provincias or ALL_PROVINCES
    
    logger.info(f"🚀 Pipeline {portal.upper()}")
    logger.info(f"📍 Provincias: {provincias}")
    logger.info(f"📄 Max páginas: {max_pages if max_pages != -1 else 'ilimitado'}")
    logger.info(f"💾 Guardar en BD: {'Sí' if use_db else 'No'}")
    
    # Crear pool de PostgreSQL si se necesita
    db_pool = None
    if use_db:
        db_pool = await PostgresConnectionPool.get_pool()
    
    # Importar componentes según portal
    portal = portal.lower()
    
    if portal == 'idealista':
        from portals.extract.idealista import IdealistaScraper
        from portals.load.idealista import IdealistaLoader
        
        scraper = IdealistaScraper(use_selenium=False)
        loader = IdealistaLoader(db_pool=db_pool) if db_pool else None
    
    elif portal == 'fotocasa':
        from portals.extract.fotocasa import FotocasaScraper
        from portals.load.fotocasa import FotocasaLoader
        
        scraper = FotocasaScraper(use_selenium=False)
        loader = FotocasaLoader(db_pool=db_pool) if db_pool else None
    
    else:
        raise ValueError(f"Portal desconocido: {portal}")
    
    # Scorer común
    scorer = ReligiousPropertyScorer()
    
    total_scrapeados = 0
    total_detectados = 0
    
    try:
        for provincia in provincias:
            logger.info(f"\n{'='*60}")
            logger.info(f"🔍 SCRAPING: {provincia.upper()}")
            logger.info(f"{'='*60}\n")
            
            async for inmueble in scraper.scrape_provincia(provincia, max_pages):
                total_scrapeados += 1
                
                # Transform
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
                
                # Load
                if score >= DETECTION_THRESHOLD:
                    total_detectados += 1
                    
                    logger.info(f"\n{'─'*60}")
                    logger.info(f"✓ DETECTADO: {inmueble.titulo[:60]}")
                    logger.info(f"📊 Score: {score:.1f}/100")
                    logger.info(f"📍 {inmueble.geo.ciudad}, {inmueble.geo.provincia}")
                    logger.info(f"💰 {inmueble.precio:,.0f} €" if inmueble.precio else "💰 N/D")
                    logger.info(f"\n🔍 Evidencias:")
                    for ev in evidences:
                        logger.info(f"   {ev}")
                    logger.info(f"{'─'*60}\n")
                    
                    if loader:
                        await loader.load(inmueble, score, evidences)
                
                if total_scrapeados % 50 == 0:
                    tasa = 100 * total_detectados / total_scrapeados if total_scrapeados > 0 else 0
                    logger.info(f"\n📊 {total_scrapeados} scrapeados | "
                              f"{total_detectados} detectados ({tasa:.1f}%)\n")
    
    finally:
        await scraper.close()
        if loader:
            await loader.close()
        
        # NO cerramos el pool aquí (es singleton global)
    
    logger.info(f"\n{'='*60}")
    logger.info(f"✅ PIPELINE {portal.upper()} COMPLETADO")
    logger.info(f"{'='*60}")
    logger.info(f"📊 Scrapeados: {total_scrapeados}")
    logger.info(f"🎯 Detectados: {total_detectados}")
    if total_scrapeados > 0:
        logger.info(f"📈 Tasa: {100*total_detectados/total_scrapeados:.1f}%")
    logger.info(f"{'='*60}\n")


async def shutdown():
    """Cierra recursos globales"""
    await PostgresConnectionPool.close_pool()
    logger.info("✓ Recursos liberados")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    try:
        asyncio.run(run_portal_pipeline(
            'idealista',
            provincias=['sevilla'],
            max_pages=2,
            use_db=True
        ))
    finally:
        asyncio.run(shutdown())