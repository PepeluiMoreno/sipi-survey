"""
Pipeline ETL completo: Extract → Score → Save + Screenshot
"""
import asyncio
import argparse
from src.modules.portals.load.base_loader import PostgresConnectionPool
from src.modules.portals.loader_factory import create_loader
from src.modules.portals.factory import create_scraper
from src.core.etl_event_system import PortalType


async def run_pipeline_with_screenshots(
    portal: str = "idealista",
    provincia: str = "sevilla",
    max_pages: int = 2,
    batch_size: int = 100,
    enable_dedup: bool = True,
    enable_screenshots: bool = True
):
    """
    Pipeline completo con screenshots
    """
    print(f"=== ETL Pipeline: {portal.upper()} - {provincia.upper()} ===\n")
    print(f"Configuración:")
    print(f"  - Max páginas: {max_pages}")
    print(f"  - Batch size: {batch_size}")
    print(f"  - Dedup: {'✓' if enable_dedup else '✗'}")
    print(f"  - Screenshots: {'✓' if enable_screenshots else '✗'}\n")
    
    # Setup
    db_pool = await PostgresConnectionPool.get_pool()
    
    # Crear scraper
    portal_enum = PortalType[portal.upper()]
    scraper = create_scraper(portal_enum)
    
    # Crear loader
    loader = await create_loader(
        portal_enum,
        db_pool=db_pool,
        batch_size=batch_size,
        enable_dedup=enable_dedup
    )
    
    # ✅ Compartir driver para screenshots
    if enable_screenshots:
        loader.driver = scraper.driver
    
    print(f"[1/2] Scraping {provincia}...")
    
    count = 0
    async for inmueble in scraper.scrape_provincia(provincia, max_pages=max_pages):
        loaded = await loader.load(inmueble)
        count += 1
        
        if count % 10 == 0:
            print(f"  Procesados: {count}, Detectados: {loader.stats.new_insertions}")
    
    await loader.close()
    
    print(f"\n[2/2] Resultados:")
    print(f"  Total procesados: {loader.stats.total_processed}")
    print(f"  Evaluados: {loader.stats.evaluated}")
    print(f"  Detectados (guardados): {loader.stats.new_insertions}")
    print(f"  Descartados (score bajo): {loader.stats.below_threshold}")
    
    if enable_screenshots:
        print(f"  Screenshots capturados: {loader.stats.screenshots_captured}")
        print(f"  Screenshots fallidos: {loader.stats.screenshots_failed}")
    
    if enable_dedup:
        print(f"  Duplicados: {loader.stats.duplicates_skipped}")
    
    await PostgresConnectionPool.close_pool()


def main():
    parser = argparse.ArgumentParser(description='Pipeline ETL para portales inmobiliarios')
    
    parser.add_argument(
        '--portal',
        type=str,
        default='idealista',
        choices=['idealista', 'fotocasa', 'pisos', 'habitaclia'],
        help='Portal a scrapear'
    )
    
    parser.add_argument(
        '--provincia',
        type=str,
        default='sevilla',
        help='Provincia a scrapear (ej: sevilla, madrid, barcelona)'
    )
    
    parser.add_argument(
        '--max-pages',
        type=int,
        default=2,
        help='Número máximo de páginas a procesar'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        default=100,
        help='Tamaño de batch para inserción en BD'
    )
    
    parser.add_argument(
        '--no-dedup',
        action='store_true',
        help='Deshabilitar deduplicación (Redis)'
    )
    
    parser.add_argument(
        '--no-screenshots',
        action='store_true',
        help='Deshabilitar captura de screenshots'
    )
    
    args = parser.parse_args()
    
    asyncio.run(run_pipeline_with_screenshots(
        portal=args.portal,
        provincia=args.provincia,
        max_pages=args.max_pages,
        batch_size=args.batch_size,
        enable_dedup=not args.no_dedup,
        enable_screenshots=not args.no_screenshots
    ))


if __name__ == '__main__':
    main()