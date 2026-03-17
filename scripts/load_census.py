#!/usr/bin/env python3
"""
Script CLI para cargar datos del censo de inmuebles

Uso:
    python scripts/load_census.py --file /path/to/Madrid.csv
    python scripts/load_census.py --file /path/to/Madrid.csv --dry-run
    python scripts/load_census.py --dir /path/to/census/output --batch-size 200
"""

import asyncio
import argparse
import logging
import sys
import os
from pathlib import Path
from dotenv import load_dotenv

# Cargar .env ANTES de importar sipi-core (que necesita DATABASE_URL)
dotenv_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path)

# Agregar sipi-core al path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "sipi-core" / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from sipi_core.db.sessions import async_session_maker
from modules.census.loader import listado_ceeLoader

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def load_file(file_path: Path, batch_size: int, dry_run: bool):
    """Carga un archivo CSV"""
    logger.info(f"Procesando archivo: {file_path}")

    async with async_session_maker() as session:
        loader = listado_ceeLoader(session)
        stats = await loader.load_from_csv(file_path, batch_size, dry_run)

        logger.info("=" * 60)
        logger.info("RESUMEN DE CARGA")
        logger.info("=" * 60)
        logger.info(f"Total filas CSV: {stats['total_rows']}")
        logger.info(f"Inmuebles creados: {stats['created']}")
        logger.info(f"Filas omitidas: {stats['skipped']}")
        logger.info(f"Errores: {stats['errors']}")
        logger.info(f"Con dependencias: {stats.get('with_dependencies', 0)}")
        logger.info("=" * 60)


async def load_directory(dir_path: Path, batch_size: int, dry_run: bool):
    """Carga todos los archivos CSV de un directorio"""
    csv_files = list(dir_path.glob("*.csv"))

    # Filtrar archivo de estadísticas
    csv_files = [f for f in csv_files if "estadisticas" not in f.name.lower()]

    logger.info(f"Encontrados {len(csv_files)} archivos CSV en {dir_path}")

    total_stats = {
        "total_rows": 0,
        "created": 0,
        "skipped": 0,
        "errors": 0,
        "with_dependencies": 0,
    }

    for csv_file in csv_files:
        logger.info(f"\n{'='*60}")
        logger.info(f"Procesando: {csv_file.name}")
        logger.info(f"{'='*60}")

        async with async_session_maker() as session:
            loader = listado_ceeLoader(session)
            stats = await loader.load_from_csv(csv_file, batch_size, dry_run)

            # Acumular estadísticas
            for key in total_stats:
                total_stats[key] += stats.get(key, 0)

    # Resumen global
    logger.info("\n" + "=" * 60)
    logger.info("RESUMEN GLOBAL - TODOS LOS ARCHIVOS")
    logger.info("=" * 60)
    logger.info(f"Archivos procesados: {len(csv_files)}")
    logger.info(f"Total filas CSV: {total_stats['total_rows']}")
    logger.info(f"Inmuebles creados: {total_stats['created']}")
    logger.info(f"Filas omitidas: {total_stats['skipped']}")
    logger.info(f"Errores: {total_stats['errors']}")
    logger.info(f"Con dependencias: {total_stats['with_dependencies']}")
    logger.info("=" * 60)


async def main():
    parser = argparse.ArgumentParser(
        description="Carga datos del censo de inmuebles desde archivos CSV"
    )
    parser.add_argument(
        "--file", type=Path, help="Ruta al archivo CSV individual"
    )
    parser.add_argument(
        "--dir", type=Path, help="Ruta al directorio con archivos CSV"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Tamaño del lote para commits (default: 100)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simula la carga sin guardar en DB",
    )

    args = parser.parse_args()

    # Validaciones
    if not args.file and not args.dir:
        parser.error("Debe especificar --file o --dir")

    if args.file and args.dir:
        parser.error("Especifique solo --file O --dir, no ambos")

    # Ejecutar
    try:
        if args.file:
            if not args.file.exists():
                logger.error(f"Archivo no encontrado: {args.file}")
                sys.exit(1)
            await load_file(args.file, args.batch_size, args.dry_run)
        else:
            if not args.dir.exists():
                logger.error(f"Directorio no encontrado: {args.dir}")
                sys.exit(1)
            await load_directory(args.dir, args.batch_size, args.dry_run)

        logger.info("\n✅ Proceso completado exitosamente")

    except Exception as e:
        logger.error(f"❌ Error durante la carga: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
