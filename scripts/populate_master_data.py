#!/usr/bin/env python3
"""
Script para poblar tablas maestras extrayendo datos únicos del censo CSV

Extrae y carga:
- Comunidades Autónomas
- Provincias
- Municipios
- Tipos de Inmueble
- Registros de Propiedad

Uso:
    python scripts/populate_master_data.py --census-dir /path/to/census/output
"""

import asyncio
import argparse
import logging
import sys
import csv
from pathlib import Path
from typing import Set, Dict, Tuple
from dotenv import load_dotenv

# Cargar .env ANTES de importar sipi-core
dotenv_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path)

# Agregar sipi-core al path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "sipi-core" / "src"))

from sipi_core.db.sessions import async_session_maker
from sipi_core.db.models.geografia import ComunidadAutonoma, Provincia, Municipio
from sipi_core.db.models.tipologias import TipoInmueble
from sipi_core.db.models.actores import RegistroPropiedad
from sqlalchemy import select, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def extract_master_data_from_csvs(census_dir: Path) -> Dict[str, Set]:
    """Extrae datos únicos de todos los CSVs del censo"""

    csv_files = list(census_dir.glob("*.csv"))
    csv_files = [f for f in csv_files if "estadisticas" not in f.name.lower()]

    logger.info(f"Analizando {len(csv_files)} archivos CSV...")

    comunidades = set()
    provincias = set()  # (comunidad, provincia)
    municipios = set()  # (provincia, municipio)
    tipos_inmueble = set()
    registros = set()

    for csv_file in csv_files:
        logger.info(f"Procesando {csv_file.name}...")

        with open(csv_file, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)

            for row in reader:
                ca = row.get("Comunidad Autónoma", "").strip()
                prov = row.get("Provincia", "").strip()
                mun = row.get("Municipio", "").strip()
                tipo = row.get("Tipo", "").strip()
                registro = row.get("REGISTRO", "").strip()

                if ca:
                    comunidades.add(ca)
                if ca and prov:
                    provincias.add((ca, prov))
                if prov and mun:
                    municipios.add((prov, mun))
                if tipo:
                    tipos_inmueble.add(tipo)
                if registro:
                    registros.add(registro)

    logger.info(f"Extraídos: {len(comunidades)} CAs, {len(provincias)} provincias, "
                f"{len(municipios)} municipios, {len(tipos_inmueble)} tipos, {len(registros)} registros")

    return {
        "comunidades": comunidades,
        "provincias": provincias,
        "municipios": municipios,
        "tipos_inmueble": tipos_inmueble,
        "registros": registros,
    }


async def populate_comunidades_autonomas(session, comunidades: Set[str]) -> Dict[str, str]:
    """Pobla tabla comunidades_autonomas y retorna mapa nombre->id"""
    logger.info(f"Creando {len(comunidades)} comunidades autónomas...")

    ca_map = {}

    # Obtener CAs existentes
    result = await session.execute(select(ComunidadAutonoma))
    for ca in result.scalars():
        ca_map[ca.nombre] = ca.id

    nuevas = 0
    for nombre in sorted(comunidades):
        if nombre not in ca_map:
            ca = ComunidadAutonoma(nombre=nombre)
            session.add(ca)
            await session.flush()
            ca_map[nombre] = ca.id
            nuevas += 1
            logger.debug(f"  - {nombre}: {ca.id}")

    await session.commit()
    logger.info(f"✅ {nuevas} comunidades autónomas creadas ({len(ca_map) - nuevas} ya existían)")
    return ca_map


async def populate_provincias(session, provincias: Set[Tuple], ca_map: Dict) -> Dict[str, str]:
    """Pobla tabla provincias y retorna mapa nombre->id"""
    logger.info(f"Creando {len(provincias)} provincias...")

    prov_map = {}

    # Obtener provincias existentes
    result = await session.execute(select(Provincia))
    for prov in result.scalars():
        prov_map[prov.nombre] = prov.id

    nuevas = 0
    for ca_nombre, prov_nombre in sorted(provincias):
        if prov_nombre in prov_map:
            continue

        ca_id = ca_map.get(ca_nombre)
        if not ca_id:
            logger.warning(f"CA no encontrada: {ca_nombre}")
            continue

        prov = Provincia(
            nombre=prov_nombre,
            comunidad_autonoma_id=ca_id
        )
        session.add(prov)
        await session.flush()
        prov_map[prov_nombre] = prov.id
        nuevas += 1
        logger.debug(f"  - {prov_nombre} ({ca_nombre}): {prov.id}")

    await session.commit()
    logger.info(f"✅ {nuevas} provincias creadas ({len(prov_map) - nuevas} ya existían)")
    return prov_map


async def populate_municipios(session, municipios: Set[Tuple], prov_map: Dict):
    """Pobla tabla municipios"""
    logger.info(f"Creando {len(municipios)} municipios...")

    # Obtener mapa provincia_id -> comunidad_autonoma_id
    prov_to_ca = {}
    result = await session.execute(select(Provincia))
    for prov in result.scalars():
        prov_to_ca[prov.id] = prov.comunidad_autonoma_id

    # Obtener municipios existentes
    result = await session.execute(select(Municipio.nombre, Municipio.provincia_id))
    existing = {(row[0], row[1]) for row in result}

    count = 0
    batch_size = 100
    batch = []

    for prov_nombre, mun_nombre in sorted(municipios):
        prov_id = prov_map.get(prov_nombre)
        if not prov_id:
            logger.warning(f"Provincia no encontrada: {prov_nombre}")
            continue

        ca_id = prov_to_ca.get(prov_id)
        if not ca_id:
            logger.warning(f"CA no encontrada para provincia: {prov_nombre}")
            continue

        # Truncar nombres largos (max 150 caracteres)
        mun_nombre_truncated = mun_nombre[:150] if len(mun_nombre) > 150 else mun_nombre

        # Skip if already exists
        if (mun_nombre_truncated, prov_id) in existing:
            continue

        mun = Municipio(
            nombre=mun_nombre_truncated,
            provincia_id=prov_id,
            comunidad_autonoma_id=ca_id
        )
        batch.append(mun)

        if len(batch) >= batch_size:
            session.add_all(batch)
            await session.commit()
            count += len(batch)
            logger.info(f"  Creados {count} municipios nuevos...")
            batch = []

    # Guardar lote final
    if batch:
        session.add_all(batch)
        await session.commit()
        count += len(batch)

    logger.info(f"✅ {count} municipios creados ({len(existing)} ya existían)")


async def populate_tipos_inmueble(session, tipos: Set[str]):
    """Pobla tabla tipos_inmueble"""
    logger.info(f"Creando {len(tipos)} tipos de inmueble...")

    # Obtener tipos existentes
    result = await session.execute(select(TipoInmueble.nombre))
    existing = {row[0] for row in result}

    # Preparar nuevos tipos (sin duplicados internos)
    tipos_unicos = set()
    for nombre in tipos:
        # Truncar nombres largos (max 100 caracteres)
        nombre_truncated = nombre[:100] if len(nombre) > 100 else nombre
        nombre_title = nombre_truncated.title()
        tipos_unicos.add(nombre_title)

    # Filtrar los que ya existen
    tipos_nuevos = tipos_unicos - existing

    # Insertar nuevos
    nuevos = 0
    for nombre_title in sorted(tipos_nuevos):
        tipo = TipoInmueble(nombre=nombre_title)
        session.add(tipo)
        nuevos += 1

    await session.commit()
    logger.info(f"✅ {nuevos} tipos de inmueble creados ({len(existing)} ya existían)")


async def populate_registros_propiedad(session, registros: Set[str]):
    """Pobla tabla registros_propiedad"""
    logger.info(f"Creando {len(registros)} registros de propiedad...")

    # Obtener registros existentes
    result = await session.execute(select(RegistroPropiedad.nombre))
    existing = {row[0] for row in result}

    # Preparar nuevos registros (sin duplicados internos)
    registros_unicos = set()
    for nombre in registros:
        # Truncar nombres largos (max 255 caracteres)
        nombre_truncated = nombre[:255] if len(nombre) > 255 else nombre
        registros_unicos.add(nombre_truncated)

    # Filtrar los que ya existen
    registros_nuevos = registros_unicos - existing

    # Insertar nuevos
    nuevos = 0
    for nombre_truncated in sorted(registros_nuevos):
        registro = RegistroPropiedad(nombre=nombre_truncated)
        session.add(registro)
        nuevos += 1

    await session.commit()
    logger.info(f"✅ {nuevos} registros de propiedad creados ({len(existing)} ya existían)")


async def main():
    parser = argparse.ArgumentParser(
        description="Pobla tablas maestras desde archivos CSV del censo"
    )
    parser.add_argument(
        "--census-dir",
        type=Path,
        required=True,
        help="Directorio con archivos CSV del censo"
    )

    args = parser.parse_args()

    if not args.census_dir.exists():
        logger.error(f"Directorio no encontrado: {args.census_dir}")
        sys.exit(1)

    try:
        # Extraer datos únicos
        logger.info("="*60)
        logger.info("EXTRACCIÓN DE DATOS ÚNICOS")
        logger.info("="*60)
        master_data = await extract_master_data_from_csvs(args.census_dir)

        # Poblar base de datos
        logger.info("\n" + "="*60)
        logger.info("POBLACIÓN DE TABLAS MAESTRAS")
        logger.info("="*60)

        async with async_session_maker() as session:
            # Set search path to include sipi schema
            await session.execute(text("SET search_path TO sipi, portals, public"))

            # 1. Comunidades Autónomas
            ca_map = await populate_comunidades_autonomas(
                session, master_data["comunidades"]
            )

            # 2. Provincias
            prov_map = await populate_provincias(
                session, master_data["provincias"], ca_map
            )

            # 3. Municipios
            await populate_municipios(
                session, master_data["municipios"], prov_map
            )

            # 4. Tipos de Inmueble
            await populate_tipos_inmueble(
                session, master_data["tipos_inmueble"]
            )

            # 5. Registros de Propiedad
            await populate_registros_propiedad(
                session, master_data["registros"]
            )

        logger.info("\n" + "="*60)
        logger.info("✅ POBLACIÓN COMPLETADA EXITOSAMENTE")
        logger.info("="*60)

    except Exception as e:
        logger.error(f"❌ Error durante la población: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
