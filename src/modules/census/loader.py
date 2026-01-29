"""
CensusLoader - Carga datos del censo de inmuebles desde CSV

Lee archivos CSV del censo de inmatriculaciones y los carga en la base de datos.
Resuelve IDs de entidades relacionadas (municipios, tipos, registros, etc.)
"""

import csv
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, text

# Imports de sipi-core
from sipi_core.db.models.inmuebles import Inmueble, Inmatriculacion
from sipi_core.db.models.actores import Diocesis, RegistroPropiedad
from sipi_core.db.models.geografia import ComunidadAutonoma, Provincia, Municipio
from sipi_core.db.models.tipologias import TipoInmueble, TipoCertificacionPropiedad

from .mapper import listado_ceeMapper

logger = logging.getLogger(__name__)


class listado_ceeLoader:
    """Carga datos del censo de inmuebles desde archivos CSV"""

    def __init__(self, session: AsyncSession):
        self.session = session
        self.mapper = listado_ceeMapper()

        # Caches para evitar queries repetidas
        self._cache_comunidades: Dict[str, str] = {}  # nombre -> id
        self._cache_provincias: Dict[str, str] = {}
        self._cache_municipios: Dict[str, str] = {}
        self._cache_tipos_inmueble: Dict[str, str] = {}
        self._cache_registros: Dict[str, str] = {}
        self._cache_diocesis: Dict[str, str] = {}

        self.stats = {
            "total_rows": 0,
            "created": 0,
            "skipped": 0,
            "errors": 0,
        }

    async def load_from_csv(
        self, csv_path: Path, batch_size: int = 100, dry_run: bool = False
    ) -> Dict[str, int]:
        """
        Carga datos desde un archivo CSV

        Args:
            csv_path: Ruta al archivo CSV
            batch_size: Tamaño del lote para commits
            dry_run: Si es True, no guarda en DB (solo simula)

        Returns:
            Diccionario con estadísticas de la carga
        """
        logger.info(f"Iniciando carga desde {csv_path}")
        logger.info(f"Modo: {'DRY RUN' if dry_run else 'PRODUCCIÓN'}")

        # Set search path to include sipi schema
        await self.session.execute(text("SET search_path TO sipi, portals, public"))

        # Precarga caches
        await self._load_caches()

        batch = []
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)

            for row in reader:
                self.stats["total_rows"] += 1

                # Mapear datos
                mapped_data = self.mapper.map_row(row)
                if not mapped_data:
                    self.stats["skipped"] += 1
                    continue

                # Resolver IDs
                try:
                    inmueble_data = await self._resolve_ids(mapped_data)
                    batch.append(inmueble_data)

                    # Commit por lotes
                    if len(batch) >= batch_size:
                        if not dry_run:
                            await self._save_batch(batch)
                        self.stats["created"] += len(batch)
                        batch = []

                except Exception as e:
                    logger.error(f"Error resolviendo IDs: {e}. Fila: {row}")
                    self.stats["errors"] += 1

        # Guardar lote final
        if batch:
            if not dry_run:
                await self._save_batch(batch)
            self.stats["created"] += len(batch)

        logger.info(f"Carga completada: {self.stats}")
        return self.stats

    async def _load_caches(self):
        """Precarga caches de entidades relacionadas"""
        logger.info("Precargando caches...")

        # Comunidades Autónomas
        result = await self.session.execute(select(ComunidadAutonoma))
        for ca in result.scalars():
            self._cache_comunidades[ca.nombre.lower()] = ca.id

        # Provincias
        result = await self.session.execute(select(Provincia))
        for prov in result.scalars():
            self._cache_provincias[prov.nombre.lower()] = prov.id

        # Municipios (con índice compuesto: provincia+municipio)
        result = await self.session.execute(select(Municipio))
        for mun in result.scalars():
            # Key: "provincia_id:nombre_municipio"
            key = f"{mun.provincia_id}:{mun.nombre.lower()}"
            self._cache_municipios[key] = mun.id

        # Tipos de Inmueble
        result = await self.session.execute(select(TipoInmueble))
        for tipo in result.scalars():
            self._cache_tipos_inmueble[tipo.nombre.lower()] = tipo.id

        # Registros de Propiedad
        result = await self.session.execute(select(RegistroPropiedad))
        for reg in result.scalars():
            self._cache_registros[reg.nombre.lower()] = reg.id

        # Diócesis
        result = await self.session.execute(select(Diocesis))
        for dioc in result.scalars():
            self._cache_diocesis[dioc.nombre.lower()] = dioc.id

        logger.info(
            f"Caches cargados: {len(self._cache_comunidades)} CAs, "
            f"{len(self._cache_provincias)} provincias, "
            f"{len(self._cache_municipios)} municipios, "
            f"{len(self._cache_tipos_inmueble)} tipos, "
            f"{len(self._cache_registros)} registros"
        )

    async def _resolve_ids(self, mapped_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Resuelve nombres a IDs de entidades relacionadas

        Args:
            mapped_data: Datos mapeados del CSV

        Returns:
            Diccionario con IDs resueltos
        """
        inmueble = mapped_data["inmueble"]
        inmatriculacion = mapped_data["inmatriculacion"]
        metadata = mapped_data["metadata"]

        # Resolver Comunidad Autónoma
        ca_name = inmueble.get("comunidad_autonoma_name", "").lower()
        ca_id = self._cache_comunidades.get(ca_name)

        # Resolver Provincia
        prov_name = inmueble.get("provincia_name", "").lower()
        prov_id = self._cache_provincias.get(prov_name)

        # Resolver Municipio (requiere provincia)
        mun_id = None
        if prov_id:
            mun_name = inmueble.get("municipio_name", "").lower()
            mun_key = f"{prov_id}:{mun_name}"
            mun_id = self._cache_municipios.get(mun_key)

        # Resolver Tipo de Inmueble
        tipo_name = inmueble.get("tipo_inmueble_name", "").lower()
        tipo_id = self._cache_tipos_inmueble.get(tipo_name)

        # Si el tipo no existe, crear uno nuevo (auto-discovery)
        if not tipo_id and tipo_name:
            tipo_id = await self._get_or_create_tipo_inmueble(tipo_name)

        # Resolver Registro de Propiedad
        reg_name = inmatriculacion.get("registro_propiedad_name", "").lower()
        reg_id = self._cache_registros.get(reg_name)

        # Resolver Diócesis desde metadata
        diocesis_name = metadata.get("titular_name", "").lower()
        diocesis_id = None
        if any(keyword in diocesis_name for keyword in ["diocesis", "obispado", "arzobispado"]):
            diocesis_id = self._cache_diocesis.get(diocesis_name)

        # Construir datos finales
        return {
            "inmueble": {
                "nombre": inmueble["nombre"],
                "descripcion": inmueble["descripcion"],
                "comunidad_autonoma_id": ca_id,
                "provincia_id": prov_id,
                "municipio_id": mun_id,
                "tipo_inmueble_id": tipo_id,
                "diocesis_id": diocesis_id,
                "estado_ciclo_vida": inmueble["estado_ciclo_vida"],
                "geo_quality": inmueble["geo_quality"],
                "activo": inmueble["activo"],
            },
            "inmatriculacion": {
                "registro_propiedad_id": reg_id,
                "tiene_dependencias": inmatriculacion["tiene_dependencias"],
                "observaciones": inmatriculacion.get("observaciones"),
            },
        }

    async def _get_or_create_tipo_inmueble(self, nombre: str) -> str:
        """Crea tipo de inmueble si no existe"""
        # Truncar si es necesario (max 100 caracteres)
        nombre_truncated = nombre[:100] if len(nombre) > 100 else nombre

        # Buscar existente
        result = await self.session.execute(
            select(TipoInmueble).where(TipoInmueble.nombre == nombre_truncated.title())
        )
        tipo = result.scalar_one_or_none()

        if not tipo:
            # Crear nuevo
            tipo = TipoInmueble(nombre=nombre_truncated.title())
            self.session.add(tipo)
            await self.session.flush()
            logger.info(f"Tipo de inmueble creado: {nombre_truncated}")

        # Actualizar cache
        self._cache_tipos_inmueble[nombre.lower()] = tipo.id
        return tipo.id

    async def _save_batch(self, batch: List[Dict[str, Any]]):
        """Guarda un lote de inmuebles en la base de datos"""
        for data in batch:
            # Crear Inmueble
            inmueble = Inmueble(**data["inmueble"])
            self.session.add(inmueble)
            await self.session.flush()  # Necesario para obtener inmueble.id

            # Crear Inmatriculacion
            inmat_data = data["inmatriculacion"]
            inmat_data["inmueble_id"] = inmueble.id
            inmatriculacion = Inmatriculacion(**inmat_data)
            self.session.add(inmatriculacion)

        await self.session.commit()
        logger.info(f"Batch guardado: {len(batch)} inmuebles")

    def get_stats(self) -> Dict[str, int]:
        """Retorna estadísticas de la carga"""
        return {**self.stats, **self.mapper.get_stats()}
