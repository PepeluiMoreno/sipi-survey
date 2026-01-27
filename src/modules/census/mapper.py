"""
CensusMapper - Mapea columnas CSV del censo a modelo Inmueble

Estrategia: Opción 2 (Conservadora)
- Crea un único registro de Inmueble con el título completo
- Marca inmatriculacion.tiene_dependencias = True cuando aplique
- NO desglosa automáticamente (desglose manual en fase posterior)
"""

from typing import Dict, Any, Optional
from datetime import datetime
import logging

from sipi.db.models.inmuebles import EstadoCicloVida, GeoQuality

logger = logging.getLogger(__name__)


class listado_ceeMapper:
    """Mapea datos del censo CSV a modelo de base de datos"""

    # Mapeo de columnas CSV a campos del modelo
    CSV_COLUMNS = {
        "Comunidad Autónoma": "comunidad_autonoma_name",
        "Provincia": "provincia_name",
        "REGISTRO": "registro_propiedad_name",
        "Municipio": "municipio_name",
        "Titulo": "titulo",
        "Tipo": "tipo_inmueble_name",
        "Templo y dependencias complementarias": "tiene_dependencias",
        "Titular": "titular_name",
        "Título distinto de certificación eclesiástica": "certificacion_no_eclesiastica",
    }

    def __init__(self):
        self.stats = {
            "processed": 0,
            "with_dependencies": 0,
            "errors": 0,
        }

    def map_row(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Mapea una fila del CSV a estructura de datos para crear Inmueble + Inmatriculacion

        Args:
            row: Diccionario con datos de una fila CSV

        Returns:
            Diccionario con datos mapeados o None si hay error
        """
        try:
            # Extraer datos básicos
            titulo = row.get("Titulo", "").strip()
            tipo = row.get("Tipo", "").strip()
            tiene_deps_str = row.get("Templo y dependencias complementarias", "False")

            # Convertir booleano
            tiene_dependencias = self._parse_boolean(tiene_deps_str)

            # Estructura de datos mapeada
            mapped_data = {
                "inmueble": {
                    "nombre": titulo if titulo else tipo,  # Nombre = Titulo, fallback a Tipo
                    "descripcion": f"Tipo: {tipo}. Titular: {row.get('Titular', 'N/A')}",
                    # Campos de ubicación (por ahora nombres, luego resolveremos IDs)
                    "comunidad_autonoma_name": row.get("Comunidad Autónoma", "").strip(),
                    "provincia_name": row.get("Provincia", "").strip(),
                    "municipio_name": row.get("Municipio", "").strip(),
                    # Tipo (por ahora nombre, luego resolveremos ID)
                    "tipo_inmueble_name": tipo,
                    # Estado inicial
                    "estado_ciclo_vida": EstadoCicloVida.INMATRICULADO,
                    "geo_quality": GeoQuality.MISSING,  # Sin coordenadas inicialmente
                    "activo": True,
                },
                "inmatriculacion": {
                    "registro_propiedad_name": row.get("REGISTRO", "").strip(),
                    "tiene_dependencias": tiene_dependencias,
                    # Datos adicionales en observaciones
                    "observaciones": self._build_observaciones(row),
                },
                "metadata": {
                    "titular_name": row.get("Titular", "").strip(),
                    "certificacion_no_eclesiastica": self._parse_boolean(
                        row.get("Título distinto de certificación eclesiástica", "False")
                    ),
                },
            }

            self.stats["processed"] += 1
            if tiene_dependencias:
                self.stats["with_dependencies"] += 1

            return mapped_data

        except Exception as e:
            logger.error(f"Error mapeando fila: {e}. Fila: {row}")
            self.stats["errors"] += 1
            return None

    def _parse_boolean(self, value: str) -> bool:
        """Convierte string a boolean"""
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in ("true", "1", "sí", "si", "yes")
        return False

    def _build_observaciones(self, row: Dict[str, Any]) -> str:
        """Construye campo observaciones con metadata del CSV"""
        obs_parts = []

        titular = row.get("Titular", "").strip()
        if titular:
            obs_parts.append(f"Titular: {titular}")

        registro = row.get("REGISTRO", "").strip()
        if registro:
            obs_parts.append(f"Registro: {registro}")

        cert_no_ecle = row.get("Título distinto de certificación eclesiástica", "False")
        if self._parse_boolean(cert_no_ecle):
            obs_parts.append("Certificación NO eclesiástica")

        return " | ".join(obs_parts) if obs_parts else None

    def get_stats(self) -> Dict[str, int]:
        """Retorna estadísticas de mapeo"""
        return self.stats.copy()

    def reset_stats(self):
        """Resetea estadísticas"""
        self.stats = {
            "processed": 0,
            "with_dependencies": 0,
            "errors": 0,
        }
