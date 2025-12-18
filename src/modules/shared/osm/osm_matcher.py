"""
Matcher OSM genérico para correlacionar inmuebles con iglesias
COMÚN para todos los portales
"""
from typing import Optional, Dict, Any
import logging
from .overpass_client import OSMChurch

logger = logging.getLogger(__name__)


class OSMMatchResult:
    """Resultado de matching con OSM"""
    
    def __init__(self, osm_church: OSMChurch, confidence: float):
        self.osm_church = osm_church
        self.confidence = confidence
    
    def __repr__(self):
        return f"OSMMatchResult(church='{self.osm_church.name}', confidence={self.confidence:.1f}%)"


class OSMMatcher:
    """
    Correlaciona inmuebles con iglesias de OSM
    Genérico para cualquier portal
    """
    
    def find_match(
        self,
        inmueble: Dict[str, Any],
        osm_churches: list
    ) -> Optional[OSMMatchResult]:
        """
        Busca el mejor match entre un inmueble y las iglesias OSM
        
        Args:
            inmueble: Diccionario con 'titulo', 'lat', 'lon'
            osm_churches: Lista de OSMChurch cercanas
            
        Returns:
            OSMMatchResult o None
        """
        if not osm_churches:
            return None
        
        titulo = (inmueble.get('titulo') or '').lower()
        
        # 1. Match exacto por nombre
        for church in osm_churches:
            church_name = church.name.lower()
            
            if church_name in titulo or titulo in church_name:
                logger.info(f"✓ Match exacto: '{church.name}' en título")
                return OSMMatchResult(
                    osm_church=church,
                    confidence=95.0
                )
        
        # 2. Iglesia muy cercana (< 50m) = alta confianza
        closest = osm_churches[0]
        if closest.distance < 50:
            logger.info(f"✓ Iglesia muy cercana: '{closest.name}' a {closest.distance:.0f}m")
            return OSMMatchResult(
                osm_church=closest,
                confidence=80.0
            )
        
        # 3. Iglesia cercana (< 150m) = confianza media
        if closest.distance < 150:
            logger.info(f"✓ Iglesia cercana: '{closest.name}' a {closest.distance:.0f}m")
            return OSMMatchResult(
                osm_church=closest,
                confidence=60.0
            )
        
        # 4. Fuera de rango confiable
        logger.debug(f"Iglesia más cercana: '{closest.name}' a {closest.distance:.0f}m (baja confianza)")
        return None