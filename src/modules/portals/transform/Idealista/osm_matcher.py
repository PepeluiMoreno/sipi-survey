"""
Matcher OSM para correlacionar inmuebles con iglesias
"""
from typing import Optional, Dict, Any
from ..overpass_queries import OverpassClient, OSMChurch
from src.core.config import config


class OSMMatchResult:
    """Resultado de matching con OSM"""
    
    def __init__(self, osm_church: OSMChurch, confidence: float):
        self.osm_church = osm_church
        self.confidence = confidence


class IdealistaOSMMatcher:
    """
    Correlaciona inmuebles de Idealista con iglesias de OSM
    """
    
    def __init__(self):
        self.overpass_client = OverpassClient()
        self.weights = config.scoring['weights']
    
    def find_match(
        self,
        inmueble: Dict[str, Any],
        osm_churches: list
    ) -> Optional[OSMMatchResult]:
        """
        Busca el mejor match entre un inmueble y las iglesias OSM
        
        Args:
            inmueble: Datos del inmueble
            osm_churches: Lista de iglesias OSM cercanas
            
        Returns:
            OSMMatchResult o None
        """
        if not osm_churches:
            return None
        
        titulo = (inmueble.get('titulo') or '').lower()
        
        # Buscar match exacto por nombre
        for church in osm_churches:
            church_name = church.name.lower()
            
            # Match exacto
            if church_name in titulo or titulo in church_name:
                return OSMMatchResult(
                    osm_church=church,
                    confidence=95.0
                )
        
        # Si hay iglesia muy cercana (< 50m), alta confianza
        closest = osm_churches[0]
        if closest.distance < 50:
            return OSMMatchResult(
                osm_church=closest,
                confidence=80.0
            )
        
        # Iglesia cercana (< 150m), confianza media
        if closest.distance < 150:
            return OSMMatchResult(
                osm_church=closest,
                confidence=60.0
            )
        
        return None