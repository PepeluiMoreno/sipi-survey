"""
Cliente Overpass API para búsqueda de edificios religiosos
COMÚN para todos los portales

Basado en el query de osmwikidata/extract/queries/churches.overpassql
Detecta:
- Iglesias, catedrales, capillas
- Monasterios, conventos
- Ermitas, basílicas
- Cruces, humilladeros, grottos
"""
import requests
import logging
from typing import List, Optional
from math import radians, cos, sin, asin, sqrt

logger = logging.getLogger(__name__)


class OSMChurch:
    """Representa un edificio religioso de OSM"""
    
    def __init__(
        self,
        osm_id: int,
        osm_type: str,
        name: str,
        lat: float,
        lon: float,
        distance: float = 0,
        tags: dict = None
    ):
        self.osm_id = osm_id
        self.osm_type = osm_type
        self.name = name
        self.lat = lat
        self.lon = lon
        self.distance = distance
        self.tags = tags or {}
        
        # Extraer tipo de edificio
        self.building_type = self._extract_building_type()
        self.denomination = tags.get('denomination', 'unknown') if tags else 'unknown'
    
    def _extract_building_type(self) -> str:
        """Extrae el tipo de edificio de los tags"""
        if not self.tags:
            return 'unknown'
        
        # Prioridad: building > amenity > place_of_worship
        building = self.tags.get('building', '')
        amenity = self.tags.get('amenity', '')
        place = self.tags.get('place_of_worship', '')
        
        if building in ['church', 'cathedral', 'chapel', 'monastery', 'convent', 'hermitage', 'basilica']:
            return building
        elif amenity == 'place_of_worship':
            return 'place_of_worship'
        elif place in ['cross', 'wayside_shrine', 'lourdes_grotto']:
            return place
        else:
            return 'unknown'
    
    def __repr__(self):
        return f"OSMChurch(name='{self.name}', type='{self.building_type}', distance={self.distance:.0f}m)"


class OverpassClient:
    """
    Cliente para Overpass API (OpenStreetMap)
    Busca edificios religiosos católicos cerca de un punto
    """
    
    OVERPASS_URL = "https://overpass-api.de/api/interpreter"
    TIMEOUT = 25
    
    def find_churches_nearby(
        self,
        lat: float,
        lon: float,
        radius_m: int
    ) -> List[OSMChurch]:
        """
        Busca edificios religiosos católicos en un radio determinado
        
        Detecta:
        - Lugares de culto católicos (amenity=place_of_worship + denomination=catholic)
        - Edificios religiosos: iglesia, catedral, capilla, monasterio, convento, ermita, basílica
        - Elementos cristianos sin denominación específica
        - Cruces, humilladeros, grottos
        
        Args:
            lat: Latitud del punto central (inmueble)
            lon: Longitud del punto central (inmueble)
            radius_m: Radio de búsqueda en metros
            
        Returns:
            Lista de edificios religiosos ordenados por distancia (más cercano primero)
        """
        
        # ================================================================
        # QUERY OVERPASS COMPLETO
        # Basado en osmwikidata/extract/queries/churches.overpassql
        # Adaptado para búsqueda por radio (around) en lugar de país
        # ================================================================
        query = f"""
        [out:json][timeout:{self.TIMEOUT}];
        (
          /* Lugares de culto católicos */
          node["amenity"="place_of_worship"]["religion"="christian"]["denomination"="catholic"](around:{radius_m},{lat},{lon});
          way["amenity"="place_of_worship"]["religion"="christian"]["denomination"="catholic"](around:{radius_m},{lat},{lon});
          relation["amenity"="place_of_worship"]["religion"="christian"]["denomination"="catholic"](around:{radius_m},{lat},{lon});
          
          /* Edificios religiosos católicos específicos */
          node["building"~"^(church|cathedral|chapel|monastery|convent|hermitage|basilica)$"]["denomination"="catholic"](around:{radius_m},{lat},{lon});
          way["building"~"^(church|cathedral|chapel|monastery|convent|hermitage|basilica)$"]["denomination"="catholic"](around:{radius_m},{lat},{lon});
          relation["building"~"^(church|cathedral|chapel|monastery|convent|hermitage|basilica)$"]["denomination"="catholic"](around:{radius_m},{lat},{lon});
          
          /* Lugares de culto cristianos sin denominación específica */
          node["amenity"="place_of_worship"]["religion"="christian"][!"denomination"](around:{radius_m},{lat},{lon});
          way["amenity"="place_of_worship"]["religion"="christian"][!"denomination"](around:{radius_m},{lat},{lon});
          relation["amenity"="place_of_worship"]["religion"="christian"][!"denomination"](around:{radius_m},{lat},{lon});
          
          /* Edificios religiosos cristianos sin denominación */
          node["building"~"^(church|cathedral|chapel|monastery|convent|hermitage|basilica)$"]["religion"="christian"][!"denomination"](around:{radius_m},{lat},{lon});
          way["building"~"^(church|cathedral|chapel|monastery|convent|hermitage|basilica)$"]["religion"="christian"][!"denomination"](around:{radius_m},{lat},{lon});
          relation["building"~"^(church|cathedral|chapel|monastery|convent|hermitage|basilica)$"]["religion"="christian"][!"denomination"](around:{radius_m},{lat},{lon});
          
          /* Cruces, humilladeros, grottos */
          node["place_of_worship"~"^(cross|wayside_shrine|lourdes_grotto)$"]["religion"="christian"](around:{radius_m},{lat},{lon});
          way["place_of_worship"~"^(cross|wayside_shrine|lourdes_grotto)$"]["religion"="christian"](around:{radius_m},{lat},{lon});
          relation["place_of_worship"~"^(cross|wayside_shrine|lourdes_grotto)$"]["religion"="christian"](around:{radius_m},{lat},{lon});
        );
        out tags center qt;
        """
        # ================================================================
        # EXPLICACIÓN:
        # 
        # 1. CATÓLICOS EXPLÍCITOS:
        #    amenity=place_of_worship + religion=christian + denomination=catholic
        #    building=(church|cathedral|...) + denomination=catholic
        # 
        # 2. CRISTIANOS SIN DENOMINACIÓN:
        #    amenity=place_of_worship + religion=christian + !denomination
        #    (probablemente católicos en España)
        # 
        # 3. ELEMENTOS RELIGIOSOS MENORES:
        #    place_of_worship=(cross|wayside_shrine|lourdes_grotto)
        # 
        # 4. TIPOS DE EDIFICIOS DETECTADOS:
        #    - church: iglesia
        #    - cathedral: catedral
        #    - chapel: capilla
        #    - monastery: monasterio
        #    - convent: convento
        #    - hermitage: ermita
        #    - basilica: basílica
        # ================================================================
        
        try:
            logger.debug(f"Overpass: buscando edificios religiosos en {radius_m}m de ({lat:.4f}, {lon:.4f})")
            
            response = requests.post(
                self.OVERPASS_URL,
                data={'data': query},
                timeout=self.TIMEOUT,
                headers={'User-Agent': 'SIPI-ETL/1.0 (patrimonio-religioso)'}
            )
            response.raise_for_status()
            
            data = response.json()
            churches = []
            
            for element in data.get('elements', []):
                osm_type = element.get('type')
                osm_id = element.get('id')
                tags = element.get('tags', {})
                name = tags.get('name', 'Sin nombre')
                
                # Obtener coordenadas
                if osm_type == 'node':
                    elem_lat = element.get('lat')
                    elem_lon = element.get('lon')
                elif 'center' in element:
                    elem_lat = element['center'].get('lat')
                    elem_lon = element['center'].get('lon')
                else:
                    continue
                
                # Calcular distancia real
                distance = self._haversine_distance(lat, lon, elem_lat, elem_lon)
                
                churches.append(OSMChurch(
                    osm_id=osm_id,
                    osm_type=osm_type,
                    name=name,
                    lat=elem_lat,
                    lon=elem_lon,
                    distance=distance,
                    tags=tags
                ))
            
            # Ordenar por distancia
            churches.sort(key=lambda x: x.distance)
            
            # Log por tipos
            if churches:
                types_count = {}
                for church in churches:
                    types_count[church.building_type] = types_count.get(church.building_type, 0) + 1
                
                logger.info(
                    f"✓ Encontrados {len(churches)} edificios religiosos en {radius_m}m: "
                    f"{', '.join(f'{count} {type}' for type, count in types_count.items())}"
                )
            else:
                logger.info(f"No se encontraron edificios religiosos en {radius_m}m")
            
            return churches
        
        except Exception as e:
            logger.error(f"Error en Overpass API: {e}")
            return []
    
    @staticmethod
    def _haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calcula distancia en metros entre dos puntos usando fórmula de Haversine"""
        R = 6371000  # Radio de la Tierra en metros
        
        lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
        
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * asin(sqrt(a))
        
        return R * c