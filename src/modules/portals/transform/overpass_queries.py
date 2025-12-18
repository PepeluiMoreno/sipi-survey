"""
Cliente para Overpass API (OpenStreetMap)
"""
import requests
from typing import List, Dict, Optional
from src.core.config import config


class OSMChurch:
    """Representa una iglesia de OSM"""
    
    def __init__(self, osm_id: int, osm_type: str, name: str, lat: float, lon: float, distance: float = 0):
        self.osm_id = osm_id
        self.osm_type = osm_type
        self.name = name
        self.lat = lat
        self.lon = lon
        self.distance = distance


class OverpassClient:
    """
    Cliente para consultas a Overpass API (OpenStreetMap)
    """
    
    def __init__(self):
        self.overpass_url = config.osm['overpass_url']
        self.timeout = config.osm['timeout']
    
    def find_churches_nearby(
        self,
        lat: float,
        lon: float,
        radius_m: int = None
    ) -> List[OSMChurch]:
        """
        Busca iglesias cercanas en OSM
        
        Args:
            lat: Latitud
            lon: Longitud
            radius_m: Radio de búsqueda en metros
            
        Returns:
            Lista de iglesias encontradas
        """
        if radius_m is None:
            radius_m = config.osm['default_search_radius_m']
        
        # Query Overpass
        query = f"""
        [out:json][timeout:{self.timeout}];
        (
          node["amenity"="place_of_worship"]["religion"="christian"](around:{radius_m},{lat},{lon});
          way["amenity"="place_of_worship"]["religion"="christian"](around:{radius_m},{lat},{lon});
          relation["amenity"="place_of_worship"]["religion"="christian"](around:{radius_m},{lat},{lon});
        );
        out center;
        """
        
        try:
            response = requests.post(
                self.overpass_url,
                data={'data': query},
                timeout=self.timeout
            )
            response.raise_for_status()
            
            data = response.json()
            churches = []
            
            for element in data.get('elements', []):
                osm_type = element.get('type')
                osm_id = element.get('id')
                name = element.get('tags', {}).get('name', 'Sin nombre')
                
                # Obtener coordenadas
                if osm_type == 'node':
                    elem_lat = element.get('lat')
                    elem_lon = element.get('lon')
                elif 'center' in element:
                    elem_lat = element['center'].get('lat')
                    elem_lon = element['center'].get('lon')
                else:
                    continue
                
                # Calcular distancia aproximada
                distance = self._haversine_distance(lat, lon, elem_lat, elem_lon)
                
                churches.append(OSMChurch(
                    osm_id=osm_id,
                    osm_type=osm_type,
                    name=name,
                    lat=elem_lat,
                    lon=elem_lon,
                    distance=distance
                ))
            
            return sorted(churches, key=lambda x: x.distance)
        
        except Exception as e:
            print(f"Error consultando Overpass API: {e}")
            return []
    
    def _haversine_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calcula distancia entre dos puntos en metros"""
        from math import radians, cos, sin, asin, sqrt
        
        # Radio de la Tierra en metros
        R = 6371000
        
        # Convertir a radianes
        lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
        
        # Fórmula de Haversine
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * asin(sqrt(a))
        
        return R * c