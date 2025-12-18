"""
Fallback de geolocalización vía Nominatim (OpenStreetMap)
"""
from typing import Optional
from pathlib import Path
import requests
import logging

logger = logging.getLogger(__name__)


class GeoFallback:
    NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
    HEADERS = {"User-Agent": "sipi-etl/1.0 (javidatascience@gmail.com)"}

    @staticmethod
    def centro_del_barrio(barrio: str, municipio: str) -> tuple[Optional[float], Optional[float]]:
        """
        Devuelve (lat, lng) del centro del barrio vía Nominatim
        """
        query = f"Centro de {barrio}, {municipio}, Spain"
        params = {"q": query, "format": "json", "limit": 1}
        try:
            r = requests.get(GeoFallback.NOMINATIM_URL, params=params, headers=GeoFallback.HEADERS, timeout=5)
            r.raise_for_status()
            data = r.json()
            if data:
                return float(data[0]["lat"]), float(data[0]["lon"])
        except Exception as e:
            logger.warning("Nominatim falló para '%s': %s", query, e)
        return None, None