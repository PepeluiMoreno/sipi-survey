"""
Scraper de Idealista con geolocalización completa
"""
import re
import logging
from typing import Optional, List
from datetime import datetime
from bs4 import BeautifulSoup

from shared.geo import NominatimClient
from portals.config.idealista import (
    LISTADO_ARTICULOS, LISTADO_ID_ATTR,
    FICHA_TITULO, FICHA_UBICACION, FICHA_PRECIO,
    FICHA_BARRIO_MUN, FICHA_CARACT_BASIC, FICHA_CARACT_EXTRA,
    FICHA_MAPA_IMG
)
from portals.extract import BaseScraper, InmuebleData, GeoData, GeoType
from portals.extract.idealista import IdealistaClient

logger = logging.getLogger(__name__)


class IdealistaScraper(BaseScraper):
    """Scraper de Idealista con scraping progresivo"""
    
    def __init__(self, use_selenium: bool = False, headless: bool = True):
        super().__init__(portal='idealista')
        self.client = IdealistaClient(use_selenium=use_selenium)
        self.geo_client = NominatimClient()
    
    async def scrape_provincia(self, provincia: str, max_pages: int = -1):
        """
        Scrapea una provincia de forma incremental
        
        Args:
            provincia: Nombre de la provincia
            max_pages: Máximo de páginas (-1 = todas)
            
        Yields:
            InmuebleData para cada inmueble encontrado
        """
        page = 1
        
        while True:
            if max_pages != -1 and page > max_pages:
                break
            
            # URL de búsqueda
            url = self.client.get_search_url(
                provincia=provincia,
                tipo='venta-casas',
                page=page
            )
            
            logger.info(f"📄 Scraping página {page} de {provincia}")
            
            # Obtener HTML
            html = self.client.get(url)
            
            if not html:
                logger.warning(f"No se pudo obtener HTML de página {page}")
                break
            
            # Parsear listado
            inmuebles_ids = self._parse_listado(html)
            
            if not inmuebles_ids:
                logger.info(f"No hay más resultados en página {page}")
                break
            
            logger.info(f"✓ Encontrados {len(inmuebles_ids)} inmuebles en página {page}")
            
            # Parsear cada ficha
            for inmueble_id in inmuebles_ids:
                try:
                    inmueble_data = await self._parse_ficha(inmueble_id, provincia)
                    
                    if inmueble_data:
                        yield inmueble_data
                
                except Exception as e:
                    logger.error(f"Error parseando ficha {inmueble_id}: {e}")
                    continue
            
            page += 1
    
    def _parse_listado(self, html: str) -> List[str]:
        """Extrae IDs de inmuebles del listado"""
        soup = BeautifulSoup(html, "lxml")
        articulos = soup.select(LISTADO_ARTICULOS)
        
        ids = []
        for art in articulos:
            inmueble_id = art.get(LISTADO_ID_ATTR)
            if inmueble_id:
                ids.append(inmueble_id)
        
        return ids
    
    async def _parse_ficha(self, inmueble_id: str, provincia: str) -> Optional[InmuebleData]:
        """Parsea la ficha de un inmueble"""
        url = self.client.get_detail_url(inmueble_id)
        
        html = self.client.get(url)
        
        if not html:
            logger.warning(f"No se pudo obtener HTML de ficha {inmueble_id}")
            return None
        
        soup = BeautifulSoup(html, "lxml")
        
        # Extraer campos básicos
        titulo_elem = soup.select_one(FICHA_TITULO)
        titulo = titulo_elem.text.strip() if titulo_elem else None
        
        ubicacion_elem = soup.select_one(FICHA_UBICACION)
        ubicacion = ubicacion_elem.text.strip() if ubicacion_elem else None
        
        precio_elem = soup.select_one(FICHA_PRECIO)
        precio = self._parse_precio(precio_elem.text) if precio_elem else None
        
        # Características
        caract_basic = [li.text.strip() for li in soup.select(FICHA_CARACT_BASIC)]
        caract_extra = [li.text.strip() for li in soup.select(FICHA_CARACT_EXTRA)]
        
        # Extraer superficie
        superficie = self._extract_superficie(caract_basic)
        
        # Barrio y municipio
        header_items = soup.select(FICHA_BARRIO_MUN)
        barrio = header_items[0].text.strip() if len(header_items) > 0 else None
        municipio = header_items[1].text.strip() if len(header_items) > 1 else None
        
        # Descripción
        descripcion = None
        desc_elem = soup.select_one("div.comment")
        if desc_elem:
            descripcion = desc_elem.text.strip()
        
        # Coordenadas (multi-estrategia)
        geo_data = await self._extract_geo(inmueble_id, ubicacion, barrio, municipio, provincia)
        
        # Crear InmuebleData
        inmueble = InmuebleData(
            id_portal=inmueble_id,
            url=url,
            titulo=titulo,
            descripcion=descripcion,
            tipo='edificio',  # TODO: detectar tipo real
            precio=precio,
            superficie=superficie,
            geo=geo_data,
            caracteristicas=caract_basic + caract_extra,
            imagenes=[],
            fecha_scraping=datetime.now().isoformat()
        )
        
        return inmueble
    
    async def _extract_geo(
        self,
        inmueble_id: str,
        ubicacion: Optional[str],
        barrio: Optional[str],
        municipio: Optional[str],
        provincia: str
    ) -> GeoData:
        """
        Extrae coordenadas usando múltiples estrategias:
        1. Mapa de Idealista (más preciso)
        2. Nominatim con ubicación completa
        3. Nominatim con barrio + municipio
        4. Nominatim con municipio solo
        """
        
        # Estrategia 1: Mapa de Idealista
        mapa_url = self.client.get_detail_url(inmueble_id) + "mapa"
        html_mapa = self.client.get(mapa_url)
        
        if html_mapa:
            soup = BeautifulSoup(html_mapa, "lxml")
            img = soup.select_one(FICHA_MAPA_IMG)
            
            if img:
                src = img.get('src', '')
                match = re.search(r"center=([\d\.]+)%2C([\d\.-]+)", src)
                
                if match:
                    lat = float(match.group(1))
                    lon = float(match.group(2))
                    
                    logger.debug(f"✓ Coords exactas desde mapa: {lat}, {lon}")
                    
                    return GeoData(
                        type=GeoType.PRECISE,
                        lat=lat,
                        lon=lon,
                        uncertainty_radius_m=50,
                        ciudad=municipio,
                        provincia=provincia
                    )
        
        # Estrategia 2: Nominatim con ubicación completa
        if ubicacion:
            lat, lon, radius = self.geo_client.geocode(
                direccion=ubicacion,
                municipio=municipio,
                provincia=provincia
            )
            
            if lat:
                logger.debug(f"✓ Coords por ubicación: {lat}, {lon}")
                return GeoData(
                    type=GeoType.APPROXIMATE,
                    lat=lat,
                    lon=lon,
                    uncertainty_radius_m=radius,
                    ciudad=municipio,
                    provincia=provincia
                )
        
        # Estrategia 3: Nominatim con barrio
        if barrio and municipio:
            lat, lon, radius = self.geo_client.geocode(
                barrio=barrio,
                municipio=municipio,
                provincia=provincia
            )
            
            if lat:
                logger.debug(f"✓ Coords por barrio: {lat}, {lon}")
                return GeoData(
                    type=GeoType.APPROXIMATE,
                    lat=lat,
                    lon=lon,
                    uncertainty_radius_m=radius,
                    ciudad=municipio,
                    provincia=provincia
                )
        
        # Estrategia 4: Solo municipio
        if municipio:
            lat, lon, radius = self.geo_client.geocode(
                municipio=municipio,
                provincia=provincia
            )
            
            if lat:
                logger.debug(f"✓ Coords por municipio: {lat}, {lon}")
                return GeoData(
                    type=GeoType.APPROXIMATE,
                    lat=lat,
                    lon=lon,
                    uncertainty_radius_m=radius,
                    ciudad=municipio,
                    provincia=provincia
                )
        
        # Sin coordenadas
        logger.warning(f"⚠ Sin coordenadas para {inmueble_id}")
        return GeoData(
            type=GeoType.NONE,
            lat=None,
            lon=None,
            uncertainty_radius_m=None,
            ciudad=municipio,
            provincia=provincia
        )
    
    @staticmethod
    def _parse_precio(texto: str) -> Optional[float]:
        """Extrae precio del texto"""
        try:
            # Ejemplo: "295.000 €"
            precio_str = texto.replace(".", "").replace("€", "").replace(",", "").strip()
            return float(precio_str)
        except:
            return None
    
    @staticmethod
    def _extract_superficie(caracteristicas: List[str]) -> Optional[float]:
        """Extrae superficie de características"""
        for caract in caracteristicas:
            # Ejemplo: "150 m² construidos"
            match = re.search(r"(\d+)\s*m²", caract)
            if match:
                return float(match.group(1))
        return None
    
    async def close(self):
        """Cierra cliente HTTP"""
        self.client.close()