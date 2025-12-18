"""
Scraper de Idealista con scoring religioso integrado
Sin TOML, sin puntitos, listo para ejecutar
"""
import re, logging, datetime, pathlib
from typing import List, Dict, Optional
from bs4 import BeautifulSoup

from config import *
from transform.geo_fallback import GeoFallback
from base_scraper import BasePortalScraper
from core.etl_event_system import PortalType


logger = logging.getLogger(__name__)


class IdealistaScraper(BasePortalScraper):
    def __init__(self):
        super().__init__(PortalType.IDEALISTA)
        self.client = None

    async def scrape(self, provincias: List[str], tipos_inmueble: List[str],
                     max_items_total: int = 100, max_pages_per_tipo: int = 2) -> List[Dict]:
        total_extraidos = 0; resultados = []
        for provincia in provincias:
            for tipo_raw in tipos_inmueble:
                tipo_limpio = tipo_raw.replace("venta-", "").rstrip("s")
                pagina = 1
                while True:
                    if max_pages_per_tipo != -1 and pagina > max_pages_per_tipo: break
                    if total_extraidos >= max_items_total: return resultados
                    url = f"https://www.idealista.com/{tipo_raw}/{provincia}-provincia/"
                    if pagina > 1: url = f"{url}pagina-{pagina}.htm"
                    html = await self.client.get(url, wait_for_selector=LISTADO_ARTICULOS)
                    if not html: break
                    inmuebles = self._parse_listado(html, provincia, tipo_limpio, pagina)
                    if not inmuebles: break
                    for inmueble in inmuebles:
                        ficha = await self._parse_ficha(inmueble["id_idealista"])
                        inmueble.update(ficha)
                        resultados.append(inmueble)
                        total_extraidos += 1
                    await self.emit_scraping_progress(provincia=provincia, tipo_inmueble=tipo_limpio,
                                                      pagina=pagina, items_extraidos=len(inmuebles))
                    pagina += 1
        return resultados

    def _parse_listado(self, html: str, provincia: str, tipo_limpio: str, pagina: int) -> List[Dict]:
        soup = BeautifulSoup(html, "lxml")
        articulos = soup.select(LISTADO_ARTICULOS)
        salida = []
        for art in articulos:
            id_idealista = art.get(LISTADO_ID_ATTR)
            if id_idealista:
                salida.append({"id_idealista": id_idealista, "provincia": provincia,
                               "tipo_inmueble": tipo_limpio, "pagina_origen": pagina,
                               "fecha_extraccion": datetime.datetime.now().date().isoformat()})
        return salida

    async def _parse_ficha(self, id_idealista: str) -> Dict:
        url = f"https://www.idealista.com/inmueble/{id_idealista}/"
        html = await self.client.get(url, wait_for_selector=FICHA_TITULO)
        if not html: return {}
        soup = BeautifulSoup(html, "lxml")

        tit   = soup.select_one(FICHA_TITULO)
        ubic  = soup.select_one(FICHA_UBICACION)
        prec  = soup.select_one(FICHA_PRECIO)
        stats = soup.select_one(FICHA_STATS_FECHA)
        stats_lnk = soup.select_one(FICHA_STATS_LINK)
        header = soup.select(FICHA_BARRIO_MUN)
        c1 = soup.select(FICHA_CARACT_BASIC)
        c2 = soup.select(FICHA_CARACT_EXTRA)

        inmueble = {
            "id_idealista": id_idealista,
            "titulo_completo": tit.text.strip() if tit else None,
            "localizacion": ubic.text.split(",")[0].strip() if ubic else None,
            "precio": int(prec.text.replace(".", "").split()[0]) if prec else None,
            "fecha_actualizacion": self._parse_fecha(stats.text) if stats else None,
            "visitas_contactos": stats_lnk.text.strip() if stats_lnk else None,
            "barrio": header[0].text.strip() if len(header) > 0 else None,
            "municipio": header[1].text.strip() if len(header) > 1 else None,
            "caracteristicas_basicas": [li.text.strip() for li in c1],
            "caracteristicas_extras": [li.text.strip() for li in c2],
            "url_ficha": url
        }

        lat, lng, precision = await self._extraer_coords(id_idealista)
        inmueble.update({"latitud": lat, "longitud": lng, "precision_geo": precision})

        from src.modules.portals.config.keywords import POSITIVE, NEGATIVE, EXPLICIT
        from src.modules.portals.idealista.config.scoring import WEIGHTS, PROXIMITY, SURFACE
        from src.modules.portals.idealista.transform.scorer import ReligiousPropertyScorer

        keywords = POSITIVE + NEGATIVE
        inmueble["keywords_encontradas"] = [kw for kw in keywords if kw.lower() in (inmueble["titulo_completo"] or "").lower()]
        if any(k.lower() in (inmueble["titulo_completo"] or "").lower() for k in EXPLICIT):
            score = 100
            evidencias = ["Keyword explícita (100 %)"]
        else:
            score = 0
            evidencias = []
            for kw in POSITIVE:
                if kw.lower() in (inmueble["titulo_completo"] or "").lower():
                    score += WEIGHTS["keywords"] // len(POSITIVE)
                    evidencias.append(f"Keyword positiva '{kw}'")
            for kw in NEGATIVE:
                if kw.lower() in (inmueble["titulo_completo"] or "").lower():
                    score -= WEIGHTS["keywords"] // len(NEGATIVE)
                    evidencias.append(f"Keyword negativa '{kw}'")

        # Proximidad OSM
        if inmueble.get("latitud") and inmueble.get("longitud"):
            churches = self.overpass.find_churches_nearby(inmueble["latitud"], inmueble["longitud"], PROXIMITY["radius_meters"])
            if churches:
                closest = churches[0]
                score += PROXIMITY["max_score"] * (1 - min(closest.distance / 300, 1))
                evidencias.append(f"{len(churches)} iglesia(s) OSM en {PROXIMITY['radius_meters']}m, más cercana a {closest.distance:.0f}m")

        # Superficie y características
        m2 = inmueble.get("m2_construidos")
        if m2 and m2 >= SURFACE["min_size_m2"]:
            score += SURFACE["max_score"]
            evidencias.append(f"Superficie ≥ {SURFACE['min_size_m2']}m²")
        extras = " ".join(inmueble.get("caracteristicas_extras", [])).lower()
        if any(t in extras for t in ["techos altos", "doble altura"]):
            score += SURFACE["bonus"]["high_ceilings"]
            evidencias.append("Techos altos/doble altura")
        if any(t in extras for t in ["varias plantas", "múltiples niveles"]):
            score += SURFACE["bonus"]["multiple_floors"]
            evidencias.append("Múltiples niveles")

        inmueble.update({"score_religioso": min(score, 100), "evidencias_religiosas": evidencias})

        return inmueble

    async def _extraer_coords(self, id_idealista: str) -> tuple[Optional[float], Optional[float], str]:
        html_mapa = await self.client.get(f"https://www.idealista.com/inmueble/{id_idealista}/mapa",
                                          wait_for_selector=FICHA_MAPA_IMG)
        if html_mapa:
            soup = BeautifulSoup(html_mapa, "lxml")
            img = soup.select_one(FICHA_MAPA_IMG)
            if img:
                src = img["src"]
                m = re.search(r"center=([\d\.]+)%2C([\d\.-]+)", src)
                if m:
                    return float(m.group(1)), float(m.group(2)), "exacta"

        barrio, municipio = inmueble.get("barrio"), inmueble.get("municipio")
        if barrio and municipio:
            lat, lng = GeoFallback.centro_del_barrio(barrio, municipio)
            if lat is not None:
                return lat, lng, "barrio"
        return None, None, None

    def _load_keywords(self) -> list[str]:
        from src.modules.portals.config.keywords import POSITIVE, NEGATIVE, EXPLICIT
        return POSITIVE + NEGATIVE

    @staticmethod
    def _parse_fecha(texto: str) -> str:
        meses = {m: i + 1 for i, m in enumerate(
            ["enero", "febrero", "marzo", "abril", "mayo", "junio",
             "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"])}
        try:
            partes = texto.replace("Anuncio actualizado el ", "").split()
            dia, mes_nombre = int(partes[0]), partes[2].lower()
            return datetime.datetime(datetime.datetime.now().year, meses[mes_nombre], dia).date().isoformat()
        except Exception:
            return None