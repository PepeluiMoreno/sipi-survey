"""
Clase base abstracta para todos los scrapers de portales inmobiliarios
Define la interfaz común que todos los portales deben implementar
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from datetime import datetime
from dataclasses import dataclass

from ...core.etl_event_system import event_bus, ETLEvent, EventType, PortalType


@dataclass
class ScraperConfig:
    """Configuración común para scrapers"""
    headless: bool = True
    max_retries: int = 3
    timeout: int = 30
    user_agent: str = None
    delay_min: float = 2.0
    delay_max: float = 5.0


@dataclass
class InmuebleData:
    """Estructura común de datos de inmueble (normalizada)"""
    id_portal: str              # ID único en el portal
    portal: str                 # Nombre del portal
    url: str                    # URL del inmueble
    titulo: str
    descripcion: Optional[str]
    precio: Optional[float]
    superficie: Optional[float]
    tipo: Optional[str]         # piso, casa, edificio, etc.
    localizacion: str
    provincia: str
    lat: Optional[float]
    lon: Optional[float]
    caracteristicas: List[str]
    imagenes: List[str]
    fecha_publicacion: Optional[datetime]
    scraped_at: datetime
    raw_data: Dict[str, Any]    # Datos originales sin normalizar


class BasePortalScraper(ABC):
    """
    Clase base que todos los scrapers deben heredar
    Define la interfaz común y proporciona funcionalidad compartida
    """
    
    def __init__(self, portal_type: PortalType, config: ScraperConfig = None):
        self.portal_type = portal_type
        self.config = config or ScraperConfig()
        self.event_bus = event_bus
        self._is_running = False
        self._should_stop = False
    
    # ========================================================================
    # Métodos abstractos (DEBEN ser implementados por cada portal)
    # ========================================================================
    
    @abstractmethod
    async def scrape_listado(
        self,
        provincia: Optional[str] = None,
        ciudad: Optional[str] = None,
        zona: Optional[str] = None,
        max_paginas: Optional[int] = None
    ) -> List[str]:
        """
        Scrape IDs de inmuebles de un listado
        
        Returns:
            Lista de IDs de inmuebles
        """
        pass
    
    @abstractmethod
    async def scrape_inmueble(self, inmueble_id: str) -> Optional[InmuebleData]:
        """
        Scrape datos completos de un inmueble específico
        
        Returns:
            InmuebleData normalizado o None si falla
        """
        pass
    
    @abstractmethod
    def extract_coordinates(self, html_or_soup: Any) -> tuple[Optional[float], Optional[float]]:
        """
        Extrae coordenadas geográficas del HTML
        Cada portal tiene su propia forma de mostrar mapas
        
        Returns:
            (lat, lon) o (None, None)
        """
        pass
    
    @abstractmethod
    def get_search_url(
        self,
        provincia: Optional[str] = None,
        ciudad: Optional[str] = None,
        zona: Optional[str] = None,
        pagina: int = 1
    ) -> str:
        """
        Construye la URL de búsqueda según el formato del portal
        
        Returns:
            URL completa de búsqueda
        """
        pass
    
    # ========================================================================
    # Métodos opcionales (pueden ser sobrescritos si es necesario)
    # ========================================================================
    
    def normalize_provincia(self, provincia: str) -> str:
        """
        Normaliza el nombre de provincia al formato del portal
        Por defecto: lowercase, guiones, sin acentos
        """
        import unicodedata
        provincia = provincia.lower()
        provincia = ''.join(
            c for c in unicodedata.normalize('NFD', provincia)
            if unicodedata.category(c) != 'Mn'
        )
        provincia = provincia.replace(' ', '-')
        return provincia
    
    def should_retry(self, error: Exception, attempt: int) -> bool:
        """
        Determina si se debe reintentar tras un error
        """
        return attempt < self.config.max_retries
    
    # ========================================================================
    # Métodos de emisión de eventos (heredados, listos para usar)
    # ========================================================================
    
    async def emit_event(
        self,
        event_type: EventType,
        data: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Emite un evento al sistema central"""
        event = ETLEvent(
            event_type=event_type,
            portal=self.portal_type,
            timestamp=datetime.now().isoformat(),
            data=data,
            metadata=metadata
        )
        await self.event_bus.emit(event)
    
    async def emit_scraping_started(self, task_name: str, total_items: Optional[int] = None):
        """Notifica inicio de scraping"""
        self._is_running = True
        await self.emit_event(
            EventType.SCRAPING_STARTED,
            {
                "task_name": task_name,
                "total_items": total_items
            }
        )
    
    async def emit_scraping_progress(
        self,
        current: int,
        total: int,
        current_item: str = None
    ):
        """Notifica progreso de scraping"""
        progress = (current / total * 100) if total > 0 else 0
        await self.emit_event(
            EventType.SCRAPING_PROGRESS,
            {
                "current": current,
                "total": total,
                "progress": round(progress, 2),
                "current_item": current_item
            }
        )
    
    async def emit_scraping_completed(self, total_scraped: int, summary: Dict[str, Any] = None):
        """Notifica finalización de scraping"""
        self._is_running = False
        await self.emit_event(
            EventType.SCRAPING_COMPLETED,
            {
                "total_scraped": total_scraped,
                "summary": summary or {}
            }
        )
    
    async def emit_scraping_error(self, error: str, context: Dict[str, Any] = None):
        """Notifica un error en el scraping"""
        await self.emit_event(
            EventType.SCRAPING_ERROR,
            {
                "error": error,
                "context": context or {}
            }
        )
    
    async def emit_detection_found(
        self,
        inmueble_id: str,
        score: float,
        evidences: list
    ):
        """Notifica detección de inmueble religioso"""
        await self.emit_event(
            EventType.DETECTION_FOUND,
            {
                "inmueble_id": inmueble_id,
                "score": score,
                "evidences": evidences
            }
        )
    
    # ========================================================================
    # Métodos de control
    # ========================================================================
    
    def stop(self):
        """Solicita detener el scraping"""
        self._should_stop = True
    
    def is_running(self) -> bool:
        """Indica si el scraper está en ejecución"""
        return self._is_running
    
    def should_continue(self) -> bool:
        """Verifica si el scraper debe continuar"""
        return not self._should_stop