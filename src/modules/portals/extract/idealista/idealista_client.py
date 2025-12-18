"""
Cliente HTTP para Idealista
Maneja peticiones, cookies, headers, rate limiting y evasión de detección
"""
import time
import random
import logging
from typing import Optional
from urllib.parse import urljoin
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from threading import Lock

from src.core.config import config

logger = logging.getLogger(__name__)


class IdealistaClient:
    """
    Cliente HTTP para scraping de Idealista
    Maneja peticiones, rate limiting y evasión de detección
    """
    BASE_URL = "https://www.idealista.com"

    def __init__(
        self,
        use_selenium: bool = True,
        headless: bool = True,
        rate_limit_delay: Optional[float] = None,
    ):
        self.use_selenium = use_selenium
        self.headless = headless
        self.rate_limit_delay = rate_limit_delay or config.scraping.get("rate_limit_delay", 1.5)
        
        self.driver: Optional[webdriver.Chrome] = None
        self.session = requests.Session()
        self._setup_session()
        
        self.last_request_time = 0.0
        self._rate_limit_lock = Lock()  # Thread-safe rate limiting

    def _setup_session(self):
        """Configura la sesión de requests con headers realistas"""
        self.session.headers.update({
            "User-Agent": config.scraping.get("user_agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "es-ES,es;q=0.9,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        })

    def _init_selenium(self):
        """Inicializa el driver de Selenium con evasión de detección"""
        if self.driver is not None:
            return
        
        opts = Options()
        if self.headless:
            opts.add_argument("--headless=new")
        
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option("useAutomationExtension", False)
        opts.add_argument(f"user-agent={config.scraping.get('user_agent')}")
        opts.add_argument("--window-size=1920,1080")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--disable-software-rasterizer")

        try:
            self.driver = webdriver.Chrome(options=opts)
            self.driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            })
            self.driver.set_page_load_timeout(config.scraping.get("request_timeout", 30))
            self.driver.implicitly_wait(5)
            logger.debug("Selenium driver inicializado correctamente")
        except Exception as e:
            logger.warning(f"Selenium no disponible, usando requests: {e}")
            self.use_selenium = False

    def _apply_rate_limit(self):
        """Thread-safe rate limiting con jitter"""
        with self._rate_limit_lock:
            elapsed = time.time() - self.last_request_time
            if elapsed < self.rate_limit_delay:
                sleep_time = self.rate_limit_delay - elapsed + random.uniform(0.0, 0.5)
                time.sleep(sleep_time)
            self.last_request_time = time.time()

    def get(self, url: str, wait_for_selector: Optional[str] = None) -> Optional[str]:
        """
        Realiza una petición GET a Idealista
        
        Args:
            url: URL completa o relativa
            wait_for_selector: Selector CSS para esperar (solo Selenium)
            
        Returns:
            HTML de la página o None si falla
        """
        if not url.startswith("http"):
            url = urljoin(self.BASE_URL, url.lstrip("/"))
        
        self._apply_rate_limit()
        
        if self.use_selenium:
            return self._get_with_selenium(url, wait_for_selector)
        return self._get_with_requests(url)

    def _get_with_selenium(self, url: str, wait_for_selector: Optional[str]) -> Optional[str]:
        """Petición con Selenium"""
        self._init_selenium()
        if self.driver is None:
            return self._get_with_requests(url)
        
        try:
            logger.debug(f"Selenium GET: {url}")
            self.driver.get(url)
            
            if wait_for_selector:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, wait_for_selector))
                )
            
            time.sleep(random.uniform(0.5, 1.5))  # Renderizado JS
            html = self.driver.page_source
            
            return None if self._is_blocked(html) else html
        
        except TimeoutException:
            logger.warning(f"Timeout esperando selector en {url}")
            return None
        except Exception as e:
            logger.error(f"Error Selenium en {url}: {e}")
            return None

    def _get_with_requests(self, url: str) -> Optional[str]:
        """Petición con requests (fallback)"""
        try:
            logger.debug(f"Requests GET: {url}")
            r = self.session.get(url, timeout=config.scraping.get("request_timeout", 30))
            r.raise_for_status()
            return None if self._is_blocked(r.text) else r.text
        except requests.RequestException as e:
            logger.error(f"Error requests en {url}: {e}")
            return None

    def _is_blocked(self, html: str) -> bool:
        """Detecta bloqueo específico de Idealista"""
        if not html:
            return True
        
        blocked_patterns = [
            "captcha",
            "access denied",
            "too many requests",
            "rate limit",
            "cloudflare",
            "security check",
            "has been blocked",
            "<title>Idealista</title>",  # Página vacía típica de bloqueo
        ]
        
        lower_html = html.lower()
        return any(p in lower_html for p in blocked_patterns)

    def get_search_url(
        self,
        provincia: str,
        tipo: str = "venta",
        operacion: str = "casas",
        page: int = 1,
    ) -> str:
        """
        Construye URL de búsqueda de Idealista
        
        Args:
            provincia: Nombre de la provincia (ej: "sevilla")
            tipo: "venta" o "alquiler"
            operacion: "casas", "pisos", "locales", etc.
            page: Número de página (1 = primera)
            
        Returns:
            URL completa de búsqueda
        """
        slug = provincia.lower().replace(" ", "-")
        if page == 1:
            path = f"/{tipo}/{operacion}/{slug}-provincia/"
        else:
            path = f"/{tipo}/{operacion}/{slug}-provincia/pagina-{page}.htm"
        return urljoin(self.BASE_URL, path)

    def get_detail_url(self, inmueble_id: str) -> str:
        """
        Construye URL de ficha de un inmueble en Idealista
        
        Args:
            inmueble_id: ID del inmueble (ej: "12345678")
            
        Returns:
            URL completa de la ficha
        """
        return urljoin(self.BASE_URL, f"/inmueble/{inmueble_id}/")

    def close(self):
        """Cierra el driver de Selenium y libera recursos"""
        if self.driver:
            try:
                self.driver.quit()
                logger.info("Selenium driver cerrado correctamente")
            except Exception as e:
                logger.warning(f"Error cerrando driver: {e}")
            finally:
                self.driver = None

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit: garantiza cierre del driver"""
        self.close()

    def __del__(self):
        """Fallback para asegurar cierre al destruir el objeto"""
        self.close()