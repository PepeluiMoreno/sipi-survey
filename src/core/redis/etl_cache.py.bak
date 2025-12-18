"""
Redis cache para el Load Layer
- Rate limiting
- Deduplicación
- Estado de scraping
- Locks distribuidos
"""
import json
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
import asyncio

try:
    import redis.asyncio as redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False


class ETLRedisCache:
    """
    Cache Redis para operaciones ETL
    """
    
    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        db: int = 1  # DB diferente al geocoder
    ):
        if not REDIS_AVAILABLE:
            raise ImportError("redis package not installed")
        
        self.redis_url = redis_url
        self.db = db
        self.client: Optional[redis.Redis] = None
    
    async def connect(self):
        """Conecta a Redis"""
        if self.client is None:
            self.client = await redis.from_url(
                self.redis_url,
                db=self.db,
                decode_responses=False  # Para locks binarios
            )
    
    async def disconnect(self):
        """Desconecta"""
        if self.client:
            await self.client.close()
            self.client = None
    
    # ========================================================================
    # DEDUPLICACIÓN
    # ========================================================================
    
    async def check_duplicate(
        self,
        portal: str,
        id_portal: str,
        ttl_hours: int = 24
    ) -> bool:
        """
        Verifica si un inmueble ya fue procesado recientemente
        
        Returns:
            True si es duplicado (ya existe), False si es nuevo
        """
        if not self.client:
            await self.connect()
        
        key = f"dedup:{portal}:{id_portal}"
        
        exists = await self.client.exists(key)
        
        if not exists:
            # Marcar como procesado
            await self.client.setex(
                key,
                timedelta(hours=ttl_hours),
                b"1"
            )
            return False
        
        return True
    
    # ========================================================================
    # RATE LIMITING
    # ========================================================================
    
    async def check_rate_limit(
        self,
        key: str,
        max_requests: int,
        window_seconds: int
    ) -> bool:
        """
        Verifica rate limit usando sliding window
        
        Args:
            key: Identificador único (ej: 'scraper:idealista')
            max_requests: Máximo de requests en la ventana
            window_seconds: Tamaño de ventana en segundos
            
        Returns:
            True si está dentro del límite, False si excedió
        """
        if not self.client:
            await self.connect()
        
        now = datetime.now().timestamp()
        window_start = now - window_seconds
        
        rate_key = f"ratelimit:{key}"
        
        # Limpiar entradas antiguas
        await self.client.zremrangebyscore(rate_key, 0, window_start)
        
        # Contar requests en la ventana
        current_count = await self.client.zcard(rate_key)
        
        if current_count >= max_requests:
            return False
        
        # Añadir nuevo request
        await self.client.zadd(rate_key, {str(now): now})
        await self.client.expire(rate_key, window_seconds)
        
        return True
    
    async def wait_for_rate_limit(
        self,
        key: str,
        max_requests: int,
        window_seconds: int,
        max_wait: int = 60
    ):
        """
        Espera hasta que el rate limit permita continuar
        """
        waited = 0
        while waited < max_wait:
            if await self.check_rate_limit(key, max_requests, window_seconds):
                return
            
            await asyncio.sleep(1)
            waited += 1
        
        raise TimeoutError(f"Rate limit wait timeout after {max_wait}s")
    
    # ========================================================================
    # DISTRIBUTED LOCKS
    # ========================================================================
    
    async def acquire_lock(
        self,
        lock_name: str,
        timeout_seconds: int = 300,
        blocking: bool = True,
        blocking_timeout: int = 10
    ) -> bool:
        """
        Adquiere un lock distribuido
        
        Args:
            lock_name: Nombre del lock
            timeout_seconds: TTL del lock (auto-release)
            blocking: Si True, espera hasta obtener el lock
            blocking_timeout: Máximo tiempo de espera
            
        Returns:
            True si adquirió el lock, False si no pudo
        """
        if not self.client:
            await self.connect()
        
        lock_key = f"lock:{lock_name}"
        lock_value = str(datetime.now().timestamp())
        
        if blocking:
            # Intentar adquirir con espera
            start = datetime.now()
            while (datetime.now() - start).seconds < blocking_timeout:
                acquired = await self.client.set(
                    lock_key,
                    lock_value,
                    nx=True,  # Solo si no existe
                    ex=timeout_seconds
                )
                
                if acquired:
                    return True
                
                await asyncio.sleep(0.1)
            
            return False
        
        else:
            # Intento único
            acquired = await self.client.set(
                lock_key,
                lock_value,
                nx=True,
                ex=timeout_seconds
            )
            
            return bool(acquired)
    
    async def release_lock(self, lock_name: str):
        """Libera un lock"""
        if not self.client:
            await self.connect()
        
        lock_key = f"lock:{lock_name}"
        await self.client.delete(lock_key)
    
    # ========================================================================
    # SCRAPING STATE
    # ========================================================================
    
    async def save_scraping_state(
        self,
        portal: str,
        state: Dict[str, Any],
        ttl_hours: int = 24
    ):
        """Guarda estado de scraping"""
        if not self.client:
            await self.connect()
        
        key = f"scraping_state:{portal}"
        
        await self.client.setex(
            key,
            timedelta(hours=ttl_hours),
            json.dumps(state)
        )
    
    async def load_scraping_state(
        self,
        portal: str
    ) -> Optional[Dict[str, Any]]:
        """Carga estado de scraping"""
        if not self.client:
            await self.connect()
        
        key = f"scraping_state:{portal}"
        data = await self.client.get(key)
        
        if data:
            return json.loads(data)
        
        return None
    
    # ========================================================================
    # JOB QUEUE (simple)
    # ========================================================================
    
    async def enqueue_load_job(
        self,
        portal: str,
        inmueble_data: Dict[str, Any]
    ):
        """Añade job de carga a la cola"""
        if not self.client:
            await self.connect()
        
        queue_key = f"load_queue:{portal}"
        
        await self.client.rpush(
            queue_key,
            json.dumps(inmueble_data)
        )
    
    async def dequeue_load_job(
        self,
        portal: str,
        timeout: int = 0
    ) -> Optional[Dict[str, Any]]:
        """Obtiene job de la cola"""
        if not self.client:
            await self.connect()
        
        queue_key = f"load_queue:{portal}"
        
        if timeout > 0:
            # Blocking pop
            result = await self.client.blpop(queue_key, timeout)
            if result:
                _, data = result
                return json.loads(data)
        else:
            # Non-blocking pop
            data = await self.client.lpop(queue_key)
            if data:
                return json.loads(data)
        
        return None
    
    async def queue_length(self, portal: str) -> int:
        """Retorna longitud de la cola"""
        if not self.client:
            await self.connect()
        
        queue_key = f"load_queue:{portal}"
        return await self.client.llen(queue_key)


# Singleton global
_etl_cache: Optional[ETLRedisCache] = None


async def get_etl_cache(redis_url: str = None) -> ETLRedisCache:
    """Obtiene instancia global"""
    global _etl_cache
    
    if _etl_cache is None:
        from os import getenv
        url = redis_url or getenv('REDIS_URL', 'redis://localhost:6379')
        _etl_cache = ETLRedisCache(redis_url=url)
        await _etl_cache.connect()
    
    return _etl_cache