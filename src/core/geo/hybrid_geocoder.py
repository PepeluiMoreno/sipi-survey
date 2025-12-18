"""
Geocoder híbrido con caché Redis 
-------------------------------------------
- Envuelve Photon (rápido) y Nominatim (preciso) con políticas de estrategia.
- Caché distribuido vía Redis (TTL 7 días) + LRU en memoria como fallback.
- 100 % async – no bloquea el event-loop ni las llamadas a Redis.
- Estrategias: FAST, BALANCED, PRECISE, CACHED_ONLY.
- Rate-limit de 1 s para Nominatim (OSM) dentro de un task aparte.
"""
from __future__ import annotations
import asyncio
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum

from .geocoder import (
    NominatimGeocoder,
    PhotonGeocoder,
    GeocodingResult
)
from modules.shared.redis import get_shared_redis   # único punto de entrada Redis
# --------------------------------------------------------------------------- #
# Dominio
# --------------------------------------------------------------------------- #
class GeocoderProvider(Enum):
    PHOTON    = "photon"
    NOMINATIM = "nominatim"


class GeocoderStrategy(Enum):
    FAST        = "fast"        # sólo Photon
    BALANCED    = "balanced"    # Photon → Nominatim si falla
    PRECISE     = "precise"     # Nominatim siempre
    CACHED_ONLY = "cached_only" # nunca consulta APIs
# --------------------------------------------------------------------------- #
# LRU memoria (misma filosofía que antes)
# --------------------------------------------------------------------------- #
@dataclass(slots=True)
class _MemCacheEntry:
    results: List[GeocodingResult]
    provider: GeocoderProvider
    ts: datetime


class InMemoryCache:
    """LRU simple sin locks (single-thread event-loop)."""
    def __init__(self, max_items: int = 512) -> None:
        self._max = max_items
        self._cache: Dict[str, _MemCacheEntry] = {}

    def get(self, address: str, country: str) -> Optional[List[GeocodingResult]]:
        key = f"{address.lower().strip()}|{country.upper()}"
        if (entry := self._cache.get(key)) is not None:
            # LRU – mover al final
            del self._cache[key]
            self._cache[key] = entry
            return entry.results
        return None

    def set(
        self,
        address: str,
        country: str,
        results: List[GeocodingResult],
        provider: GeocoderProvider
    ) -> None:
        key = f"{address.lower().strip()}|{country.upper()}"
        self._cache[key] = _MemCacheEntry(results, provider, datetime.utcnow())
        # control tamaño
        while len(self._cache) > self._max:
            # pop FIFO del dict (Python 3.7+ mantiene inserción)
            self._cache.pop(next(iter(self._cache)))

    def clear(self) -> None:
        self._cache.clear()

    def stats(self) -> Dict[str, int]:
        return {"entries": len(self._cache), "max": self._max}
# --------------------------------------------------------------------------- #
# Rate-limiter local para Nominatim
# --------------------------------------------------------------------------- #
class _RateLimiter:
    __slots__ = ("_delay", "_last")

    def __init__(self, delay: float = 1.0) -> None:
        self._delay = delay
        self._last: Optional[datetime] = None

    async def acquire(self) -> None:
        now = datetime.utcnow()
        if self._last is not None:
            elapsed = (now - self._last).total_seconds()
            if elapsed < self._delay:
                await asyncio.sleep(self._delay - elapsed)
        self._last = datetime.utcnow()
# --------------------------------------------------------------------------- #
# Geocoder híbrido ‑ 100 % async
# --------------------------------------------------------------------------- #
class HybridGeocoder:
    """
    Geocoder async que combina Photon, Nominatim, Redis y memoria.
    No bloquea el loop de eventos.
    """
    def __init__(
        self,
        strategy: GeocoderStrategy = GeocoderStrategy.BALANCED,
        use_redis: bool = True,
        redis_url: Optional[str] = None,
        mem_cache_size: int = 512,
        nominatim_delay: float = 1.0,
    ) -> None:
        self.strategy = strategy
        self.use_redis = use_redis
        self.redis_url = redis_url
        self.mem_cache = InMemoryCache(mem_cache_size)
        self.photon = PhotonGeocoder()
        self.nominatim = NominatimGeocoder()
        self.limiter = _RateLimiter(nominatim_delay)

    # ..................................................................... #
    # público
    # ..................................................................... #
    async def geocode(
        self,
        address: str,
        country: str = "ES",
        limit: int = 1,
        strategy: Optional[GeocoderStrategy] = None,
    ) -> Optional[List[GeocodingResult]]:
        """
        Geocodifica respetando estrategia y cachés.
        Siempre devuelve *limit* resultados como máximo.
        """
        strategy = strategy or self.strategy
        key = f"{address.lower().strip()}|{country.upper()}"

        # 1) Redis
        if self.use_redis:
            redis_cache = await get_shared_redis()
            cached: Optional[str] = await redis_cache.client.get(f"geocoder:{key}")
            if cached is not None:
                data = json.loads(cached)
                results = [
                    GeocodingResult(**raw) for raw in data["results"]
                ]
                return results[:limit]

        # 2) Memoria
        if (results := self.mem_cache.get(address, country)) is not None:
            return results[:limit]

        # 3) CACHED_ONLY
        if strategy is GeocoderStrategy.CACHED_ONLY:
            return None

        # 4) Real geocoding
        results: Optional[List[GeocodingResult]] = None
        provider: GeocoderProvider

        if strategy is GeocoderStrategy.FAST:
            results = await asyncio.get_running_loop().run_in_executor(
                None, self.photon.geocode, address, country, limit
            )
            provider = GeocoderProvider.PHOTON

        elif strategy is GeocoderStrategy.PRECISE:
            await self.limiter.acquire()
            results = await asyncio.get_running_loop().run_in_executor(
                None, self.nominatim.geocode, address, country, limit
            )
            provider = GeocoderProvider.NOMINATIM

        else:  # BALANCED
            results = await asyncio.get_running_loop().run_in_executor(
                None, self.photon.geocode, address, country, limit
            )
            provider = GeocoderProvider.PHOTON
            if not results:
                await self.limiter.acquire()
                results = await asyncio.get_running_loop().run_in_executor(
                    None, self.nominatim.geocode, address, country, limit
                )
                provider = GeocoderProvider.NOMINATIM

        if not results:
            return None

        # 5) Guardar en cachés (async)
        await self._save_caches(key, results, provider, country)
        return results[:limit]

    async def reverse_geocode(
        self, lat: float, lon: float
    ) -> Optional[GeocodingResult]:
        """
        Reverse geocoding vía Nominatim (rate-limited).
        """
        await self.limiter.acquire()
        return await asyncio.get_running_loop().run_in_executor(
            None, self.nominatim.reverse_geocode, lat, lon
        )

    async def get_cache_stats(self) -> Dict[str, Any]:
        """Estadísticas agregadas (Redis + memoria)."""
        stats: Dict[str, Any] = {"memory": self.mem_cache.stats()}

        if self.use_redis:
            redis_cache = await get_shared_redis()
            info = await redis_cache.client.info("stats")
            stats["redis"] = {
                "keyspace_hits": info.get("keyspace_hits", 0),
                "keyspace_misses": info.get("keyspace_misses", 0),
            }
        return stats

    async def clear_cache(self) -> None:
        """Limpia ambos niveles de caché."""
        self.mem_cache.clear()
        if self.use_redis:
            redis_cache = await get_shared_redis()
            pattern = "geocoder:*"
            cursor = 0
            while True:
                cursor, keys = await redis_cache.client.scan(
                    cursor, match=pattern, count=100
                )
                if keys:
                    await redis_cache.client.delete(*keys)
                if cursor == 0:
                    break

    # ..................................................................... #
    # privado
    # ..................................................................... #
    async def _save_caches(
        self,
        key: str,
        results: List[GeocodingResult],
        provider: GeocoderProvider,
        country: str,
    ) -> None:
        # memoria
        self.mem_cache.set(
            address=key.split("|")[0],
            country=country,
            results=results,
            provider=provider,
        )

        # redis (async)
        if self.use_redis:
            redis_cache = await get_shared_redis()
            payload = json.dumps(
                {
                    "results": [self._result_to_dict(r) for r in results],
                    "provider": provider.value,
                    "cached_at": datetime.utcnow().isoformat(),
                },
                ensure_ascii=False,
            )
            await redis_cache.client.setex(
                f"geocoder:{key}", timedelta(days=7), payload
            )

    # ------------------------------------------------------------------ #
    # helpers serialización (sin dependencias externas)
    # ------------------------------------------------------------------ #
    @staticmethod
    def _result_to_dict(r: GeocodingResult) -> Dict[str, Any]:
        return {
            "address": r.address,
            "display_name": r.display_name,
            "lat": r.lat,
            "lon": r.lon,
            "house_number": r.house_number,
            "road": r.road,
            "suburb": r.suburb,
            "city": r.city,
            "state": r.state,
            "postcode": r.postcode,
            "country": r.country,
            "osm_type": r.osm_type,
            "osm_id": r.osm_id,
            "place_type": r.place_type,
            "bbox": r.bbox,
        }