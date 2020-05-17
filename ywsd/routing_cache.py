import json
import logging

from typing import Dict, Optional

from ywsd.routing_tree import IntermediateRoutingResult


class CacheError(Exception):
    def __init__(self, error_string):
        self._error_string = error_string

    def __str__(self):
        return self._error_string


class RoutingCacheBase:
    def __init__(self, yate, settings):
        pass

    async def init(self):
        pass

    async def stop(self):
        pass

    async def retrieve(self, target) -> Optional[IntermediateRoutingResult]:
        pass

    async def update(self, results: Dict[str, IntermediateRoutingResult]):
        pass


class PythonDictRoutingCache(RoutingCacheBase):
    def __init__(self, yate, settings):
        self._cache = {}

    async def retrieve(self, target) -> Optional[IntermediateRoutingResult]:
        return self._cache.get(target)

    async def update(self, results: Dict[str, IntermediateRoutingResult]):
        self._cache.update(results)


# If aioredis is not available, this should not be a failure. We just don't support this backend then
try:
    import aioredis
except ImportError:
    pass


class RedisRoutingCache(RoutingCacheBase):
    def __init__(self, yate, settings):
        self._address = settings.CACHE_CONFIG.get("address")
        if self._address is None:
            raise CacheError("No address configured for redis.")

        self._object_lifetime = settings.CACHE_CONFIG.get("object_lifetime", 600)

        self._redis = None

    async def init(self):
        try:
            self._redis = await aioredis.create_redis_pool(self._address, timeout=20)
            logging.info("Conected to redis routing cache.")
        except (FileNotFoundError, OSError) as e:
            raise CacheError("Unable to connect to redis: {}".format(e))

    async def stop(self):
        if self._redis is not None:
            self._redis.close()
            await self._redis.wait_closed()

    async def retrieve(self, target) -> Optional[IntermediateRoutingResult]:
        data = await self._redis.get(target)
        if data is None:
            return None
        data = json.loads(data)
        return IntermediateRoutingResult.deserialize(data)

    async def update(self, results: Dict[str, IntermediateRoutingResult]):
        for key, routing_result in results.items():
            await self._redis.set(
                key,
                json.dumps(routing_result.serialize()),
                expire=self._object_lifetime,
            )
