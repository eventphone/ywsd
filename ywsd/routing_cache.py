import json
import logging

from typing import Dict, Optional

from ywsd.routing_tree import IntermediateRoutingResult

try:
    import redis.asyncio as redis
except ImportError:
    pass  # this is an optional dependency


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


class RedisRoutingCache(RoutingCacheBase):
    def __init__(self, yate, settings):
        self._object_lifetime = settings.CACHE_CONFIG.get("object_lifetime", 600)
        self._redis_pool = yate.redis_pool

    async def retrieve(self, target) -> Optional[IntermediateRoutingResult]:
        async with redis.Redis(connection_pool=self._redis_pool) as client:
            try:
                data = await client.get(target)
            except Exception as e:
                logging.error(
                    "Failure to retrieve cached routing result from redis: %s", e
                )
                raise
            if data is None:
                return None
            data = json.loads(data)
            return IntermediateRoutingResult.deserialize(data)

    async def update(self, results: Dict[str, IntermediateRoutingResult]):
        async with redis.Redis(connection_pool=self._redis_pool) as client:
            try:
                for key, routing_result in results.items():
                    await client.setex(
                        key,
                        self._object_lifetime,
                        json.dumps(routing_result.serialize()),
                    )
            except Exception as e:
                logging.error("Failure to update cached routing in redis: %s", e)
                raise
