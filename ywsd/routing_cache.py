from typing import Dict, Optional
from ywsd.routing_tree import IntermediateRoutingResult


class RoutingCacheBase:
    async def retrieve(self, target) -> Optional[IntermediateRoutingResult]:
        pass

    async def update(self, results: Dict[str, IntermediateRoutingResult]):
        pass


class PythonDictRoutingCache:
    def __init__(self):
        self._cache = {}

    async def retrieve(self, target) -> Optional[IntermediateRoutingResult]:
        return self._cache.get(target)

    async def update(self, results: Dict[str, IntermediateRoutingResult]):
        self._cache.update(results)
