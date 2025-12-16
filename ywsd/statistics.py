import asyncio
import time
from typing import Optional, List, Tuple
import logging

try:
    import redis.asyncio as redis
except ImportError:
    pass  # this is an optional dependency


def initialize_statistics(stat):
    global _stat
    _stat = stat


def submit_query_time(query_time_us: int):
    if _stat is None:
        return
    asyncio.create_task(_stat.submit_query_time(query_time_us))


def submit_stage1_routing_time(epid: str, routing_time_us: int):
    if _stat is None:
        return
    asyncio.create_task(_stat.submit_stage1_routing_time(epid, routing_time_us))


def submit_stage2_routing_time(epid: str, routing_time_us: int):
    if _stat is None:
        return
    asyncio.create_task(_stat.submit_stage2_routing_time(epid, routing_time_us))


class MeasuredQuery:
    def __enter__(self):
        self.start = time.time_ns()

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        submit_query_time((time.time_ns() - self.start) // 1000)


class Statistics:
    def __init__(self, redis, config):
        self._redis = redis
        self._history_time = config.get("HISTORY_TIME", 3600)
        self._log_query_time = config.get("LOG_QUERY_TIME", False)

    async def submit_query_time(self, value: int):
        if not self._log_query_time:
            return
        key = f"query_time:{time.time_ns()}"
        async with redis.Redis(connection_pool=self._redis) as client:
            await client.setex(key, self._history_time, value)

    async def submit_stage1_routing_time(self, epid: str, value: int):
        key = f"stage1_routing_time:{epid}:{time.time_ns()}"
        async with redis.Redis(connection_pool=self._redis) as client:
            await client.setex(key, self._history_time, value)

    async def submit_stage2_routing_time(self, epid: str, value: int):
        key = f"stage2_routing_time:{epid}:{time.time_ns()}"
        async with redis.Redis(connection_pool=self._redis) as client:
            await client.setex(key, self._history_time, value)

    async def get_query_stats(self) -> List[Tuple[str, int]]:
        return await self.read_stats("query_time")

    async def get_stage1_stats(self) -> List[Tuple[str, int]]:
        return await self.read_stats("stage1_routing_time")

    async def get_stage2_stats(self) -> List[Tuple[str, int]]:
        return await self.read_stats("stage2_routing_time")

    async def read_stats(self, keyspace: str) -> List[Tuple[str, int]]:
        async with redis.Redis(connection_pool=self._redis) as client:
            return [
                (key.lstrip(f"{keyspace}:"), await client.get(key))
                async for key in client.scan_iter(f"{keyspace}:*")
            ]


_stat: Optional[Statistics] = None
