import asyncio
from collections import defaultdict
from typing import Dict
import logging
from yate.protocol import Message

from .util import class_from_dotted_string

try:
    import redis.asyncio as redis
except ImportError:
    pass  # this is an optional dependency


class BusyCacheBase:
    def __init__(self, yate, settings):
        self._yate = yate
        self._settings = settings

    async def init(self, skip_msg_registration=False, **kwargs):
        if not skip_msg_registration:
            await self._yate.register_message_handler_async(
                "call.cdr", self._handle_cdr_message, priority=5
            )

    async def stop(self):
        pass

    def _handle_cdr_message(self, msg: Message):
        if not "operation" in msg.params:
            self._yate.answer_message(msg, False)
        # we don't let yate wait until we processed thiss
        self._yate.answer_message(msg, False)
        asyncio.create_task(self._handle_msg_task(msg))

    async def _handle_msg_task(self, msg: Message):
        extension = msg.params.get("external")
        if extension is None:
            return
        if msg.params["operation"] == "initialize":
            await self._cdr_init(extension)
        elif msg.params["operation"] == "finalize":
            await self._cdr_finalize(extension)

    async def is_busy(self, extension) -> bool:
        pass

    async def busy_status(self) -> Dict[str, int]:
        pass

    async def flush(self):
        pass

    async def _cdr_init(self, extension):
        pass

    async def _cdr_finalize(self, extension):
        pass


class PythonDictBusyCache(BusyCacheBase):
    def __init__(self, yate, settings):
        super().__init__(yate, settings)
        self._cache = defaultdict(lambda: 0)

    async def _cdr_init(self, extension):
        self._cache[extension] += 1
        logging.debug("Extension %s -> call.cdr init", extension)

    async def _cdr_finalize(self, extension):
        if self._cache[extension] > 0:
            self._cache[extension] -= 1
            logging.debug("Extension %s -> call.cdr finalize", extension)

    async def is_busy(self, extension):
        return self._cache[extension] > 0

    async def busy_status(self):
        return self._cache


class RedisBusyCache(BusyCacheBase):
    def __init__(self, yate, settings):
        super().__init__(yate, settings)

    async def _cdr_init(self, extension):
        async with redis.Redis(connection_pool=self._yate.redis_pool) as client:
            await client.hincrby("busy_cache", extension, 1)
        logging.debug("Extension %s -> call.cdr init", extension)

    async def _cdr_finalize(self, extension):
        async with redis.Redis(connection_pool=self._yate.redis_pool) as client:
            await client.hincrby("busy_cache", extension, -1)
        logging.debug("Extension %s -> call.cdr finzalize", extension)

    async def is_busy(self, extension):
        async with redis.Redis(connection_pool=self._yate.redis_pool) as client:
            status = await client.hget("busy_cache", extension)
            return status is not None and int(status) > 0

    async def busy_status(self):
        async with redis.Redis(connection_pool=self._yate.redis_pool) as client:
            return {k: int(v) for k, v in (await client.hgetall("busy_cache")).items()}

    async def flush(self):
        async with redis.Redis(connection_pool=self._yate.redis_pool) as client:
            await client.delete("busy_cache")


def main():
    asyncio.run(amain())


async def amain():
    import argparse
    import ywsd.settings

    parser = argparse.ArgumentParser(description="Yate Routing Engine Busy Cache")
    parser.add_argument(
        "--config", type=str, help="Config file to use.", default="routing_engine.yaml"
    )
    parser.add_argument(
        "--flush", help="Drop tables if they already exist", action="store_true"
    )

    args = parser.parse_args()
    settings = ywsd.settings.Settings(args.config)

    if settings.BUSY_CACHE_IMPLEMENTATION is None:
        logging.info("No busy cache configured. Exiting")
        return

    class MockYate:
        redis_pool = None

    yate = MockYate()
    if settings.REDIS is not None:
        yate.redis_pool = redis.ConnectionPool.from_url(settings.REDIS)

    busy_cache = class_from_dotted_string(settings.BUSY_CACHE_IMPLEMENTATION)(
        yate, settings
    )
    await busy_cache.init(skip_msg_registration=True)

    if args.flush:
        await busy_cache.flush()
        logging.info("Busy cache flushed.")
