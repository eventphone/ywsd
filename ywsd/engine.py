import argparse
import asyncio
import signal
import logging
from typing import Optional, Dict

import aiopg.sa
from yate.asyncio import YateAsync
from yate.protocol import Message

import ywsd.yate
from ywsd.objects import Yate
from ywsd.routing_cache import PythonDictRoutingCache, RoutingCacheBase
from ywsd.routing_tree import RoutingTree, IntermediateRoutingResult, RoutingError
from ywsd.settings import Settings


logging.basicConfig(level=logging.DEBUG)

class YateStage1RoutingEngine(YateAsync):
    def __init__(self, *args, **kwargs):
        self._settings = kwargs.pop("settings")
        super().__init__(*args, **kwargs)
        self._shutdown_future = None
        self._db_engine = None
        self._routing_cache: Optional[RoutingCacheBase] = None
        self._yates_dict: Dict[int, Yate] = {}

    @property
    def settings(self):
        return self._settings

    @property
    def yates_dict(self):
        return self._yates_dict

    @property
    def db_engine(self):
        return self._db_engine

    def run(self):
        logging.info("Initialiting YateAsync engine")
        super().run(self.main)

    async def main(self, _):
        logging.info("Initializing main application")
        self._shutdown_future = asyncio.get_event_loop().create_future()
        asyncio.get_event_loop().add_signal_handler(signal.SIGINT, lambda: self._shutdown_future.set_result(True))
        logging.info("Initializing routing cache")
        self._routing_cache = PythonDictRoutingCache()

        logging.info("Initializing database engine")
        async with aiopg.sa.create_engine(**self._settings.DB_CONFIG) as db_engine:
            self._db_engine = db_engine
            logging.info("Loading remote yates information from DB")
            async with self.db_engine.acquire() as db_connection:
                self._yates_dict = await Yate.load_yates_dict(db_connection)

            logging.info("Registering for routing messages")
            if not await self.register_message_handler_async("call.route", self._call_route_handler, 50):
                logging.error("Cannot register for call.route. Terminating...")
                return

            logging.info("Ready to route")
            await self._shutdown_future

        self._db_engine = None

    def _call_route_handler(self, msg: Message) -> Optional[bool]:
        logging.debug("Asked to route message: {}".format(msg.params))
        called = msg.params.get("called")
        if called is None or called == "":
            return False
        if called.isdigit():
            task = RoutingTask(self, msg)
            self.event_loop.create_task(task.routing_job())
        elif called.startswith("stage1-"):
            self.event_loop.create_task(self._retrieve_from_cache_for(msg))
        else:
            return False

    async def _retrieve_from_cache_for(self, msg: Message):
        called = "lateroute/" + msg.params.get("called")
        result = await self._routing_cache.retrieve(called)
        if result is None:
            # This is an invalid entry, answer the message but with invalid result
            msg.result = ""
            self.answer_message(msg, True)
        else:
            msg = ywsd.yate.encode_routing_result(msg, result)
            self.answer_message(msg, True)

    async def store_cache_infos(self, entries: Dict[str, IntermediateRoutingResult]):
        await self._routing_cache.update(entries)


class RoutingTask:
    def __init__(self, yate: YateStage1RoutingEngine, message: Message):
        self._yate = yate
        self._message = message

    async def routing_job(self):
        caller = self._message.params.get("caller")
        called = self._message.params.get("called")
        if caller is None:
            # we do not process messages without a caller
            self._yate.answer_message(self._message, False)
        # TODO: Do we need to clean caller somehow before processing?
        logging.debug("Routing {} to {}".format(caller, called))
        try:
            routing_tree = RoutingTree(caller, called, self._yate.settings)
            async with self._yate.db_engine.acquire() as db_connection:
                await routing_tree.discover_tree(db_connection)
            routing_result, routing_cache_entries = routing_tree.calculate_routing(self._yate.settings.LOCAL_YATE_ID,
                                                                                   self._yate.yates_dict)
            logging.debug("Routing result:\n{}\n{}".format(routing_result, routing_cache_entries))
        except RoutingError as e:
            self._message.params["error"] = e.error_code
            logging.info("Routing {} to {} failed: {}".format(caller, called, e.message))
        await self._yate.store_cache_infos(routing_cache_entries)
        result_message = ywsd.yate.encode_routing_result(self._message, routing_result)
        self._yate.answer_message(result_message, True)


def main():
    parser = argparse.ArgumentParser(description='Yate Stage1 Routing Engine')
    parser.add_argument("--config", type=str, help="Config file to use.", default="routing_engine.yaml")
    parser.add_argument("--verbose", help="Print out debug logs.", action="store_true")

    args = parser.parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig(level=logging.DEBUG)

    settings = Settings(args.config)
    yate_connection = settings.YATE_CONNECTION
    app = YateStage1RoutingEngine(settings=settings, **yate_connection)
    app.run()


if __name__ == "__main__":
    logging.debug("Debug logging enabled.")
    main()
