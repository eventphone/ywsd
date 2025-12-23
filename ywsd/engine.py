from datetime import datetime
from typing import Optional, Dict
import argparse
import asyncio
import logging
import os
import signal
import traceback

import aiopg.sa
from aiohttp import web

from yate.asyncio import YateAsync
from yate.protocol import Message

import ywsd.yate
from ywsd.objects import Yate, Extension, DoesNotExist
from ywsd import stage1, stage2, statistics
from ywsd.util import class_from_dotted_string, calculate_statistics_aggregates
from ywsd.routing_cache import RoutingCacheBase
from ywsd.routing_tree import IntermediateRoutingResult, RoutingTree, RoutingError
from ywsd.settings import Settings


class YateRoutingEngine(YateAsync):
    def __init__(self, *args, **kwargs):
        self._settings = kwargs.pop("settings")
        self._web_only = kwargs.pop("web_only")
        self._startup_complete_event = kwargs.pop("startup_complete_event", None)
        super().__init__(*args, **kwargs)
        self._shutdown_future = None
        self._routing_db_engine = None
        self._stage2_db_engine = None
        self._redis_pool = None
        self._busy_cache_engine = None
        self._routing_cache: Optional[RoutingCacheBase] = None
        self._yates_dict: Dict[int, Yate] = {}
        self._statistics = None

        if self._settings.WEB_INTERFACE is not None:
            self._web_app = web.Application()
            self._web_app.add_routes([web.get("/stage1", self._web_stage1_handler)])
            self._web_app.add_routes(
                [web.get("/busy_cache", self._web_busy_cache_status)]
            )
            self._web_app.add_routes(
                [web.get("/statistics", self._web_statistics_handler)]
            )
            self._app_runner = web.AppRunner(self._web_app)
        else:
            self._web_app = None

        self.set_termination_handler(self.termination_handler)

    @property
    def settings(self):
        return self._settings

    @property
    def yates_dict(self):
        return self._yates_dict

    @property
    def routing_db_engine(self):
        return self._routing_db_engine

    @property
    def stage2_db_engine(self):
        return self._stage2_db_engine

    @property
    def redis_pool(self):
        return self._redis_pool

    @property
    def busy_cache(self):
        return self._busy_cache_engine

    @staticmethod
    def termination_handler():
        # shutdown hard when connection to yate is lost
        logging.error("Connection to yate lost. Terminating.")
        os._exit(1)

    def run(self):
        if self._web_only:
            logging.info("Starting up in web server only mode")
            asyncio.run(self.main(43))
        else:
            logging.info("Initializing YateAsync engine")
            super().run(self.main)

    def trigger_shutdown(self):
        self._shutdown_future.set_result(True)

    async def main(self, _):
        logging.info("Initializing main application")
        self._shutdown_future = asyncio.get_event_loop().create_future()
        try:
            asyncio.get_event_loop().add_signal_handler(
                signal.SIGINT, lambda: self._shutdown_future.set_result(True)
            )
        except NotImplementedError:
            pass  # Ignore if not implemented. Means this program is running in windows.
        logging.info("Initializing routing cache")
        self._routing_cache = class_from_dotted_string(
            self.settings.CACHE_IMPLEMENTATION
        )(self, self.settings)
        await self._routing_cache.init()

        if self._settings.BUSY_CACHE_IMPLEMENTATION is not None and not self._web_only:
            logging.info("Initializing ywsd busy cache")
            self._busy_cache_engine = class_from_dotted_string(
                self.settings.BUSY_CACHE_IMPLEMENTATION
            )(self, self.settings)
            await self._busy_cache_engine.init()
        else:
            logging.info("Use busy cache from yate CDR in database.")

        if self._settings.REDIS is not None:
            import redis.asyncio as redis

            logging.info("Initializing redis connection")
            self._redis_pool = redis.ConnectionPool.from_url(
                self._settings.REDIS, decode_responses=True
            )

        if self._settings.STATISTICS is not None:
            self._statistics = statistics.Statistics(
                self.redis_pool, self._settings.STATISTICS
            )
            statistics.initialize_statistics(self._statistics)
            logging.info("Initialized statistics module")

        logging.info("Initializing database engine")
        async with aiopg.sa.create_engine(**self._settings.DB_CONFIG) as db_engine:
            self._routing_db_engine = db_engine
            logging.info("Loading remote yates information from DB")
            async with self.routing_db_engine.acquire() as db_connection:
                self._yates_dict = await Yate.load_yates_dict(db_connection)

            logging.info("Initializing stage2 database engine")
            async with aiopg.sa.create_engine(
                **self._settings.STAGE2_DB_CONFIG
            ) as stage2_db_engine:
                self._stage2_db_engine = stage2_db_engine

                # fire up http server if requested
                if self._web_app is not None:
                    bind = self._settings.WEB_INTERFACE.get("bind_address")
                    port = int(self._settings.WEB_INTERFACE.get("port", 9000))
                    await self._app_runner.setup()
                    site = web.TCPSite(self._app_runner, bind, port)
                    await site.start()
                    logging.info(
                        "Webserver ready. Waiting for requests on {}:{}.".format(
                            bind, port
                        )
                    )

                if not self._web_only:
                    logging.info("Registering for routing messages")
                    if not await self.register_message_handler_async(
                        "call.route", self._call_route_handler, 90
                    ):
                        logging.error("Cannot register for call.route. Terminating...")
                        return
                    await self.activate_automatic_bufsize()
                    logging.info(
                        "Yate buffer size defaults to {}".format(
                            self.get_local("bufsize")
                        )
                    )

                logging.info("Ready to route")
                if self._startup_complete_event is not None:
                    self._startup_complete_event.set()
                await self._shutdown_future

        await self._routing_cache.stop()
        if self._web_app is not None:
            await self._app_runner.cleanup()
        self._routing_db_engine = None
        self._stage2_db_engine = None

    def _call_route_handler(self, msg: Message) -> Optional[bool]:
        logging.debug("Asked to route message: {}".format(msg.params))
        called = msg.params.get("called")
        stage2_active = msg.params.get("eventphone_stage2", "0")

        if called is None or called == "":
            return False
        if called.isdigit():
            if (
                msg.params.get("connection_id", "")
                == self.settings.INTERNAL_YATE_LISTENER
                or stage2_active == "1"
            ):
                task = stage2.RoutingTask(self, msg)
            else:
                task = stage1.RoutingTask(self, msg)
            asyncio.create_task(task.routing_job())
        elif called.startswith("stage1-"):
            asyncio.create_task(self._retrieve_from_cache_for(msg))
        elif called.startswith("stage2-"):
            task = stage2.RoutingTask(self, msg)
            asyncio.create_task(task.routing_job())
        else:
            return False

    async def _retrieve_from_cache_for(self, msg: Message):
        called = "lateroute/" + msg.params.get("called")
        try:
            result = await self._routing_cache.retrieve(called)
        except Exception as e:
            backtrace = traceback.format_exc()
            logging.error(
                "An error occurred while retrieving stored routing cache for target %s: %s\nBacktrace: %s",
                called,
                e,
                backtrace,
            )
            raise
        if result is None:
            # This is an invalid entry, answer the message but with invalid result
            msg.result = ""
            self.answer_message(msg, True)
        else:
            msg = ywsd.yate.encode_routing_result(msg, result)
            self.answer_message(msg, True)

    async def store_cache_infos(self, entries: Dict[str, IntermediateRoutingResult]):
        await self._routing_cache.update(entries)

    async def _web_busy_cache_status(self, request):
        if self._busy_cache_engine is None:
            return web.Response(status=404, text="No ywsd busy cache configured.")
        return web.json_response(await self._busy_cache_engine.busy_status())

    async def _web_statistics_handler(self, request):
        scope = request.query.get("scope", "stage1")
        scopes = scope.split(",")
        results = {}
        if "stage1" in scopes or "stage1_agg" in scopes:
            stage1_data = await self._statistics.get_stage1_stats()
            stage1_times = [int(data[1]) for data in stage1_data]
            if "stage1_agg" in scopes:
                results["stage1_agg"] = calculate_statistics_aggregates(stage1_times)
            if "stage1" in scopes:
                results["stage1"] = stage1_data
        if "stage2" in scopes or "stage2_agg" in scopes:
            stage2_data = await self._statistics.get_stage2_stats()
            stage2_times = [int(data[1]) for data in stage2_data]
            if "stage2_agg" in scopes:
                results["stage2_agg"] = calculate_statistics_aggregates(stage2_times)
            if "stage2" in scopes:
                results["stage2"] = stage2_data
        if "query" in scopes or "query_agg" in scopes:
            query_data = await self._statistics.get_query_stats()
            query_times = [int(data[1]) for data in query_data]
            if "query_agg" in scopes:
                results["query_agg"] = calculate_statistics_aggregates(query_times)
            if "query" in scopes:
                results["query"] = query_data

        return web.json_response(results)

    async def _web_stage1_handler(self, request):
        params = request.query

        called = params.get("called")
        caller = params.get("caller")

        if any((caller is None, called is None)):
            return web.Response(
                status=400, text="Provide at least <caller> and <called>"
            )

        # calculate routing tree and results
        routing_tree = None
        routing_result = None
        all_routing_results = None
        routing_cache_entries = {}
        routing_status = "PROCESSING"
        routing_status_details = ""
        try:
            routing_time_start = datetime.now()
            async with self.routing_db_engine.acquire() as db_connection:
                try:
                    caller_extension = await Extension.load_extension(
                        caller, db_connection
                    )
                except DoesNotExist:
                    caller_extension = Extension.create_external(caller)
                caller_params = stage1.RoutingTask.calculate_source_parameters(
                    caller_extension
                )
                routing_tree = RoutingTree(
                    caller_extension, called, caller_params, self.settings
                )
                await routing_tree.discover_tree(db_connection)

            routing_result, routing_cache_entries = routing_tree.calculate_routing(
                self.settings.LOCAL_YATE_ID, self.yates_dict
            )
            all_routing_results = routing_tree.all_routing_results
            routing_status = "OK"
            routing_time_us = int(
                (datetime.now() - routing_time_start).total_seconds() * 1e6
            )
            if (
                routing_time_us
                >= self.settings.ROUTING_TIME_WARNING_THRESHOLD_MS * 1000
            ):
                logging.debug(
                    "Routing tree trace of slow routing operation: %s",
                    routing_tree.serialized_tree(),
                )
            routing_status_details = "Routing took {}us".format(routing_time_us)
        except RoutingError as e:
            routing_status = "ERROR"
            all_routing_results = {}
            routing_status_details = "{}: {}".format(e.error_code, e.message)
        except Exception as e:
            backtrace = traceback.format_exc()
            routing_status = "ERROR"
            all_routing_results = {}
            routing_status_details = (
                "Unexpected Exception while routing:\n{}:Backtrace:\n{}".format(
                    e, backtrace
                )
            )

        json_response_data = {
            "routing_tree": routing_tree.serialized_tree(),
            "main_routing_result": (
                routing_result.serialize() if routing_result is not None else None
            ),
            "all_routing_results": {
                key: result.serialize()
                for key, result in all_routing_results.items()
                if result.is_valid and result.target.target in routing_cache_entries
            },
            "routing_status": routing_status,
            "routing_status_details": routing_status_details,
        }
        return web.json_response(json_response_data)


def main():
    parser = argparse.ArgumentParser(description="Yate Routing Engine")
    parser.add_argument(
        "--config", type=str, help="Config file to use.", default="routing_engine.yaml"
    )
    parser.add_argument("--verbose", help="Print out debug logs.", action="store_true")
    parser.add_argument(
        "--trace", help="Print out debug logs of yate messaging.", action="store_true"
    )
    parser.add_argument(
        "--web-only",
        help="Only start the webserver. Do not connect to yate",
        dest="web_only",
        action="store_true",
    )

    args = parser.parse_args()
    settings = Settings(args.config)

    logging_basic_config_params = {
        "format": "%(asctime)s:%(name)-10s:%(levelname)-8s:%(message)s",
        "datefmt": "%Y-%m-%d:%H:%M:%S",
    }
    if settings.LOG_FILE is not None:
        logging_basic_config_params["filename"] = settings.LOG_FILE
        logging_basic_config_params["filemode"] = "a+"

    if args.verbose or settings.LOG_VERBOSE:
        logging.basicConfig(level=logging.DEBUG, **logging_basic_config_params)
    else:
        logging.basicConfig(level=logging.INFO, **logging_basic_config_params)
    if not args.trace:
        logging.getLogger("yate").setLevel(logging.INFO)

    logging.debug("Debug logging enabled.")

    yate_connection = settings.YATE_CONNECTION
    app = YateRoutingEngine(
        settings=settings, web_only=args.web_only, **yate_connection
    )
    app.run()


if __name__ == "__main__":
    main()
