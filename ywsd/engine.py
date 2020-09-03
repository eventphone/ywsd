from typing import Optional, Dict
import argparse
import asyncio
import logging
import signal
import traceback

import aiopg.sa
from aiohttp import web

from yate.asyncio import YateAsync
from yate.protocol import Message

import ywsd.yate
from ywsd.objects import Yate, Extension, DoesNotExist
from ywsd import stage1, stage2
from ywsd.util import class_from_dotted_string
from ywsd.routing_cache import RoutingCacheBase
from ywsd.routing_tree import IntermediateRoutingResult, RoutingTree, RoutingError
from ywsd.settings import Settings


class YateRoutingEngine(YateAsync):
    def __init__(self, *args, **kwargs):
        self._settings = kwargs.pop("settings")
        self._web_only = kwargs.pop("web_only")
        super().__init__(*args, **kwargs)
        self._shutdown_future = None
        self._routing_db_engine = None
        self._stage2_db_engine = None
        self._routing_cache: Optional[RoutingCacheBase] = None
        self._yates_dict: Dict[int, Yate] = {}

        if self._settings.WEB_INTERFACE is not None:
            self._web_app = web.Application()
            self._web_app.add_routes([web.get("/stage1", self._web_stage1_handler)])
            self._app_runner = web.AppRunner(self._web_app)
        else:
            self._web_app = None

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

    def run(self):
        if self._web_only:
            logging.info("Starting up in web server only mode")
            self.main_task = self.event_loop.create_task(self.main(42))
            self.event_loop.run_until_complete(self.main_task)
            self.event_loop.close()
        else:
            logging.info("Initializing YateAsync engine")
            super().run(self.main)

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

                logging.info("Ready to route")
                await self._shutdown_future

        await self._routing_cache.stop()
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
            self.event_loop.create_task(task.routing_job())
        elif called.startswith("stage1-"):
            self.event_loop.create_task(self._retrieve_from_cache_for(msg))
        elif called.startswith("stage2-"):
            task = stage2.RoutingTask(self, msg)
            self.event_loop.create_task(task.routing_job())
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
            "main_routing_result": routing_result.serialize()
            if routing_result is not None
            else None,
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
        "--web-only",
        help="Only start the webserver. Do not connect to yate",
        dest="web_only",
        action="store_true",
    )

    args = parser.parse_args()
    settings = Settings(args.config)

    logging_basic_config_params = {
        "format": "%(asctime)s:%(name)-10s:%(levelname)-8s:%(message)s",
        "datefmt": "%H:%M:%S",
    }
    if settings.LOG_FILE is not None:
        logging_basic_config_params["filename"] = settings.LOG_FILE
        logging_basic_config_params["filemode"] = "a+"

    if args.verbose or settings.LOG_VERBOSE:
        logging.basicConfig(level=logging.DEBUG, **logging_basic_config_params)
    else:
        logging.basicConfig(level=logging.INFO, **logging_basic_config_params)

    logging.debug("Debug logging enabled.")

    yate_connection = settings.YATE_CONNECTION
    app = YateRoutingEngine(
        settings=settings, web_only=args.web_only, **yate_connection
    )
    app.run()


if __name__ == "__main__":
    main()
