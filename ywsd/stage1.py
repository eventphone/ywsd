import logging
import traceback

from yate.protocol import Message

import ywsd.yate
from ywsd.objects import Extension, DoesNotExist
from ywsd.routing_tree import RoutingTree, RoutingError
from ywsd.util import retry_db_offline, OperationalError


class RoutingTask:
    def __init__(self, yate: "ywsd.engine.YateRoutingEngine", message: Message):
        self._yate = yate
        self._message = message

    async def _sanitize_caller(self, caller, db_connection) -> Extension:
        # if it comes from the internal yate listener, we just trust it
        if (
            self._message.params.get("connection_id", "")
            == self._yate.settings.INTERNAL_YATE_LISTENER
        ):
            return caller
        else:
            try:
                caller_extension = await Extension.load_extension(caller, db_connection)
                username = self._message.params.get("username")
                if username is None:
                    raise RoutingError("noauth", "User needs authentication")
                if username != caller:
                    logging.warning(
                        "User {} tries to act as caller {}. Returned Deny.".format(
                            username, caller
                        )
                    )
                    raise RoutingError(
                        "forbidden", "Invalid authentication for this caller"
                    )
                return caller_extension
            except DoesNotExist:
                # this caller doesn't exist in our database, create an external extension
                return Extension.create_external(caller)

    @staticmethod
    def calculate_source_parameters(source: Extension):
        # push parameters here like faked-caller-id or caller-language
        source_parameters = {}
        if source.outgoing_extension is not None and source.outgoing_extension != "":
            source_parameters["caller"] = source.outgoing_extension
            source_parameters["callername"] = source.outgoing_name
        else:
            # avoid name spoofing
            source_parameters["callername"] = source.name
        if source.lang is not None:
            source_parameters["osip_X-Caller-Language"] = source.lang
        if source.dialout_allowed:
            source_parameters["osip_X-Dialout-Allowed"] = "1"
        return source_parameters

    async def _calculate_stage1_routing(self, caller, called):
        try:
            async with self._yate.routing_db_engine.acquire() as db_connection:
                caller = await self._sanitize_caller(caller, db_connection)
                if caller.type != Extension.Type.EXTERNAL:
                    caller_params = RoutingTask.calculate_source_parameters(caller)
                else:
                    caller_params = {}

                logging.debug("Routing {} to {}".format(caller, called))
                routing_tree = RoutingTree(
                    caller, called, caller_params, self._yate.settings
                )
                await routing_tree.discover_tree(db_connection)

            routing_result, routing_cache_entries = routing_tree.calculate_routing(
                self._yate.settings.LOCAL_YATE_ID, self._yate.yates_dict
            )
            logging.debug(
                "Routing result:\n{}\n{}".format(routing_result, routing_cache_entries)
            )

            await self._yate.store_cache_infos(routing_cache_entries)
            return ywsd.yate.encode_routing_result(self._message, routing_result), True
        except RoutingError as e:
            if e.error_code != "noroute":
                self._message.params["error"] = e.error_code
                logging.info(
                    "Routing {} to {} failed: {}".format(caller, called, e.message)
                )
                return self._message, True
            else:
                # We decided that we do not handle the noroute case and give others (regexroute) a chance but
                # populate the caller parameters
                logging.debug(
                    "Routing {} to {} returned noroute, populate caller params and pass on".format(
                        caller, called
                    )
                )
                self._message.params.update(caller_params)
                return self._message, False
        except Exception as e:
            if type(e) == OperationalError:
                raise  # this is a database error and the routing will be re-tried
            backtrace = traceback.format_exc()
            logging.error(
                "An error occurred while routing {} to {}: {}\nBacktrace:".format(
                    caller, called, e, backtrace
                )
            )
            self._message.params["error"] = "failure"
            return self._message, True

    @retry_db_offline(count=4, wait_ms=1000)
    async def routing_job(self):
        caller = self._message.params.get("caller")
        called = self._message.params.get("called")
        if caller is None:
            # we do not process messages without a caller
            self._yate.answer_message(self._message, False)
        result_message, handled = await self._calculate_stage1_routing(caller, called)
        self._yate.answer_message(result_message, handled)
