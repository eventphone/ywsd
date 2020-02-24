import logging

from yate.protocol import Message

import ywsd.yate
from ywsd.objects import Extension, DoesNotExist
from ywsd.routing_tree import RoutingTree, RoutingError
from ywsd.util import retry_db_offline


class RoutingTask:
    def __init__(self, yate: 'ywsd.engine.YateRoutingEngine', message: Message):
        self._yate = yate
        self._message = message

    async def _sanitize_caller(self, caller, db_connection):
        # if it comes from the internal yate listener, we just trust it
        if self._message.params.get("connection_id", "") == self._yate.settings.INTERNAL_YATE_LISTENER:
            return caller
        else:
            try:
                caller_extension = await Extension.load_extension(caller, db_connection)
                username = self._message.params.get("username")
                if username is None:
                    raise RoutingError("noauth", "User needs authentication")
                if username != caller:
                    logging.warning("User {} tries to act as caller {}. Returned Deny.".format(username, caller))
                    raise RoutingError("forbidden", "Invalid authentication for this caller")
                return caller_extension
            except DoesNotExist:
                # this caller doesn't exist in our database, create an external extension
                return Extension.create_external(caller)

    async def _calculate_stage1_routing(self, caller, called):
        try:
            async with self._yate.routing_db_engine.acquire() as db_connection:
                caller = await self._sanitize_caller(caller, db_connection)

                logging.debug("Routing {} to {}".format(caller, called))
                routing_tree = RoutingTree(caller, called, self._yate.settings)
                await routing_tree.discover_tree(db_connection)

            routing_result, routing_cache_entries = routing_tree.calculate_routing(self._yate.settings.LOCAL_YATE_ID,
                                                                                   self._yate.yates_dict)
            logging.debug("Routing result:\n{}\n{}".format(routing_result, routing_cache_entries))

            await self._yate.store_cache_infos(routing_cache_entries)
            return ywsd.yate.encode_routing_result(self._message, routing_result)
        except RoutingError as e:
            self._message.params["error"] = e.error_code
            logging.info("Routing {} to {} failed: {}".format(caller, called, e.message))
            return self._message

    @retry_db_offline(count=4, wait_ms=1000)
    async def routing_job(self):
        caller = self._message.params.get("caller")
        called = self._message.params.get("called")
        if caller is None:
            # we do not process messages without a caller
            self._yate.answer_message(self._message, False)
        result_message = await self._calculate_stage1_routing(caller, called)
        self._yate.answer_message(result_message, True)
