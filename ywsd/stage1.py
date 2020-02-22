import logging

from yate.protocol import Message

import ywsd.yate
from ywsd.objects import Extension
from ywsd.routing_tree import RoutingTree, RoutingError


class RoutingTask:
    def __init__(self, yate: 'ywsd.engine.YateRoutingEngine', message: Message):
        self._yate = yate
        self._message = message

    async def routing_job(self):
        caller = self._message.params.get("caller")
        called = self._message.params.get("called")
        if caller is None:
            # we do not process messages without a caller
            self._yate.answer_message(self._message, False)
        # TODO: Do we need to clean caller somehow before processing?
        if self._message.params.get("connection_id", "") != self._yate.settings.INTERNAL_YATE_LISTENER:
            caller = Extension.create_external(caller)

        logging.debug("Routing {} to {}".format(caller, called))
        try:
            routing_tree = RoutingTree(caller, called, self._yate.settings)
            async with self._yate.routing_db_engine.acquire() as db_connection:
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