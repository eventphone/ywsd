import logging

from yate.protocol import Message

from ywsd.objects import User, ActiveCall, DoesNotExist
from ywsd.util import retry_db_offline


HEADER_NAMES = (
    "X-Eventphone-Id",
    "X-No-Call-Wait",
)


def get_headers(msg: Message):
    headers = {}
    for header in HEADER_NAMES:
        value = msg.params.get("osip_" + header)
        if value is None:
            value = msg.params.get("sip_" + header)
        headers[header] = value
    return headers


class RoutingTask:
    def __init__(self, yate: 'ywsd.engine.YateRoutingEngine', message: Message):
        self._yate = yate
        self._message = message

    async def _calculate_stage2_routing(self, caller, called):
        if called.startswith("stage2-"):
            called = called[7:]

        async with self._yate.stage2_db_engine.acquire() as db_connection:
            try:
                target = await User.load_user(called, db_connection)
            except DoesNotExist:
                self._message.params["error"] = "noroute"
                self._message.params["reason"] = "noroute"
                return False

            if target.location is None or target.location == "":
                self._message.params["error"] = "offline"
                self._message.params["reason"] = "offline"
                return False

            headers = get_headers(self._message)
            if (headers["X-No-Call-Wait"] == "1" or not target.call_waiting) and target.inuse > 0:
                self._message.params["error"] = "busy"
                return False
            if await ActiveCall.is_active_call("called", headers["X-Eventphone-Id"], db_connection):
                self._message.params["error"] = "busy"
                return False
            else:
                self._message.return_value = target.location
                self._message.params["oconnection_id"] = target.oconnection_id
                return True

    @retry_db_offline(count=4, wait_ms=1000)
    async def routing_job(self):
        caller = self._message.params.get("caller")
        called = self._message.params.get("called")

        logging.debug("Doing stage2 routing from {} to {}".format(caller, called))

        if caller is None:
            # we do not process messages without a caller
            self._yate.answer_message(self._message, False)
            return

        success = await self._calculate_stage2_routing(caller, called)
        if success:
            logging.debug("Routing successful. Target is {}".format(self._message.return_value))
        else:
            logging.debug("Routing not successful. Error is {}.".format(self._message.params["error"]))

        self._yate.answer_message(self._message, True)