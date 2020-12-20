import logging
from typing import List

from yate.protocol import Message

from ywsd.objects import User, ActiveCall, Registration, DoesNotExist
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
            value = msg.params.get("sip_" + header.lower())
        headers[header] = value
    return headers


class RoutingTask:
    def __init__(self, yate: "ywsd.engine.YateRoutingEngine", message: Message):
        self._yate = yate
        self._message = message

    def populate_additional_message_parameters(self, headers):
        self._message.params["X-Eventphone-Id"] = headers.get("X-Eventphone-Id", "")

        # Make cdrbuild import the X-Eventphone-Id into the cdr record so that we can grab it from call.cdr
        if "copyparams" in self._message.params:
            self._message.params["copyparams"] += ",X-Eventphone-Id"
        else:
            self._message.params["copyparams"] = "X-Eventphone-Id"

    async def _calculate_stage2_routing(self, caller, called):
        if called.startswith("stage2-"):
            called = called[7:]

        async with self._yate.stage2_db_engine.acquire() as db_connection:
            try:
                target = await User.load_user(called, db_connection)
            except DoesNotExist:
                try:
                    target = await User.load_trunk(called, db_connection)
                except DoesNotExist:
                    return False, False

            if target.type == "static":
                return self._static_target_routing(target)

            locations = await Registration.load_locations_for(
                target, called, db_connection
            )
            if not locations:
                self._message.params["error"] = "offline"
                self._message.params["reason"] = "offline"
                return False, True

            headers = get_headers(self._message)

            # Check if this call should be dropped
            if (
                headers["X-No-Call-Wait"] == "1" or not target.call_waiting
            ) and target.inuse > 0:
                self._message.params["error"] = "busy"
                return False, True
            if await ActiveCall.is_active_call(
                "called", headers["X-Eventphone-Id"], db_connection
            ):
                self._message.params["error"] = "busy"
                return False, True

            # calculate target(s)
            if len(locations) == 1:
                self._message.return_value = locations[0].call_target
                self._message.params["oconnection_id"] = locations[0].oconnection_id
            else:
                self._message.return_value = "fork"
                for i, location in enumerate(locations, start=1):
                    self._message.params["callto.{}".format(i)] = location.call_target
                    self._message.params[
                        "callto.{}.oconnection_id".format(i)
                    ] = location.oconnection_id

            self.populate_additional_message_parameters(headers)
            return True, True

    def _static_target_routing(self, target):
        try:
            separated_target = target.static_target.split(";")
            message_params = self._process_static_target_parameters(
                separated_target[1:]
            )
        except ValueError:
            logging.error(
                f"Encountered invalid static call target:'{target.static_target}'"
            )
            self._message.params["error"] = "failure"
            return False, True
        self._message.return_value = separated_target[0]
        headers = get_headers(self._message)
        self.populate_additional_message_parameters(headers)
        self._message.params.update(message_params)
        return True, True

    @staticmethod
    def _process_static_target_parameters(params: List[str]):
        result = {}
        for param in params:
            key, value = param.split("=", 1)
            result[key] = value
        return result

    @retry_db_offline(count=4, wait_ms=1000)
    async def routing_job(self):
        caller = self._message.params.get("caller")
        called = self._message.params.get("called")

        logging.debug("Doing stage2 routing from {} to {}".format(caller, called))

        if caller is None:
            # we do not process messages without a caller
            self._yate.answer_message(self._message, False)
            return

        success, handled = await self._calculate_stage2_routing(caller, called)
        if success:
            logging.debug(
                "Routing successful. Target is {}".format(self._message.return_value)
            )
        elif handled:
            logging.debug(
                "Routing not successful. Error is {}.".format(
                    self._message.params["error"]
                )
            )
        else:
            logging.debug("Routing not successful, noroute, pass message on")

        self._yate.answer_message(self._message, handled)
