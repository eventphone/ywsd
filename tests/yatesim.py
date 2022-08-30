import asyncio
import bisect
from collections import defaultdict
from datetime import datetime
import logging

from yate import protocol


class MessageHandler:
    def __init__(self, client, message, prio, filter_name, filter_value):
        self.client = client
        self.message = message
        self.prio = prio
        self.filter_name = filter_name
        self.filter_value = filter_value


class ActiveMessage:
    def __init__(self, data, current_prio, done_future):
        self.current_prio = current_prio
        self.data = data
        self.done_future = done_future


class YateGlobalSim:
    def __init__(self, sock_path):
        self._sock_path = sock_path
        self._server = None
        self._client_id = 0
        self._msg_id = 0
        self._clients = {}
        self._client_handlers = defaultdict(list)
        self._open_messages = {}

    @property
    def path(self):
        return self._sock_path

    async def run(self):
        self._server = await asyncio.start_unix_server(
            self._handle_client, path=self._sock_path
        )

    def find_handler(self, msg_type, min_prio):
        handlers = self._client_handlers.get(msg_type, [])
        for handler in handlers:
            if handler.prio > min_prio:
                return handler

    async def stop(self):
        self._server.close()
        await self._server.wait_closed()

    async def _handle_client(self, reader, writer):
        expect_connect = await reader.readline()
        assert expect_connect == b"%%>connect:global\n"
        cid = self._client_id
        self._client_id += 1
        self._clients[cid] = (reader, writer)

        while True:
            message = await reader.readline()
            message = message.strip()
            logging.debug("[<-%i] %s", cid, repr(message))
            if len(message) == 0:
                break
            parsed_message = protocol.parse_yate_message(message)
            self._handle_client_message(cid, parsed_message)

        self._remove_client(cid)
        writer.close()
        await writer.wait_closed()

    def _remove_client(self, cid):
        for msg_type in self._client_handlers.keys():
            self._client_handlers[msg_type] = list(
                filter(lambda x: x.client != cid, self._client_handlers[msg_type])
            )

    def _send_client(self, cid, data: bytes):
        logging.debug("[->%i] %s", cid, repr(data))
        _, writer = self._clients.get(cid)
        if not data.endswith(b"\n"):
            data += b"\n"
        writer.write(data)

    def _handle_client_message(self, cid, message):
        if isinstance(message, protocol.InstallRequest):
            handler = MessageHandler(
                cid,
                message.name,
                message.priority,
                message.filter_name,
                message.filter_value,
            )
            insert_pos = 0
            for idx, handler in enumerate(self._client_handlers[message.name]):
                if handler.prio <= message.priority:
                    insert_pos = idx
                else:
                    break
            self._client_handlers[message.name].insert(insert_pos, handler)
            response = protocol.InstallConfirm(message.priority, message.name, True)
            self._send_client(cid, response.encode())
        elif isinstance(message, protocol.SetLocalRequest):
            if message.param == "bufsize":
                message.value = "1024"
            response = protocol.SetLocalAnswer(message.param, message.value, True)
            self._send_client(cid, response.encode())
        elif isinstance(message, protocol.Message):
            if message.reply:
                local_msg = self._open_messages.get(message.id)
                if message.processed:
                    if local_msg is not None:
                        local_msg.done_future.set_result(message)
                        del self._open_messages[message.id]
                else:
                    # find next handler
                    handler = self.find_handler(message.name, local_msg.current_prio)
                    if handler is None:
                        # No further handlers. Just return unprocessed message with updated parameters
                        local_msg.done_future.set_result(message)
                        return
                    self._send_client(handler.client, local_msg.data)
                    local_msg.current_prio = handler.prio

    def submit_message(self, msg: protocol.MessageRequest):
        mid = str(self._msg_id)
        self._msg_id += 1
        data = msg.encode(mid, datetime.now().timestamp())
        future = asyncio.get_event_loop().create_future()
        handler = self.find_handler(msg.name, -1)
        if handler is None:
            future.set_result(None)
        self._send_client(handler.client, data)
        self._open_messages[mid] = ActiveMessage(data, handler.prio, future)
        return future
