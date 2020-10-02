import json
import datetime
import random
import logging
import asyncio
import aioredis
from typing import List, Dict, Tuple, Optional, Callable, Awaitable, Any

from . import utils
from .connection import HeartBridgeConnection, HeartBridgeDisconnect
from .payload_types import HeartBridgeBasePayload, HeartBridgePayloadValidationException, HeartBridgeRegisterPayload, \
    HeartBridgeUpdatePayload, HeartBridgeSubscribePayload, HeartBridgePublishPayload
from .token_issuer import PerformanceTokenIssuer, PerformanceToken
from .performance import Performance


class HeartBridgeConnectionManager:
    """
    Keep track of active connections and dispatch events via callbacks
    """
    ConnectCallBackType = Callable[[HeartBridgeConnection], Awaitable[None]]
    DisconnectCallBackType = Callable[[HeartBridgeConnection], Awaitable[None]]
    MessageCallBackType = Callable[[HeartBridgeConnection, Any], Awaitable[None]]

    def __init__(self,
                 on_connect_handler: ConnectCallBackType = None,
                 on_disconnect_handler: DisconnectCallBackType = None,
                 on_message_handler: MessageCallBackType = None):
        # Initialize the list of connections
        self._active_connections: Dict[str, HeartBridgeConnection] = {}

        # Store the callbacks
        self._on_connect_handler: HeartBridgeConnectionManager.ConnectCallBackType = on_connect_handler
        self._on_disconnect_handler: HeartBridgeConnectionManager.DisconnectCallBackType = on_disconnect_handler
        self._on_message_handler: HeartBridgeConnectionManager.MessageCallBackType = on_message_handler

    async def add_connection(self, conn: HeartBridgeConnection):
        """ Store the connection in the dict of active connections and wait for data """
        self._active_connections[str(conn)] = conn

        # Accept the websocket connection
        await conn.accept()
        logging.info("Connected: %s", conn)

        # Fire the on_connect_handler callback
        if self._on_connect_handler:
            await self._on_connect_handler(conn)

        # Start up the listen loop
        try:
            await self._listen_forever(conn)
        except HeartBridgeDisconnect:
            # The connection has been terminated
            logging.info("Disconnected: %s", conn)
            if self._on_disconnect_handler:
                await self._on_disconnect_handler(conn)

            # Remove the connection
            self._remove_connection(conn)

    async def _listen_forever(self, conn: HeartBridgeConnection):
        # Listen forever
        while True:
            payload = await conn.receive()
            logging.debug("%s [RX]: %s", conn, payload)
            # Do the callback on message received
            if self._on_message_handler:
                await self._on_message_handler(conn, payload)

    def _remove_connection(self, conn: HeartBridgeConnection):
        if str(conn) in self._active_connections:
            self._active_connections.pop(str(conn))

    def close_connection(self, conn: HeartBridgeConnection):
        conn.close()


class HeartBridgeServer:
    def __init__(self):
        self._connection_mgr = HeartBridgeConnectionManager(on_connect_handler=self.on_connect_handler,
                                                            on_disconnect_handler=self.on_disconnect_handler,
                                                            on_message_handler=self.on_message_handler)
        self._performances: Dict[str, Performance] = {}
        self._lock = asyncio.Lock()

    async def add_connection(self, conn: HeartBridgeConnection):
        await self._connection_mgr.add_connection(conn)

    async def on_connect_handler(self, conn: HeartBridgeConnection):
        logging.debug("on_connect_handler not implemented")

    async def on_disconnect_handler(self, conn: HeartBridgeConnection):
        logging.debug("on_disconnect_handler not implemented")

    async def on_message_handler(self, conn: HeartBridgeConnection, payload: Any):
        """ Dispatch messages from connected clients to the appropriate action handler """
        try:
            p = HeartBridgeBasePayload(**payload)
        except HeartBridgePayloadValidationException as e:
            self._ws_return_exception(conn, e)
            return

        logging.info("[%s] Handle Action: %s", conn, p.json())

        if p.action == "publish":
            await self.publish_handler(conn, payload)
        elif p.action == "subscribe":
            await self.subscribe_handler(conn, payload)
        elif p.action == "register":
            try:
                # Attempt to unpack the payload as a Register command
                p = HeartBridgeRegisterPayload(**payload)
            except HeartBridgePayloadValidationException as e:
                self._ws_return_exception(conn, e)
                return

            # Dispatch to the handler
            ret_val = await self.register_handler(p)

            # Return the response via websocket
            await conn.send(ret_val)
        elif p.action == "update":
            # Attempt to unpack the payload as an Update command
            try:
                p = HeartBridgeUpdatePayload(**payload)
            except HeartBridgePayloadValidationException as e:
                self._ws_return_exception(conn, e)
                return

            # Dispatch to the handler
            try:
                ret_val = await self.update_handler(p)
            except PerformanceToken.PerformanceTokenException as e:
                self._ws_return_exception(conn, e)
                return

            # Return the response via websocket
            await conn.send(ret_val)

    @staticmethod
    def _ws_return_exception(conn: HeartBridgeConnection, e: Exception):
        logging.error("[%s] Exception: %s", conn, e)
        error = {"error": str(e)}
        asyncio.get_running_loop().create_task(conn.send(error))

    @staticmethod
    def _format_exception(e: Exception):
        logging.error("Exception: %s", e)
        return {"error": str(e)}

    async def register_handler(self, payload: HeartBridgeRegisterPayload):
        # Get a new performance id and token from the token issuer
        try:
            performance_id, token_str = PerformanceTokenIssuer.register_performance(payload)
        except PerformanceToken.PerformanceTokenException as e:
            return self._format_exception(e)

        # Pack and return the token and performance id to the requester
        return_json = {
            'action': 'register_return',
            'token': token_str.decode('utf-8'),
            'performance_id': performance_id
        }

        return return_json

    async def update_handler(self, payload: HeartBridgeUpdatePayload):

        logging.info("Update: %s", payload)

        try:
            performance_id, token_str = PerformanceTokenIssuer.update_performance_token(payload)
        except PerformanceToken.PerformanceTokenException as e:
            return self._format_exception(e)

        # Pack and return the new token and the existing performance id
        return_json = {
            "action": "register_return",
            "token": token_str.decode("utf-8"),
            "performance_id": performance_id
        }

        return return_json

    async def subscribe_handler(self, conn: HeartBridgeConnection, payload: Any):
        # Attempt to unpack the payload as a Subscribe command
        try:
            p = HeartBridgeSubscribePayload(**payload)
        except HeartBridgePayloadValidationException as e:
            self._ws_return_exception(conn, e)
            return

        # Check if this node is already tracking the given performance
        async with self._lock:
            if p.performance_id not in self._performances:
                # Create the performance
                logging.info("Creating performance: %s", p.performance_id)
                self._performances[p.performance_id] = await Performance.create(p.performance_id)

        logging.debug("Subscribe: %s - %s", conn, p.performance_id)
        performance = self._performances[p.performance_id]
        await performance.add_subscriber(conn)

    async def publish_handler(self, conn: HeartBridgeConnection, payload: Any):
        # Attempt to unpack the payload as a Publish command
        try:
            p = HeartBridgePublishPayload(**payload)
        except HeartBridgePayloadValidationException as e:
            self._ws_return_exception(conn, e)
            return

        # Check validity of provided token
        try:
            token = PerformanceToken.from_token(p.token)
        except PerformanceToken.PerformanceTokenException as e:
            self._ws_return_exception(conn, e)
            return

        # Check if this node is already tracking the given performance
        async with self._lock:
            if token.performance_id not in self._performances:
                # Create the performance
                logging.info("Creating performance: %s", token.performance_id)
                self._performances[token.performance_id] = await Performance.create(token.performance_id)

        performance = self._performances[token.performance_id]

        # TODO: Publish the heartrate to the Performance
        hr_json = {"action": "heartrate_update",
                   "heartrate": p.heartrate}
        await performance.broadcast(json.dumps(hr_json))


class HeartBridgeStorage:
    """ In memory storage for tracking connections to this node """

    def __init__(self):
        self._performances: Dict[str, List[str]] = {}
        self._connections: Dict[str, List[str]] = {}

    def add_subscription(self, performance_id: str, connection_id: str, expiration: int):
        if performance_id not in self._performances:
            self._performances[performance_id] = []

        if connection_id not in self._connections:
            self._connections[connection_id] = []

        self._performances[performance_id].append(connection_id)
        self._connections[connection_id].append(performance_id)
        return self._performances[performance_id]

    def get_subscriptions(self, performance_id: str) -> List[str]:
        return self._performances.setdefault(performance_id, [])

    def remove_subscription(self, performance_id: str, connection_id: str):
        # Need to remake the list of connections by excluding the connection to be removed...
        self._performances[performance_id][:] = [x for x in self._performances[performance_id] if x != connection_id]

    # Remove a given connection id and all of the subscriptions associated with it
    def remove_connection(self, connection_id: str):
        for perf_id in self._connections['connection_id']:
            self.remove_subscription(perf_id, connection_id)


class PerformanceBroker:
    """ Shuffles events between nodes """

    def __init__(self):
        self._r: Optional[aioredis.Redis] = None

    async def connect(self):
        # self._r = redis.Redis(host='redis')
        self._r = await aioredis.create_redis_pool("redis://redis")

    async def log_subscribe(self, performance_id: str):
        if not self._r:
            await self.connect()

        key = f"perf:{performance_id}:subcnt"
        return await self._r.incr(key)

    async def publish(self, performance_id: str, heart_rate: int):
        if not self._r:
            await self.connect()

        key = f"perf:{performance_id}:hr"

        # Publish the heartrate
        await self._r.publish(key, heart_rate)

    async def subscribe(self, performance_id: str):
        if not self._r:
            await self.connect()

        # Set up a subscription to all of the channels related to the given performance id
        sub, = await self._r.psubscribe(f"perf:{performance_id}:*")

        async def listener(channel):
            logging.error("Starting listener: %s", str(channel))
            async for message in channel.iter():
                logging.error(message)

        asyncio.get_running_loop().create_task(listener(sub))

# class HeartBridgeStorageRedis(HeartBridgeStorage):
#     """ Storage driver for using a redis backend """
#
#     def __init__(self):
#         logging.info("Connecting to Redis")
#         self._r = redis.Redis(host='redis')
#
#     def add_subscription(self, performance_id: str, connection_id: str, expiration: int):
#         key = f"perf:{performance_id}"
#         self._r.lpush(key, connection_id)
#         ret = self._r.lrange(key, 0, -1)
#         ret = [x.decode('utf-8') for x in ret]
#         logging.error("%s - %s", key, str(ret))
#         return ret
#
#     def get_subscriptions(self, performance_id: str) -> List[str]:
#         key = f"perf:{performance_id}"
#         return [x.decode('utf-8') for x in self._r.lrange(key, 0, -1)]
#
#     def remove_subscription(self, performance_id: str, connection_id: str):
#         pass
#
#     def remove_connection(self, connection_id: str):
#         pass
