import datetime
import logging
import asyncio
from typing import Dict, Callable, Awaitable, Any

from .connection import HeartBridgeConnection, HeartBridgeDisconnect
from .payload_types import HeartBridgeBasePayload, HeartBridgePayloadValidationException, HeartBridgeRegisterPayload, \
    HeartBridgeUpdatePayload, HeartBridgeSubscribePayload, HeartBridgePublishPayload, HeartBridgeDeletePayload, \
    HeartBridgePerformanceStatusPayload
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
            await self._remove_connection(conn)

    async def _listen_forever(self, conn: HeartBridgeConnection):
        # Listen forever
        while True:
            payload = await conn.receive()
            logging.debug("%s [RX]: %s", conn, payload)
            # Do the callback on message received
            if self._on_message_handler:
                await self._on_message_handler(conn, payload)

    async def _remove_connection(self, conn: HeartBridgeConnection):
        if str(conn) in self._active_connections:
            self._active_connections.pop(str(conn))
        await conn.close()


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
        for perf in self._performances.values():
            await perf.remove_subscriber(conn)

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

        # Calculate the new expiration time
        expiration_time = payload.performance_date + datetime.timedelta(minutes=payload.duration)

        # Save off the details of the performance
        await Performance.save_performance_token(performance_id, token_str.decode("utf-8"), expiration_time)

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

        # Calculate the new expiration time by reparsing the new token and getting the date and duration
        token = PerformanceToken.from_token(token_str, verify=False, verify_nbf=False)
        expiration_time = token.performance_date + datetime.timedelta(minutes=token.duration)

        # Save off the details of the performance
        await Performance.save_performance_token(performance_id, token_str.decode("utf-8"), expiration_time)

        return return_json

    async def delete_handler(self, payload: HeartBridgeDeletePayload):
        # Validate the token and extract the performance_id
        try:
            token = PerformanceToken.from_token(payload.token, verify_nbf=False)
            await Performance.delete_performance(token.performance_id)
        except (PerformanceToken.PerformanceTokenException, Performance.PerformanceIDUnknown) as e:
            return self._format_exception(e)

        return_json = {
            "status": "success"
        }

        return return_json

    async def get_event_details(self, performance_id: str):
        try:
            token_str = await Performance.get_performance_token(performance_id)
        except Performance.PerformanceException as e:
            return self._format_exception(e)

        token = PerformanceToken.from_token(token_str, verify=False, verify_nbf=False)

        return_json = {
            "performance_id": token.performance_id,
            "artist": token.artist,
            "title": token.title,
            "email": token.email,
            "description": token.description,
            "performance_date": token.performance_date,
            "duration": token.duration
        }

        return return_json

    async def get_events(self):
        performances = await Performance.get_all_performance_ids()

        return_json = {
            "performances": [await self.get_event_details(perf) for perf in performances]
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

        await performance.update_heartrate(p.heartrate)

    async def get_performance_status(self, performance_id: str):
        try:
            status = await Performance.get_performance_status(performance_id)
        except Performance.PerformanceException as e:
            return self._format_exception(e)

        ret_json = {
            "status": status
        }
        return ret_json

    async def set_performance_status(self, performance_id: str, payload: HeartBridgePerformanceStatusPayload):
        # Validate the token and extract the performance_id
        try:
            token = PerformanceToken.from_token(payload.token, verify_nbf=False)
        except PerformanceToken.PerformanceTokenException as e:
            return self._format_exception(e)

        # Check if this node is already tracking the given performance
        async with self._lock:
            if token.performance_id not in self._performances:
                # Create the performance
                logging.info("Creating performance: %s", token.performance_id)
                self._performances[token.performance_id] = await Performance.create(token.performance_id)

        performance = self._performances[token.performance_id]

        await performance.set_performance_status(payload.status)

        return {"status": payload.status}
