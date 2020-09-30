import asyncio
from fastapi import FastAPI, WebSocket, HTTPException
from typing import List, Dict
from heartbridge import HeartBridgeServer, HeartBridgeConnection
from heartbridge.payload_types import HeartBridgeRegisterReturnPayload, HeartBridgeRegisterPayload, \
    HeartBridgeUpdatePayload
import json
import logging

LOGGING_FORMAT = '%(asctime)s :: %(name)s (%(levelname)s) -- %(message)s'
logging.basicConfig(format=LOGGING_FORMAT, level=logging.DEBUG)

# High level class instantiations
app = FastAPI()
hbserver = HeartBridgeServer()


class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        connection_id = websocket.headers['sec-websocket-key']

        # Store this socket connection in the connection manager
        self.active_connections[connection_id] = websocket

        # Let the HeartBridgeServer know that a new client has connected
        hbserver.connect_handler(connection_id)

    def disconnect(self, websocket: WebSocket):
        connection_id = websocket.headers['sec-websocket-key']
        hbserver.disconnect_handler(connection_id)
        self.active_connections.pop(connection_id)

    def broadcast(self, connection_ids, payload):
        logging.info("Scheduling a broadcast")
        loop = asyncio.get_running_loop()
        for conn_id in connection_ids:
            # Does this websocket client belong to this replica?
            if conn_id in self.active_connections:
                # Send the message
                loop.create_task(self.active_connections[conn_id].send_text(payload))

    async def handle_message(self, websocket: WebSocket, payload: str):
        try:
            connection_id = websocket.headers['sec-websocket-key']
            p = json.loads(payload)

            # Inspect the payload to determine what action should be taken
            action = p['action']

            ret_val = None
            if action == 'publish':
                connection_ids, broadcast_payload = await hbserver.publish_handler(payload)
                self.broadcast(connection_ids, broadcast_payload)
                ret_val = broadcast_payload
            elif action == 'subscribe':
                connection_ids, broadcast_payload = await hbserver.subscribe_handler(connection_id, payload)
                logging.info("%s - %s", connection_ids, broadcast_payload)
                self.broadcast(connection_ids, broadcast_payload)
            elif action == 'register':
                ret_val = hbserver.register_handler(payload)
            elif action == 'update':
                ret_val = hbserver.update_handler(payload)
            else:
                ret_val = json.dumps("Invalid action!")

            if ret_val and len(ret_val) > 0:
                await websocket.send_text(ret_val)
        except json.decoder.JSONDecodeError as e:
            logging.error("Invalid JSON: %s", e)
            logging.error("Payload: %s", payload)
        except Exception as e:
            logging.error("Unhandled Exception! %s", e)
            logging.exception(e)


manager = ConnectionManager()


@app.websocket("/")
async def websocket_endpoint(websocket: WebSocket):
    conn = HeartBridgeConnection(websocket)
    await hbserver.add_connection(conn)


@app.post("/register", response_model=HeartBridgeRegisterReturnPayload)
async def rest_register_endpoint(payload: HeartBridgeRegisterPayload):
    return await hbserver.register_handler(payload)


@app.post("/update", response_model=HeartBridgeRegisterReturnPayload)
async def rest_update_endpoint(payload: HeartBridgeUpdatePayload):
    ret = await hbserver.update_handler(payload)
    if "error" in ret:
        raise HTTPException(status_code=400, detail=ret)
    return ret
