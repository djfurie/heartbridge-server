import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, BackgroundTasks
from typing import List, Dict
import heartbridgeserver
import json
import logging

# High level class instantiations
app = FastAPI()
hbs = heartbridgeserver.HeartBridgeServer()


class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        connection_id = websocket.headers['sec-websocket-key']

        # Store this socket connection in the connection manager
        self.active_connections[connection_id] = websocket

        # Let the HeartBridgeServer know that a new client has connected
        hbs.connect_handler(connection_id)

    def disconnect(self, websocket: WebSocket):
        connection_id = websocket.headers['sec-websocket-key']
        hbs.disconnect_handler(connection_id)
        self.active_connections.pop(connection_id)

    def broadcast(self, connection_ids, payload):
        logging.debug("Scheduling a broadcast")
        loop = asyncio.get_running_loop()
        for conn_id in connection_ids:
            loop.create_task(self.active_connections[conn_id].send_text(payload))

    async def handle_message(self, websocket: WebSocket, payload: str, background_tasks: BackgroundTasks):
        try:
            connection_id = websocket.headers['sec-websocket-key']
            p = json.loads(payload)

            # Inspect the payload to determine what action should be taken
            action = p['action']

            ret_val = None
            if action == 'publish':
                connection_ids, broadcast_payload = hbs.publish_handler(payload)
                self.broadcast(connection_ids, broadcast_payload)
                ret_val = broadcast_payload
            elif action == 'subscribe':
                connection_ids, broadcast_payload = hbs.subscribe_handler(connection_id, payload)
                logging.debug("%s - %s", connection_ids, broadcast_payload)
                self.broadcast(connection_ids, broadcast_payload)
            elif action == 'register':
                ret_val = hbs.register_handler(payload)
            elif action == 'update':
                ret_val = hbs.update_handler(payload)
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
async def websocket_endpoint(websocket: WebSocket, background_tasks: BackgroundTasks):
    await manager.connect(websocket)

    try:
        while True:
            data = await websocket.receive_text()
            await manager.handle_message(websocket, data, background_tasks)

    except WebSocketDisconnect:
        manager.disconnect(websocket)
