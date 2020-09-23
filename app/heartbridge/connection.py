from fastapi import WebSocket, WebSocketDisconnect
from typing import Any


class HeartBridgeDisconnect(WebSocketDisconnect):
    pass


class HeartBridgeConnection:
    def __init__(self, websocket: WebSocket):
        self._ws = websocket
        self.connection_id = self._ws.headers["sec-websocket-key"]

    def __str__(self):
        return self.connection_id

    async def accept(self):
        await self._ws.accept()

    async def send(self, payload: Any):
        await self._ws.send_json(payload)

    async def receive(self):
        try:
            return await self._ws.receive_json()
        except WebSocketDisconnect as e:
            raise HeartBridgeDisconnect from e

    def close(self):
        self._ws.close()
