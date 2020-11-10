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
        if isinstance(payload, dict):
            await self._ws.send_json(payload)
        elif isinstance(payload, str):
            await self._ws.send_text(payload)
        else:
            await self._ws.send(payload)

    async def receive(self):
        try:
            return await self._ws.receive_json()
        except WebSocketDisconnect as e:
            raise HeartBridgeDisconnect from e

    def close(self):
        self._ws.close()
