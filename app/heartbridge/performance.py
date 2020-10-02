from .connection import HeartBridgeConnection
from typing import Dict, Optional
import asyncio
import logging
import aioredis


class Performance:
    @classmethod
    async def create(cls, performance_id: str):
        self = Performance(performance_id)
        # Connect to the redis database
        self._redis = await aioredis.create_redis_pool("redis://redis")

        # Start up the listening task
        asyncio.create_task(self._listen_for_events())

        return self

    def __init__(self, performance_id: str):
        self._subscribers: Dict[str, HeartBridgeConnection] = {}
        self._performance_id = performance_id
        self._redis: Optional[aioredis.Redis] = None
        self._subscriber_count_broadcast_channel: str = f"perf:{self._performance_id}:subscriber_event"
        self._heartrate_broadcast_channel: str = f"perf:{self._performance_id}:heartrate_event"

    def __del__(self):
        self._redis.close()

    def __str__(self):
        return self._performance_id

    async def _listen_for_events(self):
        channel, = await self._redis.subscribe(self._broadcast_channel)
        async for msg in channel.iter():
            logging.error("Msg: %s", msg)
            tasks = [conn.send(msg.decode("utf-8")) for conn in self._subscribers.values()]
            await asyncio.gather(*tasks)

    async def add_subscriber(self, conn: HeartBridgeConnection):
        self._subscribers[str(conn)] = conn

        # Increment the subscriber count in redis by 1
        subscriber_count = await self._redis.incr(f"perf:{self._performance_id}:subcnt")

        # Alert all subscribers that the value has changed
        await self.broadcast(subscriber_count)

    async def broadcast(self, payload: str):
        logging.info("[%s] Broadcasting: %s", self._performance_id, payload)
        await self._redis.publish(self._broadcast_channel, payload)

class PerformanceBroadcastRateLimiter:
    pass