from .connection import HeartBridgeConnection
from typing import Dict, Optional
import asyncio
import logging
import aioredis
import datetime
import json


class Performance:
    @classmethod
    async def create(cls, performance_id: str):
        self = Performance(performance_id)
        # Connect to the redis database
        self._redis = await aioredis.create_redis_pool("redis://redis")

        # Start up the listening task
        asyncio.create_task(self._listen_for_events(self._heartrate_broadcast_channel))
        asyncio.create_task(self._listen_for_events(self._subscriber_count_broadcast_channel))

        return self

    def __init__(self, performance_id: str):
        self._subscribers: Dict[str, HeartBridgeConnection] = {}
        self._performance_id = performance_id
        self._redis: Optional[aioredis.Redis] = None
        self._subscriber_count_broadcast_channel: str = f"perf:{self._performance_id}:subscriber_event"
        self._heartrate_broadcast_channel: str = f"perf:{self._performance_id}:heartrate_event"

        self._subscriber_count_rate_limiter = PerformanceBroadcastRateLimiter()

    def __del__(self):
        self._redis.close()

    def __str__(self):
        return self._performance_id

    async def _listen_for_events(self, channel):
        ch, = await self._redis.subscribe(channel)
        rate_limiter = PerformanceBroadcastRateLimiter()
        async for msg in ch.iter():
            logging.error("Channel: %s Msg: %s", channel, msg)
            rate_limiter.schedule_rate_limited_coro(self._send_to_all, msg)

    async def _send_to_all(self, payload):
        logging.error("Payload: %s", payload)
        tasks = [conn.send(payload.decode("utf-8")) for conn in self._subscribers.values()]
        await asyncio.gather(*tasks)

    async def add_subscriber(self, conn: HeartBridgeConnection):
        self._subscribers[str(conn)] = conn

        # Increment the subscriber count in redis by 1
        subscriber_count = await self._redis.incr(f"perf:{self._performance_id}:subcnt")

        # Alert all subscribers that the value has changed
        sub_cnt_json = {"action": "subscriber_count_update",
                        "performance_id": self._performance_id,
                        "active_subscriptions": subscriber_count}
        await self.broadcast(self._subscriber_count_broadcast_channel, subscriber_count)

    async def update_heartrate(self, heartrate):
        hr_json = {"action": "heartrate_update",
                   "heartrate": heartrate}
        await self.broadcast(self._heartrate_broadcast_channel, json.dumps(hr_json))

    async def broadcast(self, channel, payload: str):
        logging.info("[%s] Broadcasting: %s", self._performance_id, payload)
        await self._redis.publish(channel, payload)


class PerformanceBroadcastRateLimiter:
    def __init__(self):
        # Things to track:
        # The last time an event was sent
        self._last_event_time = datetime.datetime.utcnow() - datetime.timedelta(seconds=1)
        self._scheduled_task = None

    def schedule_rate_limited_coro(self, f, *args):
        # If there is a task scheduled aleady, cancel it and schedule a new one
        if self._scheduled_task:
            self._scheduled_task.cancel()

        wait_time = 0
        diff_time = datetime.datetime.utcnow() - self._last_event_time
        if diff_time < datetime.timedelta(seconds=1.0):
            wait_time = 1.0 - diff_time.total_seconds()

        self._scheduled_task = asyncio.create_task(self._execute_coro_with_wait(wait_time, f, *args))

    async def _execute_coro_with_wait(self, wait_time: float, f, *args):
        await asyncio.sleep(wait_time)
        await f(*args)
        self._last_event_time = datetime.datetime.utcnow()
        self._scheduled_task = None
