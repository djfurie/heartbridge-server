from .connection import HeartBridgeConnection
from typing import Dict, Optional, List
import asyncio
import logging
import aioredis
import datetime
import json


class Performance:
    class PerformanceException(Exception):
        pass

    class PerformanceIDUnknown(PerformanceException):
        pass

    @classmethod
    async def create(cls, performance_id: str):
        self = Performance(performance_id)
        # Connect to the redis database
        self._redis = await aioredis.create_redis_pool("redis://redis")

        # Start up the listening task
        asyncio.create_task(self._listen_for_events(self._heartrate_broadcast_channel))
        asyncio.create_task(
            self._listen_for_events(self._subscriber_count_broadcast_channel)
        )
        asyncio.create_task(
            self._listen_for_events(self._performance_status_broadcast_channel)
        )

        return self

    def __init__(self, performance_id: str):
        self._subscribers: Dict[str, HeartBridgeConnection] = {}
        self._performance_id = performance_id
        self._redis: Optional[aioredis.Redis] = None

        self._subscriber_count_broadcast_channel: str = (
            f"perf:{self._performance_id}:subscriber_event"
        )
        self._heartrate_broadcast_channel: str = (
            f"perf:{self._performance_id}:heartrate_event"
        )
        self._performance_status_broadcast_channel: str = (
            f"perf:{self._performance_id}:status_event"
        )

        self._subscriber_count_key: str = f"perf:{self._performance_id}:subcnt"
        self._performance_status_key: str = f"perf:{self._performance_id}:status"

        self._subscriber_count_rate_limiter = PerformanceBroadcastRateLimiter()

    def __del__(self):
        self._redis.close()

    def __str__(self):
        return self._performance_id

    async def _listen_for_events(self, channel):
        (ch,) = await self._redis.subscribe(channel)
        rate_limiter = PerformanceBroadcastRateLimiter()
        async for msg in ch.iter():
            logging.debug("Channel: %s Msg: %s", channel, msg)
            rate_limiter.schedule_rate_limited_coro(self._send_to_all, msg)

    async def _send_to_all(self, payload):
        logging.debug("Payload: %s", payload)
        tasks = [
            conn.send(payload.decode("utf-8")) for conn in self._subscribers.values()
        ]
        await asyncio.gather(*tasks)

    async def add_subscriber(self, conn: HeartBridgeConnection):
        self._subscribers[str(conn)] = conn

        # Increment the subscriber count in redis by 1
        subscriber_count = await self._redis.incr(self._subscriber_count_key)

        # Alert all subscribers that the value has changed
        sub_cnt_json = {
            "action": "subscriber_count_update",
            "performance_id": self._performance_id,
            "active_subscriptions": subscriber_count,
        }
        await self.broadcast(
            self._subscriber_count_broadcast_channel, json.dumps(sub_cnt_json)
        )

        # Subscribers should also get the current performance status immediately after subscribing
        status = await self.get_performance_status(self._performance_id)
        status_json = {"action": "performance_status_update", "status": status}
        await conn.send(status_json)

    async def remove_subscriber(self, conn: HeartBridgeConnection):
        if str(conn) in self._subscribers:
            subscriber_count = await self._redis.decr(self._subscriber_count_key)

            if subscriber_count == 0:
                await self._redis.delete(self._subscriber_count_key)

            # Alert all subscribers that the value has changed
            sub_cnt_json = {
                "action": "subscriber_count_update",
                "performance_id": self._performance_id,
                "active_subscriptions": subscriber_count,
            }
            await self.broadcast(
                self._subscriber_count_broadcast_channel, json.dumps(sub_cnt_json)
            )

    async def update_heartrate(self, heartrate):
        hr_json = {"action": "heartrate_update", "heartrate": heartrate}
        await self.broadcast(self._heartrate_broadcast_channel, json.dumps(hr_json))

    async def broadcast(self, channel, payload: str):
        logging.debug("[%s] Broadcasting: %s", self._performance_id, payload)
        await self._redis.publish(channel, payload)

    @staticmethod
    async def save_performance_token(
        performance_id: str, token: str, expire_at: datetime.datetime = None
    ):
        token_key = f"perf:{performance_id}:token"
        status_key = f"perf:{performance_id}:status"
        redis = await aioredis.create_redis_pool("redis://redis")
        await redis.set(token_key, token)
        await redis.set(status_key, 0)

        if expire_at:
            await redis.expireat(token_key, expire_at.timestamp())
            await redis.expireat(status_key, expire_at.timestamp())

        redis.close()

    @staticmethod
    async def get_performance_token(performance_id: str) -> str:
        redis = await aioredis.create_redis_pool("redis://redis")
        token = await redis.get(f"perf:{performance_id}:token")
        redis.close()

        if not token:
            raise Performance.PerformanceIDUnknown(
                f"Performance ID {performance_id} is not registered"
            )

        return token

    @staticmethod
    async def get_all_performance_ids() -> List[str]:
        redis = await aioredis.create_redis_pool("redis://redis")
        keys = await redis.keys("perf:*:token")
        keys = [x[5:11].decode("utf-8") for x in keys]
        redis.close()
        return keys

    @staticmethod
    async def delete_performance(performance_id: str) -> bool:
        redis = await aioredis.create_redis_pool("redis://redis")
        removed = await redis.delete(f"perf:{performance_id}:token")
        redis.close()

        if removed == 0:
            raise Performance.PerformanceIDUnknown(
                f"Performance ID {performance_id} is not registered"
            )

        return removed

    @staticmethod
    async def get_performance_status(performance_id: str) -> int:
        redis = await aioredis.create_redis_pool("redis://redis")
        status = await redis.get(f"perf:{performance_id}:status")
        redis.close()

        if not status:
            raise Performance.PerformanceIDUnknown(
                f"Performance ID {performance_id} is not registered"
            )

        return int(status)

    async def set_performance_status(self, status: int):
        await self._redis.set(self._performance_status_key, status)
        status_json = {"action": "performance_status_update", "status": status}
        await self.broadcast(
            self._performance_status_broadcast_channel, json.dumps(status_json)
        )


class PerformanceBroadcastRateLimiter:
    def __init__(self):
        # Things to track:
        # The last time an event was sent
        self._last_event_time = datetime.datetime.utcnow() - datetime.timedelta(
            seconds=1
        )
        self._scheduled_task = None

    def schedule_rate_limited_coro(self, f, *args):
        # If there is a task scheduled aleady, cancel it and schedule a new one
        if self._scheduled_task:
            self._scheduled_task.cancel()

        wait_time = 0
        diff_time = datetime.datetime.utcnow() - self._last_event_time
        if diff_time < datetime.timedelta(seconds=1.0):
            wait_time = 1.0 - diff_time.total_seconds()
        else:
            self._last_event_time = datetime.datetime.utcnow()

        self._scheduled_task = asyncio.create_task(
            self._execute_coro_with_wait(wait_time, f, *args)
        )

    async def _execute_coro_with_wait(self, wait_time: float, f, *args):
        await asyncio.sleep(wait_time)
        await f(*args)
        self._last_event_time = datetime.datetime.utcnow()
        self._scheduled_task = None
