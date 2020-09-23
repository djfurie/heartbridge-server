import json
import jwt
import datetime
import random
import logging
import asyncio
import aioredis
from typing import List, Dict, Tuple, Optional

LOGGING_FORMAT = '%(asctime)s :: %(name)s (%(levelname)s) -- %(message)s'
logging.basicConfig(format=LOGGING_FORMAT, level=logging.WARNING)

# Secret key for signing performer tokens
HEARTBRIDGE_SECRET = "hbsecretkey"

# Number of seconds in a day
SECS_IN_HOUR = 60 * 60
SECS_IN_DAY = 24 * SECS_IN_HOUR


class PerformanceId:
    # Parameters for generating performance ids
    VALID_PERFORMANCE_ID_CHARACTERS = "ABCDEFGHJKLMNPQRSTXYZ23456789"
    PERFORMANCE_ID_LENGTH = 6

    # Helper function for generation of performance_ids
    @staticmethod
    def generate() -> str:
        performance_id = ""
        while len(performance_id) < PerformanceId.PERFORMANCE_ID_LENGTH:
            performance_id += PerformanceId.VALID_PERFORMANCE_ID_CHARACTERS[
                random.randint(0, len(PerformanceId.VALID_PERFORMANCE_ID_CHARACTERS) - 1)]
        return performance_id

    # Helper function for validating a performance_id
    @staticmethod
    def is_valid(performance_id: str) -> bool:
        if len(performance_id) != PerformanceId.PERFORMANCE_ID_LENGTH:
            return False

        for c in performance_id:
            if c not in PerformanceId.VALID_PERFORMANCE_ID_CHARACTERS:
                return False

        return True


class PerformanceToken:
    class PerformanceTokenException(Exception):
        pass

    class PerformanceTokenDateException(PerformanceTokenException):
        pass

    def __init__(self, artist: str = "", title: str = "", performance_date: str = "", performance_id: str = ""):
        self.artist = artist
        self.title = title
        self.performance_date = int(performance_date)
        self.performance_id = performance_id

    @classmethod
    def from_json(cls, data: str):
        p = json.loads(data)
        return PerformanceToken.from_dict(p)

    @classmethod
    def from_dict(cls, data: dict):
        return cls(artist=data.setdefault('artist', ""),
                   title=data.setdefault('title', ""),
                   performance_date=data.setdefault('performance_date', ""),
                   performance_id=data.setdefault('performance_id', ""))

    @classmethod
    def from_token(cls, data: str, verify: bool = True, verify_nbf: bool = True, key=HEARTBRIDGE_SECRET):
        # Attempt to verify/decode the token
        token_data = jwt.decode(data, verify=verify, options={'verify_nbf': verify_nbf}, key=key)
        return PerformanceToken.from_dict(token_data)

    def generate(self):
        # Validate that the date is legit (validity is from 5 minutes in the past to any time in the future)
        date_now = int(datetime.datetime.now().timestamp())
        start_date = self.performance_date
        exp_date = start_date + SECS_IN_DAY
        nbf_date = start_date - SECS_IN_HOUR
        if date_now - 300 > start_date:
            raise PerformanceToken.PerformanceTokenDateException(
                f"Date {start_date} is too far in the past")

        # Validate required fields are filled in with valid values
        if not PerformanceId.is_valid(self.performance_id):
            raise PerformanceToken.PerformanceTokenException("Performance Id is invalid!")

        # Assemble the token contents
        token_payload = {'artist': self.artist,
                         'title': self.title,
                         'nbf': nbf_date,
                         'exp': exp_date,
                         'iat': date_now,
                         'performance_date': start_date,
                         'performance_id': self.performance_id}

        # Encode the token
        token = jwt.encode(token_payload,
                           HEARTBRIDGE_SECRET,
                           algorithm='HS256')
        return token


class HeartBridgeServer:
    def __init__(self):
        self._storage = HeartBridgeStorage()
        self._broker = PerformanceBroker()

    def connect_handler(self, connection_id: str):
        logging.info("Connected: %s", connection_id)
        return

    def disconnect_handler(self, connection_id: str):
        logging.info("Disconnected: %s", connection_id)
        # TODO: Terminate all subscriptions for this connection_id
        return

    async def subscribe_handler(self, connection_id: str, payload: str) -> Tuple[List[str], str]:
        p = json.loads(payload)
        performance_id = p['performance_id']

        logging.info("Subscribe: conn_id: %s -> perf_id: %s", connection_id, performance_id)

        # Set up subscriptions to expire after 24 hours
        expiration_time = int(datetime.datetime.now().timestamp()) + SECS_IN_DAY

        subscribers = self._storage.add_subscription(performance_id, connection_id, expiration_time)
        await self._broker.log_subscribe(performance_id)
        await self._broker.subscribe(performance_id)
        return subscribers, json.dumps({
            "action": "subscriber_count_update",
            "performance_id": performance_id,
            "active_subcriptions": len(subscribers)
        })

    def register_handler(self, payload: str) -> str:
        # Generate a new performance id to use
        performance_id = PerformanceId.generate()

        # TODO: Validate that the performance id isn't a duplicate

        try:
            token = PerformanceToken.from_json(payload)
            token.performance_id = performance_id
            token_str = token.generate()
        except PerformanceToken.PerformanceTokenException as e:
            logging.warning("Invalid token data: %s", e)
            return json.dumps({"error": str(e)})

        return_json = {
            'action': 'register_return',
            'token': token_str.decode('utf-8'),
            'performance_id': performance_id
        }

        return json.dumps(return_json)

    def update_handler(self, payload: str) -> str:
        p = json.loads(payload)

        logging.info("Update: %s", p)

        # Check validity of the provided token
        try:
            token = PerformanceToken.from_token(p['token'], verify_nbf=False)
        except jwt.exceptions.DecodeError as e:
            logging.error(e)
            return json.dumps({"error": str(e)})

        # Use any of the provided fields to update the token (performance_id must remain the same)
        if 'artist' in p:
            token.artist = p['artist']
        if 'title' in p:
            token.title = p['title']
        if 'performance_date' in p:
            token.performance_date = int(p['performance_date'])

        try:
            token_str = token.generate()
        except PerformanceToken.PerformanceTokenException as e:
            logging.warning("Invalid token data: %s", e)
            return json.dumps({"error": str(e)})

        return_json = {
            "action": "register_return",
            "token": token_str.decode("utf-8"),
            "performance_id": token.performance_id
        }

        return json.dumps(return_json)

    async def publish_handler(self, payload: str) -> Tuple[List[str], str]:
        p = json.loads(payload)

        # Check validity of provided token
        try:
            token = PerformanceToken.from_token(p['token'])
        except jwt.exceptions.DecodeError as e:
            logging.error(e)
            return [], json.dumps({"error": str(e)})

        logging.debug("Publish: %s", token.performance_id)

        await self._broker.publish(token.performance_id, p['heartrate'])
        subs = self._storage.get_subscriptions(token.performance_id)
        return subs, json.dumps({
            "action": "heartrate_update",
            "heartrate": p['heartrate']
        })


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
