from pydantic import BaseModel, validator, ValidationError
from typing import Optional, Literal
import datetime

from .utils import PerformanceId

HeartBridgePayloadValidationException = ValidationError


def time_cannot_be_in_past(cls, v):
    delta = v - datetime.datetime.now(datetime.timezone.utc)
    if delta < datetime.timedelta(minutes=-5):
        raise ValueError("Performance date is in the past")
    elif delta > datetime.timedelta(days=365):
        raise ValueError("Performance date is too far in the future")
    return v


def max_str_len(cls, v, field):
    if len(v) > 64:
        raise ValueError(f"{field} length is > 64")
    return v


def performance_id_is_valid(cls, v):
    if not PerformanceId.is_valid(v):
        raise ValueError("Performance ID is invalid")
    return v


class HeartBridgeBasePayload(BaseModel):
    action: Literal['register', 'subscribe', 'publish', 'update', 'register_return']

    @validator("action")
    def action_is_one_of(cls, v):
        if v not in ['register', 'subscribe', 'publish', 'update', 'register_return']:
            raise ValueError("Invalid action specified")
        return v


class HeartBridgeRegisterPayload(HeartBridgeBasePayload):
    action: Literal['register']
    artist: str
    title: str
    performance_date: datetime.datetime

    _max_str_len = validator("artist", "title", allow_reuse=True)(max_str_len)
    _not_in_past = validator("performance_date", allow_reuse=True)(time_cannot_be_in_past)


class HeartBridgeUpdatePayload(HeartBridgeRegisterPayload):
    action: Literal['update']
    artist: Optional[str] = None
    title: Optional[str] = None
    performance_date: Optional[datetime.datetime]
    token: str


class HeartBridgeSubscribePayload(HeartBridgeBasePayload):
    action: Literal['subscribe']
    performance_id: str

    _performance_id_is_valid = validator("performance_id", allow_reuse=True)(performance_id_is_valid)


class HeartBridgePublishPayload(HeartBridgeBasePayload):
    action: Literal['publish']
    heartrate: int
    token: str


class HeartBridgeRegisterReturnPayload(HeartBridgeBasePayload):
    action: Literal['register_return']
    token: str
    performance_id: str

    _performance_id_is_valid = validator("performance_id", allow_reuse=True)(performance_id_is_valid)
