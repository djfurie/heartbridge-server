from pydantic import BaseModel, validator, ValidationError, Field
from typing import Optional, Literal, List
import datetime
import pytz
import logging

from .utils import PerformanceId

HeartBridgePayloadValidationException = ValidationError


def time_cannot_be_in_past(cls, v: datetime.datetime):
    if not v.tzinfo:
        v = pytz.utc.localize(v)

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
    action: Literal["register", "subscribe", "publish", "update", "register_return"]

    @validator("action")
    def action_is_one_of(cls, v):
        if v not in ["register", "subscribe", "publish", "update", "register_return"]:
            raise ValueError("Invalid action specified")
        return v


class HeartBridgePerformanceStatusReturnPayload(BaseModel):
    status: int = Field(
        default=0,
        title="Status",
        description="The current status of the performance, can be one of '0=pending', '1=started', '2=paused', or '3=ended'",
    )


class HeartBridgePerformanceStatusPayload(HeartBridgePerformanceStatusReturnPayload):
    token: str = Field(
        title="Performance Token",
        description="A JWT formatted token that was returned from a prior 'Register' call",
    )


class HeartBridgePerformanceStatusUpdatePayload(
    HeartBridgeBasePayload, HeartBridgePerformanceStatusReturnPayload
):
    pass


class HeartBridgePerformanceDetailsPayload(HeartBridgePerformanceStatusReturnPayload):
    performance_id: str = Field(
        title="Performance ID",
        description="a 6 character string of the form [ABCDEFGHJKLMNPQRSTXYZ23456789]{6}",
    )
    artist: str = Field(
        title="Artist Name",
        description="this is the name of the artist giving the performance",
    )
    title: str = Field(
        title="Performance Title", description="the title of the performance"
    )
    email: Optional[str] = Field(
        title="Contact Email",
        description="Email address for the main contact related to this performance",
    )
    description: Optional[str] = Field(
        title="Performance Description",
        description="A short summary of what the performance is",
    )
    performance_date: datetime.datetime
    duration: int = Field(
        default=90,
        title="Performance Duration",
        description="The length of the performance, specified in minutes",
    )

    _max_str_len = validator("artist", "title", allow_reuse=True)(max_str_len)
    _performance_id_is_valid = validator("performance_id", allow_reuse=True)(
        performance_id_is_valid
    )


class HeartBridgePerformancesPayload(BaseModel):
    performances: List[HeartBridgePerformanceDetailsPayload]


class HeartBridgeDeletePayload(BaseModel):
    token: str = Field(
        title="Performance Token",
        description="A JWT formatted token that was returned from a prior 'Register' call",
    )


class HeartBridgeDeleteReturnPayload(BaseModel):
    status: Literal["success"] = Field(
        title="Success",
        description="Returns success if the token was removed from the database",
    )


class HeartBridgeRegisterPayload(HeartBridgeBasePayload):
    action: Literal["register"] = Field("register", const="register")
    artist: str = Field(
        title="Artist Name",
        description="this is the name of the artist giving the performance",
    )
    title: str = Field(
        title="Performance Title", description="the title of the performance"
    )
    email: Optional[str] = Field(
        title="Contact Email",
        description="Email address for the main contact related to this performance",
    )
    description: Optional[str] = Field(
        title="Performance Description",
        description="A short summary of what the performance is",
    )
    performance_date: datetime.datetime
    duration: int = Field(
        default=90,
        title="Performance Duration",
        description="The length of the performance, specified in minutes",
    )

    _max_str_len = validator("artist", "title", allow_reuse=True)(max_str_len)
    _not_in_past = validator("performance_date", allow_reuse=True)(
        time_cannot_be_in_past
    )


class HeartBridgeUpdatePayload(HeartBridgeRegisterPayload):
    action: Literal["update"]
    artist: Optional[str] = Field(
        title="Artist Name",
        description="this is the name of the artist giving the performance",
    )
    title: Optional[str] = Field(
        title="Performance Title", description="the title of the performance"
    )
    email: Optional[str] = Field(
        title="Contact Email",
        description="Email address for the main contact related to this performance",
    )
    description: Optional[str] = Field(
        title="Performance Description",
        description="A short summary of what the performance is",
    )
    performance_date: Optional[datetime.datetime]
    token: str = Field(
        title="Performance Token",
        description="A JWT formatted token that was returned from a prior 'Register' call",
    )


class HeartBridgeSubscribePayload(HeartBridgeBasePayload):
    action: Literal["subscribe"]
    performance_id: str = Field(
        title="Performance ID",
        description="a 6 character string of the form [ABCDEFGHJKLMNPQRSTXYZ23456789]{6}",
    )

    _performance_id_is_valid = validator("performance_id", allow_reuse=True)(
        performance_id_is_valid
    )


class HeartBridgePublishPayload(HeartBridgeBasePayload):
    action: Literal["publish"]
    heartrate: int = Field(
        title="Heart Rate", description="an integer heartrate in Beats Per Minute"
    )
    token: str = Field(
        title="Performance Token",
        description="A JWT formatted token that was returned from a prior 'Register' call",
    )


class HeartBridgeRegisterReturnPayload(HeartBridgeBasePayload):
    action: Literal["register_return"]
    token: str = Field(
        title="Performance Token",
        description="A JWT formatted token containing details about the performance.  This is used as an authentication token when publishing heartrates",
    )
    performance_id: str = Field(
        title="Performance ID",
        description="a 6 character string of the form [ABCDEFGHJKLMNPQRSTXYZ23456789]{6}.  This ID is used by audience members to specify a performance to subscribe to",
    )

    _performance_id_is_valid = validator("performance_id", allow_reuse=True)(
        performance_id_is_valid
    )
