from pydantic import BaseModel, validator, ValidationError
import datetime

HeartBridgePayloadValidationException = ValidationError


# class HeartBridgePayloadValidationException(ValidationError):
#     pass


class HeartBridgeBasePayload(BaseModel):
    action: str = ...

    @validator("action")
    def action_is_one_of(cls, v):
        if v not in ['register', 'subscribe', 'publish', 'update']:
            raise ValueError("Invalid action specified")
        return v


class HeartBridgeRegisterPayload(HeartBridgeBasePayload):
    artist: str = ...
    title: str = ...
    performance_date: datetime.datetime = ...

    @validator("performance_date")
    def time_cannot_be_in_past(cls, v):
        delta = v - datetime.datetime.now(datetime.timezone.utc)
        if delta < datetime.timedelta(minutes=-5):
            raise ValueError("Performance date is in the past")
        elif delta > datetime.timedelta(days=365):
            raise ValueError("Performance date is too far in the future")
        return v

    @validator("artist", "title")
    def max_str_len(cls, v, field):
        if len(v) > 64:
            raise ValueError(f"{field} length is > 64")
        return v
