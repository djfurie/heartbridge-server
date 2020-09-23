from pydantic import BaseModel, validator, ValidationError

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
    pass
