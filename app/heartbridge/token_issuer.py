import random

import jwt
import json
import datetime
import os
from .payload_types import HeartBridgeRegisterPayload, HeartBridgeUpdatePayload
from typing import Union
from .utils import PerformanceId
import logging

# Secret key for signing performer tokens
HEARTBRIDGE_SECRET = os.environ.setdefault("HEARTBRIDGE_SECRET", "hbsecretkey")


class PerformanceTokenIssuer:
    @staticmethod
    def register_performance(p: HeartBridgeRegisterPayload) -> [str, str]:
        # Populate the bulk of the required fields for token generation
        token = PerformanceToken.from_register_payload(p)

        # Populate with a new performance id
        token.performance_id = PerformanceId.generate()

        # Generate the token string
        return token.performance_id, token.generate()

    @staticmethod
    def update_performance_token(p: HeartBridgeUpdatePayload) -> [str, str]:
        # Validate the existing token
        token = PerformanceToken.from_token(p.token, verify_nbf=False)

        # Update any of the fields that were set
        if p.artist:
            token.artist = p.artist
        if p.title:
            token.title = p.title
        if p.performance_date:
            token.performance_date = p.performance_date

        # Return the existing performance id and the new token string
        return token.performance_id, token.generate()


class PerformanceToken:
    class PerformanceTokenException(Exception):
        pass

    class PerformanceTokenDateException(PerformanceTokenException):
        pass

    def __init__(
        self,
        artist: str = "",
        title: str = "",
        performance_date: datetime.datetime = datetime.datetime.fromtimestamp(0),
        performance_id: str = "",
        email: str = "",
        description: str = "",
        duration: int = 90,
    ):
        self.artist: str = artist
        self.title: str = title
        if type(performance_date) is int:
            performance_date = datetime.datetime.fromtimestamp(
                performance_date, tz=datetime.timezone.utc
            )

        self.performance_date: datetime.datetime = performance_date
        self.performance_id: str = performance_id
        self.email: str = email
        self.description: str = description
        self.duration: int = duration

    @classmethod
    def from_register_payload(cls, payload: HeartBridgeRegisterPayload):
        return cls(
            artist=payload.artist,
            title=payload.title,
            performance_date=payload.performance_date,
            email=payload.email,
            description=payload.description,
            duration=payload.duration,
        )

    @classmethod
    def from_json(cls, data: str):
        p = json.loads(data)
        return PerformanceToken.from_dict(p)

    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            artist=data.setdefault("artist", ""),
            title=data.setdefault("title", ""),
            performance_date=data.setdefault(
                "performance_date", datetime.datetime.fromtimestamp(0)
            ),
            performance_id=data.setdefault("performance_id", ""),
            email=data.setdefault("email", ""),
            description=data.setdefault("description", ""),
            duration=data.setdefault("duration", 90),
        )

    @classmethod
    def from_token(
        cls,
        data: str,
        verify: bool = True,
        verify_nbf: bool = True,
        key=HEARTBRIDGE_SECRET,
    ):
        # Attempt to verify/decode the token
        try:
            token_data = jwt.decode(
                data,
                verify=verify,
                options={"verify_nbf": verify_nbf},
                key=key,
                algorithms="HS256",
            )
        except jwt.exceptions.DecodeError as e:
            raise PerformanceToken.PerformanceTokenException(
                f"Token is invalid! {str(e)}"
            ) from e
        except jwt.exceptions.ImmatureSignatureError as e:
            raise PerformanceToken.PerformanceTokenDateException(
                f"Token failed NBF time check! {str(e)}"
            ) from e
        except jwt.exceptions.ExpiredSignatureError as e:
            raise PerformanceToken.PerformanceTokenDateException(
                f"Token has expired! {str(e)}"
            ) from e
        return PerformanceToken.from_dict(token_data)

    def generate(self):
        # Validate that the performance date is legit (validity is from 5 minutes in the past to any time in the future)
        date_now = datetime.datetime.now(datetime.timezone.utc)
        start_date = self.performance_date
        exp_date = start_date + datetime.timedelta(minutes=self.duration + 120)
        nbf_date = start_date - datetime.timedelta(hours=1)
        if (start_date - date_now).total_seconds() < -300:
            raise PerformanceToken.PerformanceTokenDateException(
                f"Date {start_date} is too far in the past"
            )

        # Validate required fields are filled in with valid values
        if not PerformanceId.is_valid(self.performance_id):
            raise PerformanceToken.PerformanceTokenException(
                "Performance Id is invalid!"
            )

        # Assemble the token contents
        token_payload = {
            "artist": self.artist,
            "title": self.title,
            "email": self.email,
            "description": self.description,
            "duration": self.duration,
            "nbf": nbf_date,
            "exp": exp_date,
            "iat": date_now,
            "performance_date": int(start_date.timestamp()),
            "performance_id": self.performance_id,
        }

        # Encode the token
        token = jwt.encode(token_payload, HEARTBRIDGE_SECRET, algorithm="HS256")
        return token
