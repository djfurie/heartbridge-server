import random

import jwt
import json
import datetime
import os
from .payload_types import HeartBridgeRegisterPayload
from typing import Union

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


class PerformanceToken:
    class PerformanceTokenException(Exception):
        pass

    class PerformanceTokenDateException(PerformanceTokenException):
        pass

    def __init__(self, artist: str = "", title: str = "",
                 performance_date: datetime.datetime = datetime.datetime.fromtimestamp(0),
                 performance_id: str = ""):
        self.artist: str = artist
        self.title: str = title
        self.performance_date: datetime.datetime = performance_date
        self.performance_id: str = performance_id

    @classmethod
    def from_register_payload(cls, payload: HeartBridgeRegisterPayload):
        return cls(artist=payload.artist,
                   title=payload.title,
                   performance_date=payload.performance_date)

    @classmethod
    def from_json(cls, data: str):
        p = json.loads(data)
        return PerformanceToken.from_dict(p)

    @classmethod
    def from_dict(cls, data: dict):
        return cls(artist=data.setdefault('artist', ""),
                   title=data.setdefault('title', ""),
                   performance_date=data.setdefault('performance_date', datetime.datetime.fromtimestamp(0)),
                   performance_id=data.setdefault('performance_id', ""))

    @classmethod
    def from_token(cls, data: str, verify: bool = True, verify_nbf: bool = True, key=HEARTBRIDGE_SECRET):
        # Attempt to verify/decode the token
        token_data = jwt.decode(data, verify=verify, options={'verify_nbf': verify_nbf}, key=key)
        return PerformanceToken.from_dict(token_data)

    def generate(self):
        # Validate that the performance date is legit (validity is from 5 minutes in the past to any time in the future)
        date_now = datetime.datetime.now(datetime.timezone.utc)
        start_date = self.performance_date
        exp_date = start_date + datetime.timedelta(days=1)
        nbf_date = start_date - datetime.timedelta(hours=1)
        if (start_date - date_now).total_seconds() < -300:
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
                         'performance_date': int(start_date.timestamp()),
                         'performance_id': self.performance_id}

        # Encode the token
        token = jwt.encode(token_payload,
                           HEARTBRIDGE_SECRET,
                           algorithm='HS256')
        return token


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
