import jwt
import json
import datetime

# Secret key for signing performer tokens
# FIXME: GET FROM ENV VARIABLE
HEARTBRIDGE_SECRET = "hbsecretkey"

# Number of seconds in a day
SECS_IN_HOUR = 60 * 60
SECS_IN_DAY = 24 * SECS_IN_HOUR


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
