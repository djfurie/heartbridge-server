import random


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
                random.randint(
                    0, len(PerformanceId.VALID_PERFORMANCE_ID_CHARACTERS) - 1
                )
            ]
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
