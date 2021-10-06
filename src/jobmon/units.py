from datetime import timedelta
from typing import Tuple


class TimeUnit:
    """A helper class provides static functions to switch among seconds, minutes, and hours.

    It also can be initialized with combined input of seconds, minutes, and hours,
    and converts them to any single time unit.
    """

    @staticmethod
    def min_to_sec(minutes: float) -> int:
        """Static helper function to turn minutes into seconds. Accept pointers."""
        return int(minutes * 60)

    @staticmethod
    def hour_to_sec(hour: float) -> int:
        """Static helper function to turn minutes into seconds."""
        return int(hour * 60 * 60)

    @staticmethod
    def hour_to_min(hour: float) -> float:
        """Static helper function to turn minutes into seconds."""
        return round(hour * 60.0, 2)

    @staticmethod
    def min_to_hour(min: float) -> float:
        """Static helper function to turn minutes into seconds."""
        return round(min / 60.0, 2)

    @staticmethod
    def sec_to_hour(sec: int) -> float:
        """Static helper function to turn minutes into seconds."""
        return round(sec / 3600.0, 2)

    @staticmethod
    def sec_to_min(sec: int) -> float:
        """Static helper function to turn minutes into seconds."""
        return round(sec / 60.0, 2)

    def __init__(self, sec: int = 0, min: float = 0.0, hour: float = 0.0) -> None:
        """The constructor."""
        self.seconds = sec + TimeUnit.min_to_sec(min) + TimeUnit.hour_to_sec(hour)
        self.minutes = TimeUnit.sec_to_min(sec) + min + TimeUnit.hour_to_min(hour)
        self.hours = TimeUnit.sec_to_hour(sec) + TimeUnit.min_to_hour(min) + hour
        self.readable = str(timedelta(seconds=self.seconds))


class MemUnit:
    """A helper class to convert memory units between B, k, K, m, M, g, G, t, T."""

    base_chart_to_B = {"B": 1, "k": 1000, "K": 1024, "m": 1e+6, "M": 1.049e+6, "g": 1e+9,
                       "G": 1.074e+9, "t": 1e+12, "T": 1.1e+12}

    @staticmethod
    def _split_unit(input: str) -> Tuple[int, str]:
        """Split the last char as the unit. Default is M."""
        if input[-1] in MemUnit.base_chart_to_B.keys():
            return int(input[: -1]), input[-1]
        else:
            return int(input), "M"

    @staticmethod
    def _to_B(input: str) -> int:
        value, unit = MemUnit._split_unit(input)
        return int(value * MemUnit.base_chart_to_B[unit])

    @staticmethod
    def convert(input: str, to: str = "M") -> int:
        """Convert memory to different units.

        input: a string in format "1G", "1g", etc
        to: the unit to convert to; takes B, k, K, m, M, g, G, t, T.
        Return: an int of the memory value of your specified unit
        """
        value = MemUnit._to_B(input)
        if to not in MemUnit.base_chart_to_B.keys():
            to = "M"
        return round(value / MemUnit.base_chart_to_B[to])
