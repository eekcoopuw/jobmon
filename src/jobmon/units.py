from datetime import timedelta
import re
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

    base_chart_to_B = {"B": 1, "K": 1024, "M": 1.049e+6, "G": 1.074e+9, "T": 1.1e+12}

    @staticmethod
    def _split_unit(input: str) -> Tuple[int, str]:
        """Split the number and the unit. Return 0 M for invalid; use M when no unit."""
        values = re.findall(r"^\d+", input)
        if len(values) == 0:
            return 0, "M"
        value = values[0]
        units = re.findall(r"\D+$", input)
        if len(units) == 0:
            unit = "M"
        else:
            unit = units[0]
            unit = re.sub("[B|b].*", "B", unit)
            unit = re.sub("[K|k].*", "K", unit)
            unit = re.sub("[M|m].*", "M", unit)
            unit = re.sub("[G|g].*", "G", unit)
            unit = re.sub("[T|t].*", "T", unit)
            if unit not in MemUnit.base_chart_to_B.keys():
                unit = "M"
        return int(value), unit

    @staticmethod
    def _to_B(input: str) -> int:
        value, unit = MemUnit._split_unit(input)
        return int(value * MemUnit.base_chart_to_B[unit])

    @staticmethod
    def convert(input: str, to: str = "M") -> int:
        """Convert memory to different units.

        input: a string in format "1G", "1g", "1Gib", "1gb",etc.
               Any input is valid. For example, "abc" -> "0M"; "1kitty" -> "1K";
               "100 gb"-> "100G"; "1 kitty" -> 1K; "100G00M" -> "100M".
        to: the unit to convert to; takes B, K, M, G, T.
        Lower case unit will be treated as capital.
        Return: an int of the memory value of your specified unit
        """
        input = re.sub(r"\s", "", input)
        value = MemUnit._to_B(input)
        to = to.upper()
        if to not in MemUnit.base_chart_to_B.keys():
            to = "M"
        return round(value / MemUnit.base_chart_to_B[to])
