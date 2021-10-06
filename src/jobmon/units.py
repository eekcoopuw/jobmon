from datetime import timedelta


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
