import logging
from typing import Optional, Any
import sys

DEFAULT_FORMAT = "%(asctime)s [%(name)-12s] %(module)s %(levelname)-8s: %(message)s"
class ClientLogging:
    """
    This is a class to provide jobmon user an easy way to enable logging.
    Please don't use it in our code, so that we allow users to apply their own logger.
    """
    def __init__(self,
                 log_format: Optional[str] = None,
                 log_level: Optional[int] = logging.INFO):
        self._format = log_format if log_format else DEFAULT_FORMAT
        self._level = log_level

    def attach(self, logger_name: Optional[str] = None, handler: Optional[Any] = None):
        """
        A method to attach a log handler to a given log lever
        Args:
            logger_name: The logger to attach the handler to. Use root logger if none.
            handler: The handler to attach to. Use StdOut if none.

        Returns: None

        """
        if logger_name:
            logger = logging.getLogger(logger_name)
        else:
            logger = logging.getLogger()
        logger.setLevel(self._level)
        logging.basicConfig(format=self._format)
        if handler:
            logger.addHandler(handler)
        else:
            logger.addHandler(logging.StreamHandler(sys.stdout))
