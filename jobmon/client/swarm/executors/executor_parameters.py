import logging
from typing import Tuple, Union, Dict

logger = logging.getLogger(__name__)


class ExecutorParameters(object):
    """Base parameter class for executors, each executor has specific '
    parameters and must validate them accordingly"""

    def __init__(self):
        logger.info("Initializing Base Class ExecutorParameters")

    def return_adjusted(self) -> Union['ExecutorParameters', None]:
        """
        If the parameters need to be adjusted then create and return a new
        object, otherwise None
        """
        raise NotImplementedError

    def is_valid(self) -> Tuple[bool, Dict, Union[str, None]]:
        """
        If the object is valid, return (True, None), otherwise
        (False, error_message), deliberately does not throw so it can be used
        where the client does not want an exception
        """
        return True, {}, None

    def return_validated(self, params) -> 'ExecutorParameters':
        return self

    def is_valid_throw(self) -> Tuple[bool, 'ExecutorParameters']:
        """
        Calls is_valid and converts a False result into an exception
        """
        valid, obj, message = self.is_valid()
        if valid:
            return True, obj
        else:
            raise ValueError(message)
