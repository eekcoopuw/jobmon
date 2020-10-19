from logging import Logger
import sys


# Use as base class for server side error
class ServerSideException(Exception):
    """Used for all exceptions on the server side (JQS, JSM)"""

    def __init__(self, msg: str):
        self.msg = msg

    def __str__(self):
        return repr(self.msg)


# Use for error caused by client mistakes
class InvalidUsage(ServerSideException):
    # make status_code a parameter for future extension. So far we only use 400
    def __init__(self, msg, status_code=None, payload=None):
        super().__init__(msg)
        self.status_code = status_code
        if self.status_code is None:
            # by default, use 400
            self.status_code = 400
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.msg
        return rv


# Use for Internal Server Error
class ServerError(ServerSideException):
    # make status_code a parameter for future extension. So far we only use 500
    def __init__(self, msg, status_code=None, payload=None):
        super().__init__(msg)
        self.status_code = status_code
        if self.status_code is None:
            # by default, use 500
            self.status_code = 500
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.msg
        return rv


# Use to log and raise server errors
def log_and_raise(msg: str, logger: Logger):
    """
    Something bad happened, send the message to warnings, logger and
    re-raise as a wrapped exception. A useful place to add global error
    handling without boiler-plate code
    """
    logger.error(msg)
    # Use value and traceback so that the original exception is not lost
    (_, value, traceback) = sys.exc_info()
    print(msg)
    print(value)
    raise ServerError(f"{msg} from {value}").with_traceback(traceback)


# Use to raise user mistakes
def raise_user_error(msg: str, logger: Logger):
    """
    Something bad happened, send the message to warnings, logger and
    re-raise as a wrapped exception. A useful place to add global error
    handling without boiler-plate code
    """
    logger.info(msg)
    # Use value and traceback so that the original exception is not lost
    (_, value, traceback) = sys.exc_info()
    raise InvalidUsage(f"{msg} from {value}").with_traceback(traceback)
