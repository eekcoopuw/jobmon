from logging import Logger
import sys
import warnings


class ServerSideException(Exception):
    """Used for all exceptions on the server side (JQS, JSM)"""

    def __init__(self, msg: str):
        self.msg = msg

    def __str__(self):
        return repr(self.msg)


class InvalidUsage(ServerSideException):
    # make status_code a parameter for future extension. So far we only use 400
    def __init__(self, msg, status_code=None, payload=None):
        ServerSideException.__init__(self, msg)
        self.status_code = status_code
        if self.status_code is None:
            # by default, use 400
            self.status_code = 400
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.msg
        return rv


class ServerError(ServerSideException):
    # make status_code a parameter for future extension. So far we only use 500
    def __init__(self, msg, status_code=None, payload=None):
        ServerSideException.__init__(self, msg)
        self.status_code = status_code
        if self.status_code is None:
            # by default, use 500
            self.status_code = 500
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.msg
        return rv


def log_and_raise(msg: str, logger: Logger):
    """
    Something bad happened, send the message to warnings, logger and
    re-raise as a wrapped exception. A useful place to add global error
    handling without boiler-plate code
    """
    warnings.warn(msg)
    logger.error(msg)
    # Use value and traceback so that the original exception is not lost
    (_, value, traceback) = sys.exc_info()
    raise ServerSideException(f"{msg} from {value}").with_traceback(traceback)
