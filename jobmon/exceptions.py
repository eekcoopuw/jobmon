from logging import Handler


class ReturnCodes(object):
    """Responder response codes.
    OK means no error, all the rest are errors.
    Please add more as the need arises."""
    OK = 0
    INVALID_RESPONSE_FORMAT = 1
    INVALID_REQUEST = 2
    INVALID_ACTION = 3
    GENERIC_ERROR = 4
    NO_RESULTS = 5
    UNKNOWN_EXIT_STATE = 99


class ServerRunning(Exception):
    pass


class ServerStartLocked(Exception):
    pass


class CannotConnectToCentralJobMonitor(Exception):
    pass


class CentralJobMonitorNotAlive(Exception):
    pass


class NoResponseReceived(Exception):
    pass


class NoSocket(Exception):
    pass


class InvalidRequest(Exception):
    pass


class InvalidResponse(Exception):
    pass


class InvalidAction(Exception):
    pass


def log_exceptions(job):
    def wrapper(func):
        def catch_and_send(*args, **kwargs):
            try:
                func(*args, **kwargs)
            except Exception as e:
                job.log_error(str(e))
                raise
        return catch_and_send
    return wrapper


class ZmqHandler(Handler):

    def __init__(self, job):
        super().__init__()
        self.job = job

    def emit(self, record):
        self.job.log_error(record.message)
