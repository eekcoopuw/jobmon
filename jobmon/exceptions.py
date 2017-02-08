from logging import Handler


class ReturnCodes(object):
    """Responder response codes.
    OK means no error, all the rest are errors.
    Please add more as the need arises."""
    OK = 0
    INVALID_RESPONSE_FORMAT = 1
    INVALID_ACTION = 2
    GENERIC_ERROR = 3
    NO_RESULTS = 4


class ServerRunning(Exception):
    pass


class ServerStartLocked(Exception):
    pass


class CannotConnectToCentralJobMonitor(Exception):
    pass


class CentralJobMonitorNotAlive(Exception):
    pass


class NoResponseRecieved(Exception):
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
