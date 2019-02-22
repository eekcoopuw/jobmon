from logging import Handler


class ReturnCodes(object):
    """Responder response codes.
    OK means no error, all the rest are errors.
    Please add more as the need arises."""
    OK = 0
    INVALID_RESPONSE_FORMAT = 1
    INVALID_REQUEST_FORMAT = 2
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


class NoDatabase(Exception):
    pass


class SGENotAvailable(Exception):
    pass


class UnsafeSSHDirectory(Exception):
    pass
