

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
    WORKER_NODE_ENV_FAILURE = 198
    WORKER_NODE_CLI_FAILURE = 199
    # tested large error codes and they were adjusted by sge, subtracting 128
    # multiple times until they fell within a given range and those were the
    # exit codes that were returned in qacct


class InvalidResponse(Exception):
    pass


class UnsafeSSHDirectory(Exception):
    pass


class RemoteExitInfoNotAvailable(Exception):
    pass


class CallableReturnedInvalidObject(Exception):
    pass


class WorkflowAlreadyExists(Exception):
    pass


class WorkflowAlreadyComplete(Exception):
    pass
