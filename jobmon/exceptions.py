

class ReturnCodes(object):
    """Responder response codes. Please add more as the need arises."""
    OK = 0
    WORKER_NODE_ENV_FAILURE = 198
    WORKER_NODE_CLI_FAILURE = 199
    OK = 200
    # tested large error codes and they were adjusted by sge, subtracting 128
    # multiple times until they fell within a given range and those were the
    # exit codes that were returned in qacct


class InvalidResponse(Exception):
    pass


class RemoteExitInfoNotAvailable(Exception):
    pass


class CallableReturnedInvalidObject(Exception):
    pass


class WorkflowAlreadyExists(Exception):
    pass


class WorkflowAlreadyComplete(Exception):
    pass


class WorkflowNotResumable(Exception):
    pass


class EmptyWorkflowError(Exception):
    pass


class SchedulerStartupTimeout(Exception):
    pass


class SchedulerNotAlive(Exception):
    pass


class WorkflowRunStateError(Exception):
    pass


class ResumeSet(Exception):
    pass
