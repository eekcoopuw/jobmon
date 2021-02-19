"""Custom Exceptions used throughout Jobmon."""


class ReturnCodes(object):
    """Bash return codes used in execution wrapper."""

    OK = 0
    WORKER_NODE_ENV_FAILURE = 198
    WORKER_NODE_CLI_FAILURE = 199
    # tested large error codes and they were adjusted by sge, subtracting 128
    # multiple times until they fell within a given range and those were the
    # exit codes that were returned in qacct


class InvalidResponse(Exception):
    """Invalid Response type Exception."""

    pass


class RemoteExitInfoNotAvailable(Exception):
    """Exception raised when Exit Info is not available for different executor types."""

    pass


class CallableReturnedInvalidObject(Exception):
    """Invalid Object got returned."""

    pass


class WorkflowAlreadyExists(Exception):
    """Workflow with the same workflow args already exists."""

    pass


class WorkflowAlreadyComplete(Exception):
    """This Workflow is already done."""

    pass


class WorkflowNotResumable(Exception):
    """This Workflow is not set to be resumed."""

    pass


class EmptyWorkflowError(Exception):
    """This Workflow is empty."""

    pass


class SchedulerStartupTimeout(Exception):
    """Scheduler was not able to start in time."""

    pass


class SchedulerNotAlive(Exception):
    """The Scheduler is not running."""

    pass


class WorkflowRunStateError(Exception):
    """Error with the Workflow Run status."""

    pass


class ResumeSet(Exception):
    """Resume Exception."""

    pass


class NodeDependencyNotExistError(Exception):
    """Dependency does not exist."""

    pass


class DuplicateNodeArgsError(Exception):
    """Multiple nodes with the same args for the same TaskTemplate not allowed."""

    pass
