"""Constants declared for different statuses, types and codes throughout Jobmon."""


class ExecutorParameterSetType:
    """Constant Types for Executor Parameter Sets."""

    ORIGINAL = 'O'
    VALIDATED = 'V'
    ADJUSTED = 'A'


class TaskInstanceStatus:
    """Statuses used for Task Instances."""

    INSTANTIATED = 'I'
    NO_EXECUTOR_ID = 'W'
    SUBMITTED_TO_BATCH_EXECUTOR = 'B'
    RUNNING = 'R'
    RESOURCE_ERROR = 'Z'
    UNKNOWN_ERROR = 'U'
    ERROR = 'E'
    DONE = 'D'
    KILL_SELF = 'K'
    ERROR_FATAL = 'F'


class TaskStatus:
    """Statuses used for Tasks."""

    REGISTERED = 'G'
    QUEUED_FOR_INSTANTIATION = 'Q'
    INSTANTIATED = 'I'
    RUNNING = 'R'
    ERROR_RECOVERABLE = 'E'
    ADJUSTING_RESOURCES = 'A'
    ERROR_FATAL = 'F'
    DONE = 'D'


class WorkflowRunStatus:
    """Statuses used for Workflow Runs."""

    REGISTERED = 'G'
    BOUND = 'B'
    RUNNING = 'R'
    DONE = 'D'
    ABORTED = 'A'
    STOPPED = 'S'
    ERROR = 'E'
    COLD_RESUME = 'C'
    HOT_RESUME = 'H'
    TERMINATED = 'T'


class WorkflowStatus:
    """Statuses used for Workflows."""

    REGISTERED = 'G'
    BOUND = 'B'
    ABORTED = 'A'
    CREATED = 'C'
    RUNNING = 'R'
    SUSPENDED = 'S'
    FAILED = 'F'
    DONE = 'D'


class QsubAttribute:
    """SGE exit codes that Jobmon will detect and handle in a special way."""

    NO_EXEC_ID = -99999
    UNPARSABLE = -33333
    ERROR_CODE_SET_KILLED_FOR_INSUFFICIENT_RESOURCES = (137, 247, -9)
