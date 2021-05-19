"""Constants declared for different statuses, types and codes throughout Jobmon."""


class TaskResourcesType:
    """Constant Types for Task Resources."""

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
    LINKING = 'L'
    BOUND = 'B'
    ABORTED = 'A'
    RUNNING = 'R'
    DONE = 'D'
    STOPPED = 'S'
    ERROR = 'E'
    COLD_RESUME = 'C'
    HOT_RESUME = 'H'
    TERMINATED = 'T'
    INSTANTIATING = 'I'
    LAUNCHED = 'O'


class WorkflowStatus:
    """Statuses used for Workflows."""

    REGISTERING = 'G'
    QUEUED = 'Q'
    ABORTED = 'A'
    RUNNING = 'R'
    HALTED = 'H'
    FAILED = 'F'
    DONE = 'D'
    INSTANTIATING = 'I'
    LAUNCHED = 'O'


class QsubAttribute:
    """SGE exit codes that Jobmon will detect and handle in a special way."""

    NO_EXEC_ID = -99999
    UNPARSABLE = -33333
    ERROR_CODE_SET_KILLED_FOR_INSUFFICIENT_RESOURCES = (137, 247, -9)
