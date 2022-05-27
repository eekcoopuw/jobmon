"""Constants declared for different statuses, types and codes throughout Jobmon."""


class ArgType:

    NODE_ARG = 1
    TASK_ARG = 2
    OP_ARG = 3


class TaskResourcesType:
    """Constant Types for Task Resources."""

    ORIGINAL = "O"
    VALIDATED = "V"
    ADJUSTED = "A"


class TaskInstanceStatus:
    """Statuses used for Task Instances."""

    QUEUED = "Q"
    INSTANTIATED = "I"
    NO_DISTRIBUTOR_ID = "W"
    LAUNCHED = "O"
    RUNNING = "R"
    TRIAGING = "T"
    RESOURCE_ERROR = "Z"
    UNKNOWN_ERROR = "U"
    ERROR = "E"
    DONE = "D"
    KILL_SELF = "K"
    ERROR_FATAL = "F"


class TaskStatus:
    """Statuses used for Tasks."""

    REGISTERING = "G"
    QUEUED = "Q"
    INSTANTIATING = "I"
    LAUNCHED = "O"
    RUNNING = "R"
    DONE = "D"
    ERROR_RECOVERABLE = "E"
    ADJUSTING_RESOURCES = "A"
    ERROR_FATAL = "F"


class WorkflowRunStatus:
    """Statuses used for Workflow Runs."""

    REGISTERED = "G"
    LINKING = "L"
    BOUND = "B"
    ABORTED = "A"
    RUNNING = "R"
    DONE = "D"
    STOPPED = "S"
    ERROR = "E"
    COLD_RESUME = "C"
    HOT_RESUME = "H"
    TERMINATED = "T"
    INSTANTIATED = "I"
    LAUNCHED = "O"


class WorkflowStatus:
    """Statuses used for Workflows."""

    REGISTERING = "G"
    QUEUED = "Q"
    ABORTED = "A"
    INSTANTIATING = "I"
    LAUNCHED = "O"
    RUNNING = "R"
    DONE = "D"
    HALTED = "H"
    FAILED = "F"


class QsubAttribute:
    """SGE exit codes that Jobmon will detect and handle in a special way."""

    ERROR_CODE_SET_KILLED_FOR_INSUFFICIENT_RESOURCES = (137, 247, -9)


class Direction:
    """A generic utility class.

    Used to represent one-dimensional direction,
    such as upstream/downstream.
    """

    UP = "up"
    DOWN = "down"


class SpecialChars:
    """A generic utility class.

    Used to define special chars.
    """

    ILLEGAL_SPECIAL_CHARACTERS = r"/\\'\" "


class ExecludeTTVs:
    """A hard-coded list.

    Used to exclude task template versions with huge tasks that cause DB crash.
    """

    EXECLUDE_TTVS = {1}  # bashtask
