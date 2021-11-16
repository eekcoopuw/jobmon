"""Task Instance Status Table."""
from jobmon.constants import TaskInstanceStatus as Statuses
from jobmon.server.web.models import DB


class TaskInstanceStatus(DB.Model):
    """The table in the database that holds on the possible statuses for TaskInstance."""

    __tablename__ = "task_instance_status"
    INSTANTIATED = Statuses.INSTANTIATED
    NO_DISTRIBUTOR_ID = Statuses.NO_DISTRIBUTOR_ID
    SUBMITTED_TO_BATCH_DISTRIBUTOR = Statuses.SUBMITTED_TO_BATCH_DISTRIBUTOR
    LAUNCHED = Statuses.LAUNCHED
    RUNNING = Statuses.RUNNING
    RESOURCE_ERROR = Statuses.RESOURCE_ERROR
    UNKNOWN_ERROR = Statuses.UNKNOWN_ERROR
    ERROR = Statuses.ERROR
    DONE = Statuses.DONE
    KILL_SELF = Statuses.KILL_SELF
    ERROR_FATAL = Statuses.ERROR_FATAL

    id = DB.Column(DB.String(1), primary_key=True)
    label = DB.Column(DB.String(150))
