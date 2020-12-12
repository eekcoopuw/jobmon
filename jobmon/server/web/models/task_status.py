from jobmon.constants import TaskStatus as Statuses
from jobmon.server.web.models import DB


class TaskStatus(DB.Model):
    """The table in the database that holds on the possible statuses for Job"""

    __tablename__ = 'task_status'

    REGISTERED = Statuses.REGISTERED
    QUEUED_FOR_INSTANTIATION = Statuses.QUEUED_FOR_INSTANTIATION
    INSTANTIATED = Statuses.INSTANTIATED
    RUNNING = Statuses.RUNNING
    ERROR_RECOVERABLE = Statuses.ERROR_RECOVERABLE
    ADJUSTING_RESOURCES = Statuses.ADJUSTING_RESOURCES
    ERROR_FATAL = Statuses.ERROR_FATAL
    DONE = Statuses.DONE

    id = DB.Column(DB.String(1), primary_key=True)
    label = DB.Column(DB.String(150))
