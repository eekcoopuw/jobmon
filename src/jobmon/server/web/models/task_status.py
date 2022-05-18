"""Task Instance Status Database table."""
from sqlalchemy import Column, String

from jobmon.constants import TaskStatus as Statuses
from jobmon.server.web.models import Base


class TaskStatus(Base):
    """The table in the database that holds on the possible statuses for a Task."""

    __tablename__ = "task_status"

    REGISTERING = Statuses.REGISTERING
    QUEUED = Statuses.QUEUED
    INSTANTIATING = Statuses.INSTANTIATING
    LAUNCHED = Statuses.LAUNCHED
    RUNNING = Statuses.RUNNING
    ERROR_RECOVERABLE = Statuses.ERROR_RECOVERABLE
    ADJUSTING_RESOURCES = Statuses.ADJUSTING_RESOURCES
    ERROR_FATAL = Statuses.ERROR_FATAL
    DONE = Statuses.DONE

    id = Column(String(1), primary_key=True)
    label = Column(String(150))
