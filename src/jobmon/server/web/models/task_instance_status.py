"""Task Instance Status Table."""
from sqlalchemy import Column, String

from jobmon.constants import TaskInstanceStatus as Statuses
from jobmon.server.web.models import Base


class TaskInstanceStatus(Base):
    """The table in the database that holds on the possible statuses for TaskInstance."""

    __tablename__ = "task_instance_status"
    QUEUED = Statuses.QUEUED
    INSTANTIATED = Statuses.INSTANTIATED
    NO_DISTRIBUTOR_ID = Statuses.NO_DISTRIBUTOR_ID
    LAUNCHED = Statuses.LAUNCHED
    RUNNING = Statuses.RUNNING
    TRIAGING = Statuses.TRIAGING
    RESOURCE_ERROR = Statuses.RESOURCE_ERROR
    UNKNOWN_ERROR = Statuses.UNKNOWN_ERROR
    ERROR = Statuses.ERROR
    DONE = Statuses.DONE
    KILL_SELF = Statuses.KILL_SELF
    ERROR_FATAL = Statuses.ERROR_FATAL

    id = Column(String(1), primary_key=True)
    label = Column(String(150))
