"""Task Instance Status Table."""
from sqlalchemy import Column, String
from sqlalchemy.orm import Session

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


def add_task_instance_statuses(session: Session):
    statuses = [
        TaskInstanceStatus(id='Q', label='QUEUED'),
        TaskInstanceStatus(id='I', label='INSTANTIATED'),
        TaskInstanceStatus(id='W', label='NO_DISTRIBUTOR_ID'),
        TaskInstanceStatus(id='O', label='LAUNCHED'),
        TaskInstanceStatus(id='R', label='RUNNING'),
        TaskInstanceStatus(id='T', label='TRIAGING'),
        TaskInstanceStatus(id='Z', label='RESOURCE_ERROR'),
        TaskInstanceStatus(id='U', label='UNKNOWN_ERROR'),
        TaskInstanceStatus(id='E', label='ERROR'),
        TaskInstanceStatus(id='D', label='DONE'),
        TaskInstanceStatus(id='K', label='KILL_SELF'),
        TaskInstanceStatus(id='F', label='ERROR_FATAL'),
    ]
    session.add_all(statuses)
