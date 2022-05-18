"""Workflow status database table."""
from sqlalchemy import Column, String

from jobmon.constants import WorkflowStatus as Statuses
from jobmon.server.web.models import Base


class WorkflowStatus(Base):
    """Workflow Status database table."""

    __tablename__ = "workflow_status"

    REGISTERING = Statuses.REGISTERING
    QUEUED = Statuses.QUEUED
    ABORTED = Statuses.ABORTED
    RUNNING = Statuses.RUNNING
    HALTED = Statuses.HALTED
    FAILED = Statuses.FAILED
    DONE = Statuses.DONE
    INSTANTIATING = Statuses.INSTANTIATING
    LAUNCHED = Statuses.LAUNCHED

    id = Column(String(1), primary_key=True)
    label = Column(String(150), nullable=False)
