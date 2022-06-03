"""Workflow status database table."""
from sqlalchemy import Column, String
from sqlalchemy.orm import Session

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


def add_workflow_statuses(session: Session):
    statuses = [
        WorkflowStatus(id='G', label='REGISTERING'),
        WorkflowStatus(id='Q', label='QUEUED'),
        WorkflowStatus(id='I', label='INSTANTIATING'),
        WorkflowStatus(id='O', label='LAUNCHED'),
        WorkflowStatus(id='A', label='ABORTED'),
        WorkflowStatus(id='R', label='RUNNING'),
        WorkflowStatus(id='D', label='DONE'),
        WorkflowStatus(id='F', label='FAILED'),
        WorkflowStatus(id='H', label='HALTED'),
    ]
    session.add_all(statuses)
