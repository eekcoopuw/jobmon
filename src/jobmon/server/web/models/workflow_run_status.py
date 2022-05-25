"""Workflow Run Status Database Table."""
from sqlalchemy import Column, String
from sqlalchemy.orm import Session

from jobmon.constants import WorkflowRunStatus as Statuses
from jobmon.server.web.models import Base


class WorkflowRunStatus(Base):
    """Workflow Run Status Database Table."""

    __tablename__ = "workflow_run_status"

    REGISTERED = Statuses.REGISTERED
    LINKING = Statuses.LINKING
    BOUND = Statuses.BOUND
    ABORTED = Statuses.ABORTED
    RUNNING = Statuses.RUNNING
    DONE = Statuses.DONE
    STOPPED = Statuses.STOPPED
    ERROR = Statuses.ERROR
    COLD_RESUME = Statuses.COLD_RESUME
    HOT_RESUME = Statuses.HOT_RESUME
    TERMINATED = Statuses.TERMINATED
    INSTANTIATED = Statuses.INSTANTIATED
    LAUNCHED = Statuses.LAUNCHED

    id = Column(String(1), primary_key=True)
    label = Column(String(150), nullable=False)


def add_workflow_run_statuses(session: Session):
    statuses = [
        WorkflowRunStatus(id='G', label='REGISTERED'),
        WorkflowRunStatus(id='L', label='LINKING'),
        WorkflowRunStatus(id='B', label='BOUND'),
        WorkflowRunStatus(id='A', label='ABORTED'),
        WorkflowRunStatus(id='R', label='RUNNING'),
        WorkflowRunStatus(id='D', label='DONE'),
        WorkflowRunStatus(id='S', label='STOPPED'),
        WorkflowRunStatus(id='E', label='ERROR'),
        WorkflowRunStatus(id='C', label='COLD_RESUME'),
        WorkflowRunStatus(id='H', label='HOT_RESUME'),
        WorkflowRunStatus(id='T', label='TERMINATED'),
        WorkflowRunStatus(id='I', label='INSTANTIATED'),
        WorkflowRunStatus(id='O', label='LAUNCHED'),
    ]
    session.add_all(statuses)
