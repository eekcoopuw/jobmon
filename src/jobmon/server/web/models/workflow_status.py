"""Workflow status database table."""
from jobmon.constants import WorkflowStatus as Statuses
from jobmon.server.web.models import DB


class WorkflowStatus(DB.Model):
    """Workflow Status database table."""

    __tablename__ = 'workflow_status'

    REGISTERING = Statuses.REGISTERING
    QUEUED = Statuses.QUEUED
    ABORTED = Statuses.ABORTED
    RUNNING = Statuses.RUNNING
    HALTED = Statuses.HALTED
    FAILED = Statuses.FAILED
    DONE = Statuses.DONE

    id = DB.Column(DB.String(1), primary_key=True)
    label = DB.Column(DB.String(150), nullable=False)
