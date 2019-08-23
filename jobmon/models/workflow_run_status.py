import logging

from jobmon.models import DB


logger = logging.getLogger(__name__)


class WorkflowRunStatus(DB.Model):
    __tablename__ = 'workflow_run_status'

    RUNNING = 'R'
    STOPPED = 'S'
    ERROR = 'E'
    DONE = 'D'

    id = DB.Column(DB.String(1), primary_key=True)
    label = DB.Column(DB.String(150), nullable=False)
