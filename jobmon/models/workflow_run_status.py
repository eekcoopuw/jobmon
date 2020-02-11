import logging

from jobmon.models import DB


logger = logging.getLogger(__name__)


class WorkflowRunStatus(DB.Model):
    __tablename__ = 'workflow_run_status'

    REGISTERED = 'G'
    BOUND = 'B'
    RUNNING = 'R'
    DONE = 'D'
    ABORTED = 'A'
    STOPPED = 'S'
    ERROR = 'E'
    COLD_RESUME = 'C'
    HOT_RESUME = 'H'
    TERMINATED = 'T'

    id = DB.Column(DB.String(1), primary_key=True)
    label = DB.Column(DB.String(150), nullable=False)
