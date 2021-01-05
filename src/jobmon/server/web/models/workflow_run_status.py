from jobmon.constants import WorkflowRunStatus as Statuses
from jobmon.server.web.models import DB


class WorkflowRunStatus(DB.Model):
    __tablename__ = 'workflow_run_status'

    REGISTERED = Statuses.REGISTERED
    BOUND = Statuses.BOUND
    RUNNING = Statuses.RUNNING
    DONE = Statuses.DONE
    ABORTED = Statuses.ABORTED
    STOPPED = Statuses.STOPPED
    ERROR = Statuses.ERROR
    COLD_RESUME = Statuses.COLD_RESUME
    HOT_RESUME = Statuses.HOT_RESUME
    TERMINATED = Statuses.TERMINATED

    id = DB.Column(DB.String(1), primary_key=True)
    label = DB.Column(DB.String(150), nullable=False)
