from sqlalchemy.sql import func

from jobmon.models import DB
from jobmon.models.workflow_run_status import WorkflowRunStatus


class WorkflowRun(DB.Model):

    __tablename__ = 'workflow_run'

    id = DB.Column(DB.Integer, primary_key=True)
    workflow_id = DB.Column(DB.Integer, DB.ForeignKey('workflow.id'))
    user = DB.Column(DB.String(150))
    executor_class = DB.Column(DB.String(150))
    status = DB.Column(DB.String(1),
                       DB.ForeignKey('workflow_run_status.id'),
                       default=WorkflowRunStatus.RUNNING)
    created_date = DB.Column(DB.DateTime, default=func.UTC_TIMESTAMP())
    status_date = DB.Column(DB.DateTime, default=func.UTC_TIMESTAMP())
    heartbeat_date = DB.Column(DB.DateTime, default=func.UTC_TIMESTAMP())
