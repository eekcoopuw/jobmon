from datetime import datetime

from jobmon.models import DB
from jobmon.models.workflow_run_status import WorkflowRunStatus


class WorkflowRun(DB.Model):

    __tablename__ = 'workflow_run'

    id = DB.Column(DB.Integer, primary_key=True)
    workflow_id = DB.Column(DB.Integer, DB.ForeignKey('workflow.id'))
    user = DB.Column(DB.String(150))
    hostname = DB.Column(DB.String(150))
    pid = DB.Column(DB.Integer)
    stderr = DB.Column(DB.String(1000))
    stdout = DB.Column(DB.String(1000))
    project = DB.Column(DB.String(150))
    working_dir = DB.Column(DB.String(1000), default=None)
    slack_channel = DB.Column(DB.String(150))
    executor_class = DB.Column(DB.String(150))
    resource_adjustment = DB.Column(DB.Float, default=0.5)
    created_date = DB.Column(DB.DateTime, default=datetime.utcnow)
    status_date = DB.Column(DB.DateTime, default=datetime.utcnow)
    status = DB.Column(DB.String(1),
                       DB.ForeignKey('workflow_run_status.id'),
                       nullable=False,
                       default=WorkflowRunStatus.RUNNING)

    workflow = DB.relationship("Workflow", backref="workflow_runs", lazy=True)

    @classmethod
    def from_wire(cls, dct):
        return cls(
            id=dct['id'],
            workflow_id=dct['workflow_id'],
            user=dct['user'],
            hostname=dct['hostname'],
            pid=dct['pid'],
            stderr=dct['stderr'],
            stdout=dct['stdout'],
            project=dct['project'],
            working_dir=dct['working_dir'],
            slack_channel=dct['slack_channel'],
            executor_class=dct['executor_class'],
            resource_adjustment=dct['resource_adjustment'],
            status=dct['status'],
        )

    def to_wire(self):
        return {
            'id': self.id,
            'workflow_id': self.workflow_id,
            'user': self.user,
            'hostname': self.hostname,
            'pid': self.pid,
            'stderr': self.stderr,
            'stdout': self.stdout,
            'project': self.project,
            'working_dir': self.working_dir,
            'slack_channel': self.slack_channel,
            'executor_class': self.executor_class,
            'resource_adjustment': self.resource_adjustment,
            'status': self.status,
        }
