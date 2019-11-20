from datetime import datetime
from sqlalchemy.sql import func

from jobmon.models import DB
from jobmon.models.workflow_status import WorkflowStatus


class Workflow(DB.Model):

    __tablename__ = 'workflow'

    @classmethod
    def from_wire(cls, dct):
        pass

    def to_wire(self):
        pass

    id = DB.Column(DB.Integer, primary_key=True)
    tool_version_id = DB.Column(DB.Integer, DB.ForeignKey("tool_version.id"))
    dag_id = DB.Column(DB.Integer, DB.ForeignKey('dag.id'))
    workflow_args_hash = DB.Column(DB.Integer)
    task_hash = DB.Column(DB.Integer)
    description = DB.Column(DB.Text(collation='utf8_general_ci'))
    name = DB.Column(DB.String(150))
    status = DB.Column(DB.String(1),
                       DB.ForeignKey('workflow_status.id'),
                       default=WorkflowStatus.CREATED)
    created_date = DB.Column(DB.DateTime, default=func.UTC_TIMESTAMP())
    status_date = DB.Column(DB.DateTime, default=func.UTC_TIMESTAMP())
    heartbeat_date = DB.Column(DB.DateTime, default=datetime.utcnow)

    dag = DB.relationship(
        "Dag", back_populates="workflow")
    # TODO: FSM transitions here
