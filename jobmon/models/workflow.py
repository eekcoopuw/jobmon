from datetime import datetime

from jobmon.models import DB
from jobmon.models.workflow_status import WorkflowStatus


class Workflow(DB.Model):

    __tablename__ = 'workflow'

    @classmethod
    def from_wire(cls, dct):
        return cls(id=dct['id'],
                   dag_id=dct['dag_id'],
                   workflow_args=dct['workflow_args'],
                   workflow_hash=dct['workflow_hash'],
                   description=dct['description'],
                   name=dct['name'],
                   user=dct['user'],
                   status=dct['status'])

    def to_wire(self):
        return {'id': self.id, 'dag_id': self.dag_id, 'workflow_args':
                self.workflow_args, 'workflow_hash': self.workflow_hash,
                'description': self.description, 'name': self.name, 'user':
                self.user,'status': self.status}

    id = DB.Column(
        DB.Integer, primary_key=True)
    dag_id = DB.Column(
        DB.Integer, DB.ForeignKey('task_dag.dag_id'))
    workflow_args = DB.Column(DB.Text)
    workflow_hash = DB.Column(DB.Text)
    description = DB.Column(DB.Text(collation='utf8_general_ci'))
    name = DB.Column(DB.String(150))
    user = DB.Column(DB.String(150))
    created_date = DB.Column(
        DB.DateTime, default=datetime.utcnow)
    status_date = DB.Column(
        DB.DateTime, default=datetime.utcnow)
    status = DB.Column(DB.String(1),
                       DB.ForeignKey('workflow_status.id'),
                       nullable=False,
                       default=WorkflowStatus.CREATED)

    task_dag = DB.relationship(
        "TaskDagMeta", back_populates="workflow")
