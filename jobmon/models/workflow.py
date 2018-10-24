from datetime import datetime

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import relationship

from jobmon.models.sql_base import Base
from jobmon.models.workflow_status import WorkflowStatus
from jobmon.models.workflow_run import WorkflowRun
from jobmon.models.task_dag import TaskDagMeta


class Workflow(Base):

    __tablename__ = 'workflow'

    @classmethod
    def from_wire(cls, dct):
        return cls(id=dct['id'], dag_id=dct['dag_id'],
                   workflow_args=dct[
                       'workflow_args'],
                   workflow_hash=dct[
                       'workflow_hash'],
                   description=dct[
                       'description'], name=dct['name'],
                   user=dct['user'], status=dct['status'])

    def to_wire(self):
        return {'id': self.id, 'dag_id': self.dag_id, 'workflow_args':
                self.workflow_args, 'workflow_hash': self.workflow_hash,
                'description': self.description, 'name': self.name, 'user':
                self.user, 'status': self.status}

    id = Column(
        Integer, primary_key=True)
    dag_id = Column(
        Integer, ForeignKey('task_dag.dag_id'))
    workflow_args = Column(Text)
    workflow_hash = Column(Text)
    description = Column(Text)
    name = Column(String(150))
    user = Column(String(150))
    created_date = Column(
        DateTime, default=datetime.utcnow)
    status_date = Column(
        DateTime, default=datetime.utcnow)
    status = Column(String(1),
                    ForeignKey(
                        'workflow_status.id'),
                    nullable=False,
                    default=WorkflowStatus.CREATED)

    workflow_runs = relationship(
        "WorkflowRun", back_populates="workflow")
    task_dag = relationship(
        "TaskDagMeta", back_populates="workflow")
