from datetime import datetime
import logging

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from jobmon.models.sql_base import Base
from jobmon.models.workflow_run_status import WorkflowRunStatus


logger = logging.getLogger(__name__)


class WorkflowRun(Base):

    __tablename__ = 'workflow_run'

    id = Column(Integer, primary_key=True)
    workflow_id = Column(Integer, ForeignKey('workflow.id'))
    user = Column(String(150))
    hostname = Column(String(150))
    pid = Column(Integer)
    stderr = Column(String(1000))
    stdout = Column(String(1000))
    project = Column(String(150))
    working_dir = Column(String(1000), default=None)
    slack_channel = Column(String(150))
    executor_class = Column(String(150))
    created_date = Column(DateTime, default=datetime.utcnow)
    status_date = Column(DateTime, default=datetime.utcnow)
    status = Column(String(1),
                    ForeignKey('workflow_run_status.id'),
                    nullable=False,
                    default=WorkflowRunStatus.RUNNING)

    workflow = relationship("Workflow", back_populates="workflow_runs")

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
            'status': self.status,
        }
