import getpass
import os
import socket
from datetime import datetime

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String

from sqlalchemy.orm import relationship

from jobmon.config import config
from jobmon.requester import Requester
from jobmon.sql_base import Base


class WorkflowRunStatus(Base):
    __tablename__ = 'workflow_run_status'

    RUNNING = 1
    STOPPED = 2
    COMPLETE = 3

    id = Column(Integer, primary_key=True)
    label = Column(String(150), nullable=False)


class WorkflowRunDAO(Base):

    __tablename__ = 'workflow_run'

    @classmethod
    def from_wire(cls, dct):
        return cls(id=dct['dag_id'], job_id=dct['job_id'],
                   name=dct['name'], command=dct['command'],
                   slots=dct['slots'], mem_free=dct['mem_free'],
                   project=dct['project'], status=dct['status'],
                   num_attempts=dct['num_attempts'],
                   max_attempts=dct['max_attempts'],
                   context_args=dct['context_args'])

    def to_wire(self):
        return {'dag_id': self.dag_id, 'job_id': self.job_id, 'name':
                self.name, 'command': self.command, 'status': self.status,
                'slots': self.slots, 'mem_free': self.mem_free,
                'project': self.project, 'num_attempts': self.num_attempts,
                'max_attempts': self.max_attempts,
                'context_args': self.context_args}

    id = Column(Integer, primary_key=True)
    workflow_id = Column(Integer, ForeignKey('workflow.id'))
    user = Column(String(150))
    hostname = Column(String(150))
    pid = Column(Integer)
    created_date = Column(DateTime, default=datetime.utcnow())
    status_date = Column(DateTime, default=datetime.utcnow())
    status = Column(Integer,
                    ForeignKey('workflow_status.id'),
                    nullable=False,
                    default=WorkflowRunStatus.RUNNING)

    workflow = relationship("WorkflowDAO", back_populates="workflow_runs")


class WorkflowRun(object):

    def __init__(self, workflow_id):
        self.workflow_id = workflow_id
        self.jsm_req = Requester(config.jm_rep_conn)
        rc, wfr_id = self.jsm_req.send_request({
            'action': 'add_workflow_run',
            'kwargs': {'workflow_id': workflow_id,
                       'user': getpass.getuser(),
                       'hostname': socket.gethostname(),
                       'pid': os.getpid()}
        })
        self.id = wfr_id

    def update_stopped(self):
        self._update_status(WorkflowRunStatus.STOPPED)

    def update_complete(self):
        self._update_status(WorkflowRunStatus.COMPLETE)

    def _update_status(self, status):
        rc, wfr_id = self.jsm_req.send_request({
            'action': 'update_workflow_run',
            'kwargs': {'wfr_id': self.id, 'status': status}
        })
