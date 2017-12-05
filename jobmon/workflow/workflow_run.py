import getpass
import os
import socket
from datetime import datetime

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String

from sqlalchemy.orm import relationship

from jobmon.config import config
from jobmon.exceptions import ReturnCodes
from jobmon.requester import Requester
from jobmon.sql_base import Base


class WorkflowRunStatus(Base):
    __tablename__ = 'workflow_run_status'

    RUNNING = 1
    STOPPED = 2
    ERROR = 3
    DONE = 4

    id = Column(Integer, primary_key=True)
    label = Column(String(150), nullable=False)


class WorkflowRunDAO(Base):

    __tablename__ = 'workflow_run'

    id = Column(Integer, primary_key=True)
    workflow_id = Column(Integer, ForeignKey('workflow.id'))
    user = Column(String(150))
    hostname = Column(String(150))
    pid = Column(Integer)
    created_date = Column(DateTime, default=datetime.utcnow())
    status_date = Column(DateTime, default=datetime.utcnow())
    status = Column(Integer,
                    ForeignKey('workflow_run_status.id'),
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
        if rc != ReturnCodes.OK:
            raise ValueError("Invalid Reponse")
        self.id = wfr_id

    def update_done(self):
        self._update_status(WorkflowRunStatus.DONE)

    def update_error(self):
        self._update_status(WorkflowRunStatus.ERROR)

    def update_stopped(self):
        self._update_status(WorkflowRunStatus.STOPPED)

    def _update_status(self, status):
        rc, wfr_id = self.jsm_req.send_request({
            'action': 'update_workflow_run',
            'kwargs': {'wfr_id': self.id, 'status': status}
        })
