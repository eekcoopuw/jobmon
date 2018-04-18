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
    stderr = Column(String(1000))
    stdout = Column(String(1000))
    project = Column(String(150))
    slack_channel = Column(String(150))
    created_date = Column(DateTime, default=datetime.utcnow)
    status_date = Column(DateTime, default=datetime.utcnow)
    status = Column(Integer,
                    ForeignKey('workflow_run_status.id'),
                    nullable=False,
                    default=WorkflowRunStatus.RUNNING)

    workflow = relationship("WorkflowDAO", back_populates="workflow_runs")


class WorkflowRun(object):
    """
    WorkflowRun enables tracking for multiple runs of a single Workflow. A
    Workflow may be started/paused/ and resumed multiple times. Each start or
    resume represents a new WorkflowRun.

    In order for a Workflow can be deemed to be DONE (successfully), it
    must have 1 or more WorkflowRuns. In the current implementation, a Workflow
    Job may belong to one or more WorkflowRuns, but once the Job reaches a DONE
    state, it will no longer be added to a subsequent WorkflowRun. However,
    this is not enforced via any database constraints.
    """

    def __init__(self, workflow_id, stderr, stdout, project,
                 slack_channel='jobmon-alerts'):
        self.workflow_id = workflow_id
        self.jsm_req = Requester(config.jm_rep_conn)
        self.stderr = stderr
        self.stdout = stdout
        self.project = project
        rc, wfr_id = self.jsm_req.send_request({
            'action': 'add_workflow_run',
            'kwargs': {'workflow_id': workflow_id,
                       'user': getpass.getuser(),
                       'hostname': socket.gethostname(),
                       'pid': os.getpid(),
                       'stderr': stderr,
                       'stdout': stdout,
                       'project': project,
                       'slack_channel': slack_channel,
                       }
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
