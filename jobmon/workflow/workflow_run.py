import getpass
import os
import socket
from datetime import datetime
import logging
from time import sleep

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String

from sqlalchemy.orm import relationship

from jobmon.config import config
from jobmon.exceptions import ReturnCodes, SGENotAvailable
from jobmon.requester import Requester
try:
    from jobmon.sge import qdel, qstat
except SGENotAvailable:
    pass
from jobmon.sql_base import Base
from jobmon.utils import kill_remote_process

logger = logging.getLogger(__name__)


class WorkflowRunStatus(Base):
    __tablename__ = 'workflow_run_status'

    RUNNING = 'R'
    STOPPED = 'S'
    ERROR = 'E'
    DONE = 'D'

    id = Column(String(1), primary_key=True)
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
    status = Column(String(1),
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
        self.jsm_req = Requester(config.jm_port)
        self.stderr = stderr
        self.stdout = stdout
        self.project = project
        self.kill_previous_workflow_runs()
        rc, wfr_id = self.jsm_req.send_request(
            app_route='/add_workflow_run',
            message={'workflow_id': workflow_id,
                     'user': getpass.getuser(),
                     'hostname': socket.gethostname(),
                     'pid': os.getpid(),
                     'stderr': stderr,
                     'stdout': stdout,
                     'project': project,
                     'slack_channel': slack_channel},
            request_type='post')
        if rc != ReturnCodes.OK:
            raise ValueError("Invalid Reponse to add_workflow_run")
        self.id = wfr_id

    def check_if_workflow_is_running(self):
        rc, status, wf_run_id, hostname, pid, user = \
            self.jsm_req.send_request(
                app_route='/is_workflow_running',
                message={'workflow_id': self.workflow_id},
                request_type='get')
        if rc != ReturnCodes.OK:
            raise ValueError("Invalid Reponse to is_workflow_running")
        return status, wf_run_id, hostname, pid, user

    def kill_previous_workflow_runs(self):
        """First check the database for last WorkflowRun... where we store a
        hostname + pid + running_flag

        If in the database as 'running,' check the hostname
        + pid to see if the process is actually still running:

            A) If so, kill those pids and any still running jobs
            B) Then flip the database of the previous WorkflowRun to STOPPED
        """
        status, wf_run_id, hostname, pid, user = \
            self.check_if_workflow_is_running()
        if not status:
            return
        if user != getpass.getuser():
            msg = ("Workflow_run_id {} for this workflow_id is still in "
                   "running mode by user {}. Please ask this user to kill "
                   "their processes and qdel their jobs. Be aware that if you "
                   "restart this workflow prior to the other user killing "
                   "theirs, this error will not re-raise but you may be "
                   "creating orphaned processes and hard-to-find bugs"
                   .format(wf_run_id, user))
            logger.error(msg)
            _, _ = self.jsm_req.send_request(
                app_route='/update_workflow_run',
                message={'workflow_run_id': wf_run_id,
                         'status': WorkflowRunStatus.STOPPED},
                request_type='post')
            raise RuntimeError(msg)
        else:
            kill_remote_process(hostname, pid)
            _, sge_ids = self.jsm_req.send_request(
                app_route='/get_sge_ids_of_previous_workflow_run',
                message={'workflow_run_id': wf_run_id},
                request_type='get')
            if sge_ids:
                qdel(sge_ids)
            self.poll_for_lagging_jobs(sge_ids)
            _, _ = self.jsm_req.send_request(
                app_route='/update_workflow_run',
                message={'workflow_run_id': wf_run_id,
                         'status': WorkflowRunStatus.STOPPED},
                request_type='post')

    def poll_for_lagging_jobs(self, sge_ids):
        lagging_jobs = qstat(jids=sge_ids)
        logger.info("Qdelling sge_ids {} from a previous workflow run, and "
                    "polling to ensure they disappear from qstat"
                    .format(sge_ids))
        seconds = 0
        while seconds <= 60 and lagging_jobs:
            seconds += 5
            sleep(5)
            lagging_jobs = qstat(jids=sge_ids)
            if seconds == 60 and lagging_jobs:
                raise RuntimeError("Polled for 60 seconds waiting for qdel-ed "
                                   "sge_ids {} to disappear from qstat but "
                                   "they still exist. Timing out."
                                   .format(lagging_jobs.job_id.unique()))

    def update_done(self):
        self._update_status(WorkflowRunStatus.DONE)

    def update_error(self):
        self._update_status(WorkflowRunStatus.ERROR)

    def update_stopped(self):
        self._update_status(WorkflowRunStatus.STOPPED)

    def _update_status(self, status):
        rc, wfr_id = self.jsm_req.send_request(
            app_route='/update_workflow_run',
            message={'wfr_id': self.id, 'status': status},
            request_type='post')
