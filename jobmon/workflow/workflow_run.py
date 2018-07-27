from builtins import str
import getpass
import os
import socket
from datetime import datetime
import logging

from http import HTTPStatus
from sqlalchemy import Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from jobmon.config import config
from jobmon.models import JobInstance
from jobmon.requester import Requester
from jobmon.sql_base import Base
from jobmon.utils import kill_remote_process
from jobmon.attributes.constants import workflow_run_attribute

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
    working_dir = Column(String(1000), default=None)
    slack_channel = Column(String(150))
    executor_class = Column(String(150))
    created_date = Column(DateTime, default=datetime.utcnow)
    status_date = Column(DateTime, default=datetime.utcnow)
    status = Column(String(1),
                    ForeignKey('workflow_run_status.id'),
                    nullable=False,
                    default=WorkflowRunStatus.RUNNING)

    workflow = relationship("WorkflowDAO", back_populates="workflow_runs")

    @classmethod
    def from_wire(cls, dct):
        return cls(
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
                 slack_channel='jobmon-alerts', working_dir=None,
                 reset_running_jobs=True):
        self.workflow_id = workflow_id
        self.jsm_req = Requester(config.jsm_port)
        self.jqs_req = Requester(config.jqs_port)
        self.stderr = stderr
        self.stdout = stdout
        self.project = project
        self.working_dir = working_dir
        self.kill_previous_workflow_runs(reset_running_jobs)
        rc, response = self.jsm_req.send_request(
            app_route='/add_workflow_run',
            message={'workflow_id': workflow_id,
                     'user': getpass.getuser(),
                     'hostname': socket.gethostname(),
                     'pid': os.getpid(),
                     'stderr': stderr,
                     'stdout': stdout,
                     'project': project,
                     'slack_channel': slack_channel,
                     'working_dir': self.working_dir},
            request_type='post')
        wfr_id = response['workflow_run_id']
        if rc != HTTPStatus.OK:
            raise ValueError("Invalid Reponse to add_workflow_run")
        self.id = wfr_id

    def check_if_workflow_is_running(self):
        rc, response = \
            self.jqs_req.send_request(
                app_route='/workflow/{}/workflow_run'.format(self.workflow_id),
                message={},
                request_type='get')
        if rc != HTTPStatus.OK:
            raise ValueError("Invalid Response to is_workflow_running")
        return response['is_running'], response['workflow_run_dct']

    def kill_previous_workflow_runs(self, reset_running_jobs):
        """First check the database for last WorkflowRun... where we store a
        hostname + pid + running_flag

        If in the database as 'running,' check the hostname
        + pid to see if the process is actually still running:

            A) If so, kill those pids and any still running jobs
            B) Then flip the database of the previous WorkflowRun to STOPPED
        """
        status, wf_run = self.check_if_workflow_is_running()
        if not status:
            return
        if wf_run.user != getpass.getuser():
            msg = ("Workflow_run_id {} for this workflow_id is still in "
                   "running mode by user {}. Please ask this user to kill "
                   "their processes. If they are using the SGE executor, "
                   "please ask them to qdel their jobs. Be aware that if you "
                   "restart this workflow prior to the other user killing "
                   "theirs, this error will not re-raise but you may be "
                   "creating orphaned processes and hard-to-find bugs"
                   .format(wf_run.id, wf_run.user))
            logger.error(msg)
            _, _ = self.jsm_req.send_request(
                app_route='/update_workflow_run',
                message={'workflow_run_id': wf_run.id,
                         'status': WorkflowRunStatus.STOPPED},
                request_type='post')
            raise RuntimeError(msg)
        else:
            kill_remote_process(wf_run.hostname, wf_run.pid)
            if reset_running_jobs:
                if wf_run.executor_class == "SequentialExecutor":
                    from jobmon.executors.sequential import SequentialExecutor
                    previous_executor = SequentialExecutor()
                elif wf_run.executor_class == "SGEExecutor":
                    from jobmon.executors.sequential import SGEExecutor
                    previous_executor = SGEExecutor()
                elif wf_run.executor_class == "DummyExecutor":
                    from jobmon.executors.sequential import DummyExecutor
                    previous_executor = DummyExecutor()
                else:
                    raise ValueError("{} is not supported by this version of "
                                     "jobmon".format(wf_run.executor_class))
                # get job instances of workflow run
                _, response = self.jqs_req.send_request(
                    app_route=('/workflow_run/<workflow_run_id>/job_instance'
                               .format(wf_run.id)),
                    message={},
                    request_type='get')
                job_instances = [JobInstance.from_wire(ji)
                                 for ji in response['job_instances']]
                if job_instances:
                    previous_executor.terminate_job_instances(job_instances)
            _, _ = self.jsm_req.send_request(
                app_route='/update_workflow_run',
                message={'workflow_run_id': wf_run.id,
                         'status': WorkflowRunStatus.STOPPED},
                request_type='post')

    def update_done(self):
        self._update_status(WorkflowRunStatus.DONE)

    def update_error(self):
        self._update_status(WorkflowRunStatus.ERROR)

    def update_stopped(self):
        self._update_status(WorkflowRunStatus.STOPPED)

    def _update_status(self, status):
        rc, _ = self.jsm_req.send_request(
            app_route='/update_workflow_run',
            message={'wfr_id': self.id, 'status': status},
            request_type='post')

    def add_workflow_run_attribute(self, attribute_type, value):
        """
        Create a workflow_run attribute entry in the database.

        Args:
            attribute_type (int): attribute_type id from
                                  workflow_run_attribute_type table
            value (int): value associated with attribute

        Raises:
            ValueError: If the args are not valid.
                        attribute_type should be int and
                        value should be convertible to int
                        or be string for TAG attribute
        """
        if not isinstance(attribute_type, int):
            raise ValueError("Invalid attribute_type: {}, {}"
                             .format(attribute_type,
                                     type(attribute_type).__name__))
        elif (not attribute_type == workflow_run_attribute.TAG and
              not int(value)
              ) or (attribute_type == workflow_run_attribute.TAG and
                    not isinstance(value, str)):
            raise ValueError("Invalid value type: {}, {}"
                             .format(value,
                                     type(value).__name__))
        else:
            rc, workflow_run_attribute_id = self.jsm_req.send_request(
                app_route='/add_workflow_run_attribute',
                message={'workflow_run_id': str(self.id),
                         'attribute_type': str(attribute_type),
                         'value': str(value)},
                request_type='post')
            return workflow_run_attribute_id
