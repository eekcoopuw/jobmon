from builtins import str
import getpass
import os
import socket
from datetime import datetime
import logging

from jobmon.client.the_client_config import get_the_client_config
from jobmon.models.job_instance import JobInstance
from jobmon.models.workflow_run_status import WorkflowRunStatus
from jobmon.client.requester import Requester
from jobmon.client.swarm.executors.sge_utils import get_project_limits
from jobmon.client.utils import kill_remote_process
from jobmon.attributes.constants import workflow_run_attribute

try:  # Python 3.5+
    from http import HTTPStatus as StatusCodes
except ImportError:
    try:  # Python 3
        from http import client as StatusCodes
    except ImportError:  # Python 2
        import httplib as StatusCodes

logger = logging.getLogger(__name__)


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
        self.requester = Requester(get_the_client_config(), 'jsm')
        self.stderr = stderr
        self.stdout = stdout
        self.project = project
        self.working_dir = working_dir
        self.kill_previous_workflow_runs(reset_running_jobs)
        rc, response = self.requester.send_request(
            app_route='/workflow_run',
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
        if rc != StatusCodes.OK:
            raise ValueError("Invalid Reponse to add_workflow_run")
        self.id = wfr_id
        self.add_project_limit_attribute('start')

    def add_project_limit_attribute(self, timing):
        if timing == 'start':
            atype = workflow_run_attribute.SLOT_LIMIT_AT_START
        else:
            atype = workflow_run_attribute.SLOT_LIMIT_AT_END
        limits = get_project_limits(self.project)
        self.add_workflow_run_attribute(attribute_type=atype, value=limits)

    def check_if_workflow_is_running(self):
        """Query the JQS to see if the workflow is currently running"""
        rc, response = \
            self.reqester.send_request(
                app_route='/workflow/{}/workflow_run'.format(self.workflow_id),
                message={},
                request_type='get')
        if rc != StatusCodes.OK:
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
        if wf_run['user'] != getpass.getuser():
            msg = ("Workflow_run_id {} for this workflow_id is still in "
                   "running mode by user {}. Please ask this user to kill "
                   "their processes. If they are using the SGE executor, "
                   "please ask them to qdel their jobs. Be aware that if you "
                   "restart this workflow prior to the other user killing "
                   "theirs, this error will not re-raise but you may be "
                   "creating orphaned processes and hard-to-find bugs"
                   .format(wf_run['id'], wf_run['user']))
            logger.error(msg)
            _, _ = self.requester.send_request(
                app_route='/workflow_run',
                message={'wfr_id': wf_run.id,
                         'status': WorkflowRunStatus.STOPPED},
                request_type='put')
            raise RuntimeError(msg)
        else:
            kill_remote_process(wf_run['hostname'], wf_run['pid'])
            logger.info("Kill previous workflow runs: {}".format(wf_run['id']))
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
                _, response = self.requester.send_request(
                    app_route=('/workflow_run/<workflow_run_id>/job_instance'
                               .format(wf_run.id)),
                    message={},
                    request_type='get')
                job_instances = [JobInstance.from_wire(ji)
                                 for ji in response['job_instances']]
                if job_instances:
                    previous_executor.terminate_job_instances(job_instances)
            _, _ = self.requester.send_request(
                app_route='/workflow_run',
                message={'workflow_run_id': wf_run.id,
                         'status': WorkflowRunStatus.STOPPED},
                request_type='put')

    def update_done(self):
        """Update the status of the workflow_run as done"""
        self.add_project_limit_attribute('end')
        self._update_status(WorkflowRunStatus.DONE)

    def update_error(self):
        """Update the status of the workflow_run as errored"""
        self.add_project_limit_attribute('end')
        self._update_status(WorkflowRunStatus.ERROR)

    def update_stopped(self):
        """Update the status of the workflow_run as stopped"""
        self.add_project_limit_attribute('end')
        self._update_status(WorkflowRunStatus.STOPPED)

    def _update_status(self, status):
        """Update the status of the workflow_run with whatever status is
        passed
        """
        rc, _ = self.requester.send_request(
            app_route='/workflow_run',
            message={'wfr_id': self.id, 'status': status,
                     'status_date': str(datetime.utcnow())},
            request_type='put')

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
        elif (not attribute_type == workflow_run_attribute.TAG and not
              int(value)) or (attribute_type == workflow_run_attribute.TAG and
                              not isinstance(value, str)):
            raise ValueError("Invalid value type: {}, {}"
                             .format(value,
                                     type(value).__name__))
        else:
            rc, workflow_run_attribute_id = self.requester.send_request(
                app_route='/workflow_run_attribute',
                message={'workflow_run_id': str(self.id),
                         'attribute_type': str(attribute_type),
                         'value': str(value)},
                request_type='post')
            return workflow_run_attribute_id
