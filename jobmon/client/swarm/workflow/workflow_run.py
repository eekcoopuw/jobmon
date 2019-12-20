from builtins import str
from datetime import datetime
import getpass
from http import HTTPStatus as StatusCodes
import os
import socket

from jobmon.client import shared_requester
from jobmon.client.swarm.job_management.executor_job_instance import \
    ExecutorJobInstance
from jobmon.models.attributes.constants import workflow_run_attribute
from jobmon.models.workflow_run_status import WorkflowRunStatus
from jobmon.client.client_logging import ClientLogging as logging

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
                 slack_channel='jobmon-alerts', executor_class='SGEExecutor',
                 working_dir=None, reset_running_jobs=True,
                 requester=shared_requester):
        self.workflow_id = workflow_id
        self.requester = requester
        self.stderr = stderr
        self.stdout = stdout
        self.project = project
        self.executor_class = executor_class
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
                     'executor_class': executor_class,
                     'working_dir': working_dir},
            request_type='post')
        wfr_id = response['workflow_run_id']
        if rc != StatusCodes.OK:
            raise ValueError(f"Invalid Response to add_workflow_run: {rc}")
        self.id = wfr_id

    def get_previous_workflow_run(self):
        rc, response = self.requester.send_request(
            app_route=f'/workflow/{self.workflow_id}/previous_workflow_run',
            message={},
            request_type='get')
        if rc != StatusCodes.OK:
            raise ValueError("Invalid Response to get_previous_workflow_run")
        return response['workflow_run']

    def kill_previous_workflow_runs(self, reset_running_jobs):
        """First check the database for last WorkflowRun... where we store a
        hostname + pid + running_flag. If there was a previous workflow run:

            A) Set the job instances to kill themselves in case qdel doesn't
               work and they enter a running state (If its a Cold Resume)
            B) Qdel all job instances from the prior workflow run (If its a
               Cold Resume)
            C) Mark the prior workflow run to Cold or Hot Resume in case it
               is still running elsewhere (though it should have been stopped
               in order to create a new workflow run) so that it will tear down
                its threads and prevent any new job instance creation
        """
        old_wf_run = self.get_previous_workflow_run()
        if len(old_wf_run) == 0:
            return
        old_workflow_run_id = old_wf_run['id']
        # instead of killing the old workflow runs, mark them as CR or HR,
        logger.info(f"Set previous workflow run to resume: "
                    f"{old_workflow_run_id}")
        if reset_running_jobs:
            if old_wf_run['executor_class'] == "SequentialExecutor":
                from jobmon.client.swarm.executors.sequential import \
                    SequentialExecutor
                previous_executor = SequentialExecutor()
            elif old_wf_run['executor_class'] == "SGEExecutor":
                from jobmon.client.swarm.executors.sge import SGEExecutor
                previous_executor = SGEExecutor()
            elif old_wf_run['executor_class'] == "DummyExecutor":
                from jobmon.client.swarm.executors.dummy import \
                    DummyExecutor
                previous_executor = DummyExecutor()
            else:
                raise ValueError("{} is not supported by this version of "
                                 "jobmon".format(old_wf_run['executor_class']))
            # set non-terminal job instances to k state
            logger.info(f'Setting job instances with B, I, and R state to'
                        f' K for the following workflow_run_id: '
                        f'{old_workflow_run_id}')
            k_state_code, k_state_resp = self.requester.send_request(
                app_route=f'/job_instance/{old_workflow_run_id}/'
                f'nonterminal_to_k_status',
                message={},
                request_type='post'
            )
            if k_state_code != StatusCodes.OK:
                err_msg = f'Expected code {StatusCodes.OK} when setting ' \
                    f'nonterminal job instances to K state, ' \
                    f'instead got status: {k_state_code} with ' \
                    f'the following response: {k_state_resp}'
                logger.error(err_msg)
                raise RuntimeError(err_msg)

            # get all job instances from workflow run because even if they
            # are in K state they may not have had qdel attempted yet
            _, response = self.requester.send_request(
                app_route=f'/workflow_run/{old_workflow_run_id}/job_instance',
                message={},
                request_type='get')
            job_instances = [ExecutorJobInstance.from_wire(
                ji, executor=previous_executor)
                for ji in response['job_instances']]
            jiid_exid_tuples = [(ji.job_instance_id, ji.executor_id)
                                for ji in job_instances]
            if job_instances:
                previous_executor.terminate_job_instances(jiid_exid_tuples)
            # set workflow_run status to cold resume
            logger.info(f"Setting this workflow run status to "
                        f"{WorkflowRunStatus.COLD_RESUME}: "
                        f"{old_workflow_run_id}")

            _, _ = self.requester.send_request(
                app_route='/workflow_run',
                message={'workflow_run_id': old_workflow_run_id,
                         'status': WorkflowRunStatus.COLD_RESUME},
                request_type='put')
        else:
            # A hot resume
            logger.info(f"Setting this workflow run status to "
                        f"{WorkflowRunStatus.HOT_RESUME}: "
                        f"{old_workflow_run_id}")
            _, _ = self.requester.send_request(
                app_route='/workflow_run',
                message={'workflow_run_id': old_workflow_run_id,
                         'status': WorkflowRunStatus.HOT_RESUME},
                request_type='put')

    def update_done(self):
        """Update the status of the workflow_run as done"""
        self._update_status(WorkflowRunStatus.DONE)

    def update_error(self):
        """Update the status of the workflow_run as errored"""
        self._update_status(WorkflowRunStatus.ERROR)

    def update_stopped(self):
        """Update the status of the workflow_run as stopped"""
        self._update_status(WorkflowRunStatus.STOPPED)

    def _update_status(self, status):
        """Update the status of the workflow_run with whatever status is
        passed
        """
        rc, _ = self.requester.send_request(
            app_route='/workflow_run',
            message={'workflow_run_id': self.id, 'status': status,
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
