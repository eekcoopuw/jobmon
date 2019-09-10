import os
import pytest
import subprocess
import uuid
from time import sleep
from multiprocessing import Process

from jobmon import BashTask
from jobmon import PythonTask
from jobmon import StataTask
from jobmon import Workflow
from jobmon.models.task_dag import TaskDagMeta
from jobmon.models.job import Job
from jobmon.models.job_instance_status import JobInstanceStatus
from jobmon.models.job_instance import JobInstance
from jobmon.models.job_status import JobStatus
from jobmon.models.workflow_run import WorkflowRun as WorkflowRunDAO
from jobmon.models.workflow_run_status import WorkflowRunStatus
from jobmon.models.workflow import Workflow as WorkflowDAO
from jobmon.models.workflow_status import WorkflowStatus
from jobmon.client import shared_requester as req
from jobmon.client import client_config
from jobmon.client.swarm.executors import sge_utils
from jobmon.client.swarm.executors.base import ExecutorParameters
from jobmon.client.swarm.workflow.task_dag import DagExecutionStatus
from jobmon.client.swarm.workflow.workflow import WorkflowAlreadyComplete, \
    WorkflowAlreadyExists, ResumeStatus
from jobmon.client.utils import gently_kill_command

path_to_file = os.path.dirname(__file__)


def cleanup_jlm(workflow):
    """If a dag is not completed properly and all threads are disconnected,
    a new job list manager will access old job instance factory/reconciler
    threads instead of creating new ones. So we need to make sure threads get
    cleand up at the end"""

    if workflow.task_dag.job_list_manager:
        workflow.task_dag.job_list_manager.disconnect()


@pytest.fixture
def fast_heartbeat():
    old_heartbeat = client_config.heartbeat_interval
    client_config.heartbeat_interval = 10
    yield
    client_config.heartbeat_interval = old_heartbeat


def mock_slack(msg, channel):
    print("{} to be posted to channel: {}".format(msg, channel))


