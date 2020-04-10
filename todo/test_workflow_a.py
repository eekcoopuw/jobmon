import os
import pytest
import subprocess
import uuid
from time import sleep

from jobmon.client import BashTask
from jobmon.client import StataTask
from jobmon.client import Workflow
from jobmon.models.job import Job
from jobmon.models.job_instance_status import JobInstanceStatus
from jobmon.models.job_instance import JobInstance
from jobmon.models.job_status import JobStatus
from jobmon.models.workflow_run import WorkflowRun as WorkflowRunDAO
from jobmon.models.workflow_run_status import WorkflowRunStatus
from jobmon.models.workflow import Workflow as WorkflowDAO
from jobmon.models.workflow_status import WorkflowStatus
from jobmon.client import shared_requester as req
from jobmon.client.swarm.workflow.task_dag import DagExecutionStatus
from jobmon.client.swarm.workflow.workflow import WorkflowAlreadyComplete, \
    ResumeStatus
from tests.conftest import teardown_db
import tests.workflow_utils as wu
from tests.conftest import teardown_db


path_to_file = os.path.dirname(__file__)


def test_nodename_on_fail(db_cfg, simple_workflow_w_errors):
    err_wf = simple_workflow_w_errors
    dag_id = err_wf.task_dag.dag_id

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():

        # Get ERROR job instances
        jobs = DB.session.query(Job).filter_by(dag_id=dag_id).all()
        jobs = [j for j in jobs if j.status == JobStatus.ERROR_FATAL]
        jis = [ji for job in jobs for ji in job.job_instances
               if ji.status == JobInstanceStatus.ERROR]
        DB.session.commit()

        # Make sure all their node names were recorded
        nodenames = [ji.nodename for ji in jis]
        assert nodenames and all(nodenames)
    teardown_db(db_cfg)
