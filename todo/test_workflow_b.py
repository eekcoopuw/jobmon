import os
import pytest

from jobmon.client import BashTask
from jobmon.client import PythonTask
from jobmon.client import Workflow
from jobmon.models.task_dag import TaskDagMeta
from jobmon.models.job import Job
from jobmon.models.job_instance_status import JobInstanceStatus
from jobmon.models.workflow_run_status import WorkflowRunStatus
from jobmon.models.workflow import Workflow as WorkflowDAO
from jobmon.models.workflow_status import WorkflowStatus
from jobmon.client.swarm.executors.base import ExecutorParameters
from jobmon.client.swarm.workflow.task_dag import DagExecutionStatus
from jobmon.client.swarm.workflow.workflow import WorkflowAlreadyComplete, \
    WorkflowAlreadyExists, ResumeStatus

import tests.workflow_utils as wu
from tests.conftest import teardown_db

path_to_file = os.path.dirname(__file__)


@pytest.mark.qsubs_jobs
def test_workflow_sge_args(db_cfg, env_var):
    """Test to make sure that the correct information is available to the
     worker cli. For some reason, there are times when this fails and whatever
     executor this workflow is pointing at has a working directory configured,
     but the executor that is actually qsubbing does not have a working
     directory configured. Is this because of the jqs and jsm interfering or a
     problem with the job instance reconcilers not accessing the correct
     executor somehow? """
    teardown_db(db_cfg)
    t1 = PythonTask(name="check_env",
                    script='{}/executor_args_check.py'
                    .format(os.path.dirname(os.path.realpath(__file__))),
                    num_cores=1, max_attempts=1)
    t2 = BashTask("sleep 2", upstream_tasks=[t1], num_cores=1)
    t3 = BashTask("sleep 3", upstream_tasks=[t2], num_cores=1)

    wfa = "sge_args_dag"
    workflow = Workflow(workflow_args=wfa, project='proj_tools',
                        working_dir='/ihme/centralcomp/auto_test_data',
                        stderr='/tmp', stdout='/tmp')
    workflow.add_tasks([t1, t2, t3])
    wf_status = workflow.execute()

    # TODO Grab a ref to the executor

    # If the working directory is not set correctly then executor_args_check.py
    # will fail and write its error message into the job_instance_error_log
    # table.
    # This test is flakey. Gather more information by printing out the error.
    # TODO this was added in May 2019, if the test has ceased to be flakey
    # by July then remove this extra logging.
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    if wf_status != DagExecutionStatus.SUCCEEDED:
        print(">>>>>> FLAKEY TEST test_workflow_sge_args FAILED <<<<<<<<<<<<<")
        with app.app_context():
            query = (
                "SELECT job_instance_id, description  "
                "FROM job_instance_error_log ")
            results = DB.session.execute(query)
            for row in results:
                print("Error: jid {}, description {}".format(row[0], row[1]))
            DB.session.commit()

    assert workflow.workflow_run.project == 'proj_tools'
    assert workflow.workflow_run.working_dir == (
        '/ihme/centralcomp/auto_test_data')
    assert workflow.workflow_run.stderr == '/tmp'
    assert workflow.workflow_run.stdout == '/tmp'
    assert workflow.workflow_run.executor_class == 'SGEExecutor'
    assert wf_status == DagExecutionStatus.SUCCEEDED
    teardown_db(db_cfg)


def test_same_wf_args_diff_dag(db_cfg, env_var):
    teardown_db(db_cfg)
    wf1 = Workflow(workflow_args="same", project='proj_tools')
    task1 = BashTask("sleep 2", num_cores=1)
    wf1.add_task(task1)

    wf2 = Workflow(workflow_args="same", project='proj_tools')
    task2 = BashTask("sleep 3", num_cores=1)
    wf2.add_task(task2)

    exit_status = wf1.execute()

    assert exit_status == 0

    with pytest.raises(WorkflowAlreadyExists):
        wf2.run()
    teardown_db(db_cfg)
