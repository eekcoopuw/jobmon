import logging
import os
import pytest
import sys
import time

from datetime import datetime, timedelta

from cluster_utils.io import makedirs_safely

from jobmon.client.swarm.executors import sge_utils
from jobmon.models.job_status import JobStatus
from jobmon.client.swarm.workflow.task_dag import DagExecutionStatus
from jobmon.client.swarm.workflow.bash_task import BashTask
from jobmon.models.job import Job
from jobmon.models.workflow_status import WorkflowStatus
from .mock_sleep_and_write_task import SleepAndWriteFileMockTask

logger = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)
path_to_file = os.path.dirname(__file__)


@pytest.mark.qsubs_jobs
def test_resume_real_dag(real_dag, tmp_out_dir):
    root_out_dir = "{}/mocks/test_resume_real_dag".format(tmp_out_dir)
    makedirs_safely(root_out_dir)
    command_script = sge_utils.true_path(f"{path_to_file}/remote_sleep_and_write.py")
    a_output_file_name = "{}/a.out".format(root_out_dir)
    task_a = SleepAndWriteFileMockTask(
        command=("python {cs} --sleep_secs 1 --output_file_path {ofn} "
                 "--name {n}".format(cs=command_script, ofn=a_output_file_name,
                                     n=a_output_file_name)),
        upstream_tasks=[]  # To be clear
    )
    real_dag.add_task(task_a)

    b_output_file_name = "{}/b.out".format(root_out_dir)
    task_b = SleepAndWriteFileMockTask(
        command=("python {cs} --sleep_secs 1 --output_file_path {ofn} "
                 "--name {n}".format(cs=command_script, ofn=b_output_file_name,
                                     n=b_output_file_name)),
        upstream_tasks=[task_a]
    )
    real_dag.add_task(task_b)

    c_output_file_name = "{}/c.out".format(root_out_dir)
    task_c = SleepAndWriteFileMockTask(
        command=("python {cs} --sleep_secs 1 --output_file_path {ofn} "
                 "--name {n}".format(cs=command_script, ofn=c_output_file_name,
                                     n=c_output_file_name)),
        upstream_tasks=[task_b]
    )
    real_dag.add_task(task_c)

    d_output_file_name = "{}/d.out".format(root_out_dir)
    task_d = SleepAndWriteFileMockTask(
        command=("python {cs} --sleep_secs 1 --output_file_path {ofn} "
                 "--name {n}".format(cs=command_script, ofn=d_output_file_name,
                                     n=d_output_file_name)),
        upstream_tasks=[task_c]
    )
    real_dag.add_task(task_d)

    e_output_file_name = "{}/e.out".format(root_out_dir)
    task_e = SleepAndWriteFileMockTask(
        command=("python {cs} --sleep_secs 1 --output_file_path {ofn} "
                 "--name {n}".format(cs=command_script, ofn=e_output_file_name,
                                     n=e_output_file_name)),
        upstream_tasks=[task_d]
    )
    real_dag.add_task(task_e)

    logger.debug("real_dag: {}".format(real_dag))
    # set the real_dag to fail after 2 tasks
    real_dag._set_fail_after_n_executions(2)
    logger.debug("in launcher, self.fail_after_n_executions is {}"
                 .format(real_dag.fail_after_n_executions))

    # ensure real_dag officially "fell over"
    with pytest.raises(ValueError):
        real_dag._execute()

    # ensure the real_dag that "fell over" has 2 out of the 5 jobs complete
    logger.debug(f"All completed are {real_dag.job_list_manager.all_done}")
    bound_tasks = list(real_dag.job_list_manager.bound_tasks.values())
    assert bound_tasks[0].status == JobStatus.DONE
    assert bound_tasks[1].status == JobStatus.DONE
    assert bound_tasks[2].status != JobStatus.DONE
    assert bound_tasks[3].status != JobStatus.DONE
    assert bound_tasks[4].status != JobStatus.DONE

    # relaunch real_dag, and ensure all tasks are marked complete now.
    # the real_dag will keep track of all completed tasks from last run of
    # the real_dag, and so the number of all_completed will be all 5
    real_dag._set_fail_after_n_executions(None)
    real_dag.bind_to_db(real_dag.dag_id)
    status, all_completed, all_previously_complete, \
        all_failed = real_dag._execute()
    assert status == DagExecutionStatus.SUCCEEDED
    assert all_previously_complete == 2
    assert all_completed == 3
    assert all_failed == 0


@pytest.mark.qsubs_jobs
def test_reset_running_jobs_false(real_dag, db_cfg):
    name1 = 'over_runtime_task1'
    task_1 = BashTask(command='sleep 10', name=name1, max_runtime_seconds=1)
    real_dag.add_task(task_1)
    name2 = 'over_runtime_task2'
    task_2 = BashTask(command='sleep 9', name=name2, max_runtime_seconds=1)
    real_dag.add_task(task_2)
    real_dag._execute()

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        job1 = DB.session.query(Job).filter_by(name=name1).first()
        assert job1.status == 'F'
        job2 = DB.session.query(Job).filter_by(name=name1).first()
        assert job2.status == 'F'

    real_dag._set_fail_after_n_executions(None)
    real_dag.bind_to_db(real_dag.dag_id, reset_running_jobs=False)
    with app.app_context():
        job1 = DB.session.query(Job).filter_by(name=name1).first()
        assert job1.status == 'E'
        job2 = DB.session.query(Job).filter_by(name=name1).first()
        assert job2.status == 'E'


@pytest.mark.qsubs_jobs
def test_resume_dag_heartbeat_race_condition(simple_workflow, db_cfg):
    # testing the fix for the following:
    # GBDSCI-2321, release 1.1.2 only:
    # set workflow run to running when logging heartbeat so if a resumed
    # workflow errors out due to a heartbeat from a previous workflow run
    # it will be corrected.
    app = db_cfg['app']
    DB = db_cfg['DB']

    with app.app_context():
        # wait for workflow to complete
        tries = 0
        while True:
            if simple_workflow.status == WorkflowStatus.DONE:
                break
            elif tries > 5:
                # raise error? or something else?
                pass
            else:
                tries += 1
                time.sleep(5)

        # mark workflow as failed
        query = """
            UPDATE workflow
            SET workflow_status = 'E'
            WHERE dag_id = :dag_id
        """
        DB.session.execute(query, {'dag_id': simple_workflow.dag_id})

        # mark one job status as failed

        # then change the heartbeat date to simulate the race condition
        query = """
            UPDATE task_dag
            SET heartbeat_date = :new_time, 
            WHERE dag_id = :dag_id
        """
        new_time = datetime.utcnow() - timedelta(minutes=15)
        DB.session.execute(query, {'new_time': new_time,
                                   'dag_id': simple_workflow.dag_id})
        DB.commit()
        #
        # now try and resume the workflow
        simple_workflow.resume = True
        simple_workflow.reset_running_jobs = True
        simple_workflow.execute()

        # check that new

        # check what..?
        # that it is in error state?
        # assert workflow.status == WorkflowStatus.ERROR  ??
        # then changes to running after first heartbeat?
        # assert heartbeat_date > workflow_created_date ??

