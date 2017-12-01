import logging
import sys
import pytest

from cluster_utils.io import makedirs_safely

from jobmon.models import JobStatus
from jobmon import sge
from .mock_sleep_and_write_task import SleepAndWriteFileMockTask

logger = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)


def test_resume_dag(db_cfg, jsm_jqs, task_dag_manager, tmp_out_dir):
    root_out_dir = "{}/mocks/test_resume_dag".format(tmp_out_dir)
    makedirs_safely(root_out_dir)
    dag = task_dag_manager.create_task_dag(name="test_resume_dag")
    command_script = sge.true_path("tests/remote_sleep_and_write.py")

    a_output_file_name = "{}/a.out".format(root_out_dir)
    task_a = SleepAndWriteFileMockTask(
        command=("python {cs} --sleep_secs 1 --output_file_path {ofn} "
                 "--name {n}".format(cs=command_script, ofn=a_output_file_name,
                                     n=a_output_file_name)),
        upstream_tasks=[]  # To be clear
    )
    dag.add_task(task_a)

    b_output_file_name = "{}/b.out".format(root_out_dir)
    task_b = SleepAndWriteFileMockTask(
        command=("python {cs} --sleep_secs 1 --output_file_path {ofn} "
                 "--name {n}".format(cs=command_script, ofn=b_output_file_name,
                                     n=b_output_file_name)),
        upstream_tasks=[task_a]
    )
    dag.add_task(task_b)

    c_output_file_name = "{}/c.out".format(root_out_dir)
    task_c = SleepAndWriteFileMockTask(
        command=("python {cs} --sleep_secs 1 --output_file_path {ofn} "
                 "--name {n}".format(cs=command_script, ofn=c_output_file_name,
                                     n=c_output_file_name)),
        upstream_tasks=[task_b]
    )
    dag.add_task(task_c)

    d_output_file_name = "{}/d.out".format(root_out_dir)
    task_d = SleepAndWriteFileMockTask(
        command=("python {cs} --sleep_secs 1 --output_file_path {ofn} "
                 "--name {n}".format(cs=command_script, ofn=d_output_file_name,
                                     n=d_output_file_name)),
        upstream_tasks=[task_c]
    )
    dag.add_task(task_d)

    e_output_file_name = "{}/e.out".format(root_out_dir)
    task_e = SleepAndWriteFileMockTask(
        command=("python {cs} --sleep_secs 1 --output_file_path {ofn} "
                 "--name {n}".format(cs=command_script, ofn=e_output_file_name,
                                     n=e_output_file_name)),
        upstream_tasks=[task_d]
    )
    dag.add_task(task_e)

    logger.debug("DAG: {}".format(dag))
    dag._set_fail_after_n_executions(2)  # set the dag to fail after 2 tasks
    logger.debug("in launcher, self.fail_after_n_executions is {}"
                 .format(dag.fail_after_n_executions))

    # ensure dag officially "fell over"
    with pytest.raises(ValueError):
        dag.execute()

    # ensure the dag that "fell over" has 2 out of the 5 jobs complete
    statuses = list(dag.job_list_manager.job_statuses.values())
    assert statuses[0] == JobStatus.DONE
    assert statuses[1] == JobStatus.DONE
    assert statuses[2] != JobStatus.DONE
    assert statuses[3] != JobStatus.DONE
    assert statuses[4] != JobStatus.DONE

    # relaunch dag, and ensure all tasks are marked complete now. the dag will
    # keep track of all completed tasks from last run of the dag, and so the
    # number of all_completed will be all 5
    dag._set_fail_after_n_executions(None)
    rc, all_completed, all_failed = dag.execute()
    assert rc is True
    assert all_completed == 5
    assert all_failed == 0
