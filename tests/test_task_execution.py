
from cluster_utils.io import makedirs_safely

from jobmon import sge
from jobmon.models import JobStatus
from jobmon.workflow.task_dag import TaskDag
from jobmon.workflow.bash_task import BashTask
from jobmon.workflow.python_task import PythonTask
from jobmon.workflow.r_task import RTask
from jobmon.workflow.stata_task import StataTask


def test_bash_task(db_cfg, jsm_jqs):
    """
    Create a dag with one very simple BashTask and execute it
    """
    name = "test_bash_task"
    dag = TaskDag(name=name)

    task = BashTask(command="date", project="proj_jenkins")
    dag.add_task(task)
    (rc, num_completed, num_previously_complete, num_failed) = dag.execute()

    assert rc
    assert num_completed == 1
    assert task.status == JobStatus.DONE


def test_python_task(db_cfg, jsm_jqs, tmp_out_dir):
    """
    Execute a PythonTask
    """
    name = "test_python_task"
    dag = TaskDag(name=name)

    root_out_dir = "{t}/mocks/{n}".format(t=tmp_out_dir, n=name)
    makedirs_safely(root_out_dir)

    output_file_name = "{t}/mocks/{n}/mock.out".format(t=tmp_out_dir, n=name)

    task = PythonTask(script=sge.true_path("tests/remote_sleep_and_write.py"),
                      args=["--sleep_secs", "1",
                            "--output_file_path", output_file_name,
                            "--name", name],
                      project="proj_jenkins")
    dag.add_task(task)
    (rc, num_completed, num_previously_complete, num_failed) = dag.execute()

    assert rc
    assert num_completed == 1
    assert task.status == JobStatus.DONE


def test_R_task(db_cfg, jsm_jqs, tmp_out_dir):
    """
    Execute an RTask
    """
    name = "test_R_task"
    dag = TaskDag(name=name)

    root_out_dir = "{t}/mocks/{n}".format(t=tmp_out_dir, n=name)
    makedirs_safely(root_out_dir)

    task = RTask(script=sge.true_path("tests/simple_R_script.r"), project="proj_jenkins")
    dag.add_task(task)
    (rc, num_completed, num_previously_complete, num_failed) = dag.execute()

    assert rc
    assert num_completed == 1
    assert task.status == JobStatus.DONE


def test_stata_task(db_cfg, jsm_jqs, tmp_out_dir):
    """
    Execute a simple stata Task
    """
    name = "test_stata_task"
    dag = TaskDag(name=name)

    root_out_dir = "{t}/mocks/{n}".format(t=tmp_out_dir, n=name)
    makedirs_safely(root_out_dir)

    task = StataTask(script=sge.true_path("tests/simple_stata_script.do"), project="proj_jenkins")
    dag.add_task(task)
    (rc, num_completed, num_previously_complete, num_failed) = dag.execute()

    assert rc
    assert num_completed == 1
    assert task.status == JobStatus.DONE
