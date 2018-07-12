from subprocess import check_output
from time import sleep

from cluster_utils.io import makedirs_safely

from jobmon import sge
from jobmon.database import session_scope
from jobmon.executors.sge import SGEExecutor
from jobmon.models import Job, JobStatus
from jobmon.workflow.bash_task import BashTask
from jobmon.workflow.python_task import PythonTask
from jobmon.workflow.r_task import RTask
from jobmon.workflow.stata_task import StataTask
from jobmon.workflow.task_dag import DagExecutionStatus


def match_name_to_sge_name(jid):
    # Try this a couple of times... SGE is weird
    retries = 5
    while retries > 0:
        try:
            sge_jobname = check_output(
                "qacct -j {} | grep jobname".format(jid),
                shell=True).decode()
            break
        except:
            try:
                sge_jobname = check_output(
                    "qstat -j {} | grep job_name".format(jid),
                    shell=True).decode()
                break
            except:
                pass
            sleep(5 - retries)
            retries = retries - 1
            if retries == 0:
                raise RuntimeError("Attempted to use qstat to get jobname. "
                                   "Giving up after 5 retries")
    sge_jobname = sge_jobname.split()[-1].strip()
    return sge_jobname


def get_task_status(dag, task):
    job_list_manager = dag.job_list_manager
    return job_list_manager.status_from_task(task)


def test_bash_task(dag_factory):
    """
    Create a dag with one very simple BashTask and execute it
    """
    name = 'bash_task'
    task = BashTask(command="date", name=name, mem_free=1, max_attempts=2,
                    max_runtime=60)
    executor = SGEExecutor(project='proj_jenkins')
    dag = dag_factory(executor)
    dag.add_task(task)
    (rc, num_completed, num_previously_complete, num_failed) = (dag._execute())

    assert rc == DagExecutionStatus.SUCCEEDED
    assert num_completed == 1
    assert get_task_status(dag, task) == JobStatus.DONE

    with session_scope() as session:
        job = session.query(Job).filter_by(name=name).first()
        jid = [ji for ji in job.job_instances][0].executor_id
        assert job.mem_free == 1
        assert job.max_attempts == 2
        assert job.max_runtime == 60

    sge_jobname = match_name_to_sge_name(jid)
    assert sge_jobname == name


def test_python_task(dag_factory, tmp_out_dir):
    """
    Execute a PythonTask
    """
    name = 'python_task'
    root_out_dir = "{t}/mocks/{n}".format(t=tmp_out_dir, n=name)
    makedirs_safely(root_out_dir)

    output_file_name = "{t}/mocks/{n}/mock.out".format(t=tmp_out_dir, n=name)

    task = PythonTask(script=sge.true_path("tests/remote_sleep_and_write.py"),
                      args=["--sleep_secs", "1",
                            "--output_file_path", output_file_name,
                            "--name", name],
                      name=name, mem_free=1, max_attempts=2, max_runtime=60)
    executor = SGEExecutor(project='proj_jenkins')
    dag = dag_factory(executor)
    dag.add_task(task)
    (rc, num_completed, num_previously_complete, num_failed) = (dag._execute())

    assert rc == DagExecutionStatus.SUCCEEDED
    assert num_completed == 1
    assert get_task_status(dag, task) == JobStatus.DONE

    with session_scope() as session:
        job = session.query(Job).filter_by(name=name).first()
        jid = [ji for ji in job.job_instances][0].executor_id
        assert job.mem_free == 1
        assert job.max_attempts == 2
        assert job.max_runtime == 60

    sge_jobname = match_name_to_sge_name(jid)
    assert sge_jobname == name


def test_R_task(dag_factory, tmp_out_dir):
    """
    Execute an RTask
    """
    name = 'r_task'

    root_out_dir = "{t}/mocks/{n}".format(t=tmp_out_dir, n=name)
    makedirs_safely(root_out_dir)

    task = RTask(script=sge.true_path("tests/simple_R_script.r"), name=name,
                 mem_free=1, max_attempts=2, max_runtime=60)
    executor = SGEExecutor(project='proj_jenkins')
    dag = dag_factory(executor)
    dag.add_task(task)
    (rc, num_completed, num_previously_complete, num_failed) = (dag._execute())

    assert rc == DagExecutionStatus.SUCCEEDED
    assert num_completed == 1
    assert get_task_status(dag, task) == JobStatus.DONE

    with session_scope() as session:
        job = session.query(Job).filter_by(name=name).first()
        jid = [ji for ji in job.job_instances][0].executor_id
        assert job.mem_free == 1
        assert job.max_attempts == 2
        assert job.max_runtime == 60

    sge_jobname = match_name_to_sge_name(jid)
    assert sge_jobname == name


def test_stata_task(dag_factory, tmp_out_dir):
    """
    Execute a simple stata Task
    """
    name = 'stata_task'
    root_out_dir = "{t}/mocks/{n}".format(t=tmp_out_dir, n=name)
    makedirs_safely(root_out_dir)

    task = StataTask(script=sge.true_path("tests/simple_stata_script.do"),
                     name=name, mem_free=1, max_attempts=2, max_runtime=60)
    executor = SGEExecutor(project='proj_jenkins')
    dag = dag_factory(executor)
    dag.add_task(task)
    (rc, num_completed, num_previously_complete, num_failed) = (dag._execute())

    assert rc == DagExecutionStatus.SUCCEEDED
    assert num_completed == 1
    assert get_task_status(dag, task) == JobStatus.DONE

    with session_scope() as session:
        job = session.query(Job).filter_by(name=name).first()
        jid = [ji for ji in job.job_instances][0].executor_id
        assert job.mem_free == 1
        assert job.max_attempts == 2
        assert job.max_runtime == 60

    sge_jobname = match_name_to_sge_name(jid)
    assert sge_jobname == name
