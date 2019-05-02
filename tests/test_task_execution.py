import os
import pytest
from subprocess import check_output
from time import sleep

from cluster_utils.io import makedirs_safely

from jobmon.client.swarm.executors import sge_utils as sge
from jobmon.client.swarm.executors.sge import SGEExecutor
from jobmon.models.job import Job
from jobmon.models.job_status import JobStatus
from jobmon.client.swarm.workflow.bash_task import BashTask
from jobmon.client.swarm.workflow.python_task import PythonTask
from jobmon.client.swarm.workflow.r_task import RTask
from jobmon.client.swarm.workflow.stata_task import StataTask
from jobmon.client.swarm.workflow.task_dag import DagExecutionStatus


def match_name_to_sge_name(jid):
    # Try this a couple of times... SGE is weird
    retries = 10
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
            sleep(10 - retries)
            retries = retries - 1
            if retries == 0:
                raise RuntimeError("Attempted to use qstat to get jobname. "
                                   "Giving up after {} "
                                   "retries".format(retries))
    sge_jobname = sge_jobname.split()[-1].strip()
    return sge_jobname


def get_task_status(real_dag, task):
    job_list_manager = real_dag.job_list_manager
    return job_list_manager.status_from_task(task)


@pytest.mark.qsubs_jobs
def test_bash_task(db_cfg, dag_factory):
    """Create a dag with one very simple BashTask and execute it"""
    name = 'bash_task'
    task = BashTask(command="date", name=name, mem_free='1G', max_attempts=2,
                    slots=1, max_runtime_seconds=60)
    executor = SGEExecutor(project='proj_tools')
    real_dag = dag_factory(executor)
    real_dag.add_task(task)
    (rc, num_completed, num_previously_complete, num_failed) = (
        real_dag._execute())

    assert rc == DagExecutionStatus.SUCCEEDED
    assert num_completed == 1
    assert get_task_status(real_dag, task) == JobStatus.DONE

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        job = DB.session.query(Job).filter_by(name=name).first()
        jid = [ji for ji in job.job_instances][0].executor_id
        assert job.mem_free == '1G'
        assert job.max_attempts == 2
        assert job.max_runtime_seconds == 60

    sge_jobname = match_name_to_sge_name(jid)
    assert sge_jobname == name


@pytest.mark.qsubs_jobs
def test_python_task(db_cfg, dag_factory, tmp_out_dir):
    """Execute a PythonTask"""
    name = 'python_task'
    root_out_dir = "{t}/mocks/{n}".format(t=tmp_out_dir, n=name)
    makedirs_safely(root_out_dir)

    output_file_name = "{t}/mocks/{n}/mock.out".format(t=tmp_out_dir, n=name)

    task = PythonTask(script=sge.true_path("tests/remote_sleep_and_write.py"),
                      args=["--sleep_secs", "1",
                            "--output_file_path", output_file_name,
                            "--name", name],
                      name=name, mem_free='1G', max_attempts=2, slots=1,
                      max_runtime_seconds=60)

    executor = SGEExecutor(project='proj_tools')
    real_dag = dag_factory(executor)
    real_dag.add_task(task)
    (rc, num_completed, num_previously_complete, num_failed) = (
        real_dag._execute())

    assert rc == DagExecutionStatus.SUCCEEDED
    assert num_completed == 1
    assert get_task_status(real_dag, task) == JobStatus.DONE

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        job = DB.session.query(Job).filter_by(name=name).first()
        jid = [ji for ji in job.job_instances][0].executor_id
        assert job.mem_free == '1G'
        assert job.max_attempts == 2
        assert job.max_runtime_seconds == 60

    sge_jobname = match_name_to_sge_name(jid)
    assert sge_jobname == name


def test_exceed_mem_task(db_cfg, dag_factory):
    name = 'mem_task'
    task = PythonTask(script=sge.true_path("tests/exceed_mem.py"),
                      name=name, mem_free='130M', max_attempts=2, slots=1,
                      max_runtime_seconds=40)

    executor = SGEExecutor(project='proj_tools')
    real_dag = dag_factory(executor)
    real_dag.add_task(task)
    (rc, num_completed, num_previously_complete, num_failed) = (
        real_dag._execute())

    ret_vals = get_task_status(real_dag, task)

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        job = DB.session.query(Job).filter_by(name=name).first()
        jid = [ji for ji in job.job_instances][0].executor_id
        resp = check_output(f"qacct -j {jid} | grep exit_status", shell=True,
                            universal_newlines=True)
        assert '247' in resp
        assert job.job_instances[0].status == 'E'
        assert job.status == 'F'

    sge_jobname = match_name_to_sge_name(jid)
    assert sge_jobname == name


def test_under_request_then_pass(db_cfg, dag_factory):
    name = 'mem_task'
    task = PythonTask(script=sge.true_path("tests/exceed_mem.py"),
                      name=name, mem_free='600M', max_attempts=2, slots=1,
                      max_runtime_seconds=40)

    executor = SGEExecutor(project='proj_tools')
    real_dag = dag_factory(executor)
    real_dag.add_task(task)
    (rc, num_completed, num_previously_complete, num_failed) = (
        real_dag._execute())

    ret_vals = get_task_status(real_dag, task)

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        job = DB.session.query(Job).filter_by(name=name).first()
        jid = [ji for ji in job.job_instances][0].executor_id
        resp = check_output(f"qacct -j {jid} | grep exit_status", shell=True,
                            universal_newlines=True)
        assert '247' in resp
        assert job.job_instances[0].status == 'E'
        assert job.job_instances[1].status == 'D'
        assert job.status == 'D'

    sge_jobname = match_name_to_sge_name(jid)
    assert sge_jobname == name


def test_kill_self_task(db_cfg, dag_factory):
    name = 'kill_self_task'
    task = PythonTask(script=sge.true_path("tests/kill.py"),
                      name=name, mem_free='130M', max_attempts=2, slots=1,
                      max_runtime_seconds=40)

    executor = SGEExecutor(project='proj_tools')
    real_dag = dag_factory(executor)
    real_dag.add_task(task)
    (rc, num_completed, num_previously_complete, num_failed) = (
        real_dag._execute())

    ret_vals = get_task_status(real_dag, task)

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        job = DB.session.query(Job).filter_by(name=name).first()
        jid = [ji for ji in job.job_instances][0].executor_id
        resp = check_output(f"qacct -j {jid} | grep exit_status", shell=True,
                            universal_newlines=True)
        assert '9' in resp
        assert job.job_instances[0].status == 'E'
        assert job.status == 'F'


    sge_jobname = match_name_to_sge_name(jid)
    assert sge_jobname == name



@pytest.mark.qsubs_jobs
def test_R_task(db_cfg, dag_factory, tmp_out_dir):
    """Execute an RTask"""
    name = 'r_task'

    root_out_dir = "{t}/mocks/{n}".format(t=tmp_out_dir, n=name)
    makedirs_safely(root_out_dir)

    task = RTask(script=sge.true_path("tests/simple_R_script.r"), name=name,
                 mem_free='1G', max_attempts=2, max_runtime_seconds=60,
                 slots=1)
    executor = SGEExecutor(project='proj_tools')
    real_dag = dag_factory(executor)
    real_dag.add_task(task)
    (rc, num_completed, num_previously_complete, num_failed) = (
        real_dag._execute())

    assert rc == DagExecutionStatus.SUCCEEDED
    assert num_completed == 1
    assert get_task_status(real_dag, task) == JobStatus.DONE

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        job = DB.session.query(Job).filter_by(name=name).first()
        jid = [ji for ji in job.job_instances][0].executor_id
        assert job.mem_free == '1G'
        assert job.max_attempts == 2
        assert job.max_runtime_seconds == 60

    sge_jobname = match_name_to_sge_name(jid)
    assert sge_jobname == name


@pytest.mark.qsubs_jobs
def test_stata_task(db_cfg, dag_factory, tmp_out_dir):
    """Execute a simple stata Task"""
    name = 'stata_task'
    root_out_dir = "{t}/mocks/{n}".format(t=tmp_out_dir, n=name)
    makedirs_safely(root_out_dir)

    task = StataTask(script=sge.true_path("tests/simple_stata_script.do"),
                     name=name, mem_free='1G', max_attempts=2,
                     max_runtime_seconds=60, slots=1)
    executor = SGEExecutor(project='proj_tools')
    executor.set_temp_dir(root_out_dir)
    dag = dag_factory(executor)
    dag.add_task(task)
    rc, num_completed, num_previously_complete, num_failed = dag._execute()

    assert rc == DagExecutionStatus.SUCCEEDED
    assert num_completed == 1
    assert get_task_status(dag, task) == JobStatus.DONE

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        job = DB.session.query(Job).filter_by(name=name).first()
        sge_id = [ji for ji in job.job_instances][0].executor_id
        job_instance_id = [ji for ji in job.job_instances][0].job_instance_id
        assert job.mem_free == '1G'
        assert job.max_attempts == 2
        assert job.max_runtime_seconds == 60

    sge_jobname = match_name_to_sge_name(sge_id)
    assert sge_jobname == name
    assert os.path.exists(os.path.join(
        root_out_dir,
        "{jid}-simple_stata_script.do".format(jid=job_instance_id)))
    assert os.path.exists(os.path.join(
        root_out_dir,
        "{jid}-simple_stata_script.log".format(jid=job_instance_id)))


@pytest.mark.skip("Need to use a specific queue for fair cluster so these "
                  "tests would only work on the prod cluster")
@pytest.mark.qsubs_jobs
def test_specific_queue(db_cfg, dag_factory, tmp_out_dir):
    name = 'c2_nodes_only'
    root_out_dir = "{t}/mocks/{n}".format(t=tmp_out_dir, n=name)
    makedirs_safely(root_out_dir)

    output_file_name = "{t}/mocks/{n}/mock.out".format(t=tmp_out_dir, n=name)

    task = PythonTask(script=sge.true_path("tests/remote_sleep_and_write.py"),
                      args=["--sleep_secs", "1",
                            "--output_file_path", output_file_name,
                            "--name", name],
                      name=name, mem_free='1G', max_attempts=2,
                      max_runtime_seconds=60, queue='all.q@@c2-nodes')
    executor = SGEExecutor(project='proj_tools')
    dag = dag_factory(executor)
    dag.add_task(task)
    (rc, num_completed, num_previously_complete, num_failed) = (dag._execute())

    assert rc == DagExecutionStatus.SUCCEEDED
    assert num_completed == 1
    assert get_task_status(dag, task) == JobStatus.DONE

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        job = DB.session.query(Job).filter_by(name=name).first()
        assert job.queue == 'all.q@@c2-nodes'
        jids = [ji.nodename for ji in job.job_instances]
        assert all(['c2' in nodename for nodename in jids])

