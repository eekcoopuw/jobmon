import os
import pytest
import sys

from jobmon.client.swarm.executors import sge_utils as sge
from jobmon.client.swarm.workflow.executable_task import ExecutableTask
from jobmon.client.swarm.workflow.bash_task import BashTask
from jobmon.client.swarm.workflow.python_task import PythonTask
from jobmon.client.swarm.workflow.r_task import RTask
from jobmon.client.swarm.workflow.stata_task import StataTask
from jobmon.models.job import Job

path_to_file = os.path.dirname(__file__)


def test_good_names():
    assert ExecutableTask.is_valid_job_name("fred")
    assert ExecutableTask.is_valid_job_name("fred123")
    assert ExecutableTask.is_valid_job_name("fred_and-friends")


def test_bad_names():
    with pytest.raises(ValueError) as exc:
        ExecutableTask.is_valid_job_name("")
    assert "None" in str(exc.value)

    with pytest.raises(ValueError) as exc:
        ExecutableTask.is_valid_job_name("16")
    assert "digit" in str(exc.value)

    with pytest.raises(ValueError) as exc:
        ExecutableTask.is_valid_job_name("bad/dog")
    assert "special" in str(exc.value)


class TheTestTask(ExecutableTask):
    """Test version of a Task for use in this module"""

    def __init__(self, command, upstream_tasks=[]):
        ExecutableTask.__init__(self, command, upstream_tasks)


def test_equality():
    a = TheTestTask("a")
    a_again = TheTestTask("a")
    assert a == a_again

    b = TheTestTask("b", upstream_tasks=[a, a_again])
    assert b != a
    assert len(b.upstream_tasks) == 1


def test_hash_name_compatibility():
    a = TheTestTask("a")
    assert a.hash_name == a.name


def test_bash_task_equality():
    a = BashTask(command="echo 'Hello World'")
    a_again = BashTask(command="echo 'Hello World'")
    assert a == a_again

    b = BashTask(command="echo 'Hello Jobmon'", upstream_tasks=[a, a_again])
    assert b != a
    assert len(b.upstream_tasks) == 1


def test_hashing_bash_characters():
    a = BashTask(command="touch ~/mytestfile")
    assert a.is_valid_job_name(a.name)


def test_bash_task_args(db_cfg, job_list_manager_sge):
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    a = BashTask(command="echo 'Hello Jobmon'", num_cores=1, mem_free='2G',
                 max_attempts=1)
    job_id = job_list_manager_sge.bind_task(a).job_id

    with app.app_context():
        job = DB.session.query(Job).filter_by(job_id=job_id).all()
        num_cores = job[0].executor_parameter_set.num_cores
        m_mem_free = job[0].executor_parameter_set.m_mem_free
        max_attempts = job[0].max_attempts
        DB.session.commit()
        # check all job args
        assert num_cores == 1
        assert m_mem_free == 2
        assert max_attempts == 1


def test_python_task_equality():
    a = PythonTask(script='~/runme.py', args=[1])
    a_again = PythonTask(script='~/runme.py', args=[1])
    assert a == a_again

    b = PythonTask(script='~/runme.py', args=[2], upstream_tasks=[a, a_again])
    assert b != a
    assert len(b.upstream_tasks) == 1


def test_python_task_args(db_cfg, job_list_manager_sge):
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    a = PythonTask(script='~/runme.py', env_variables={'OP_NUM_THREADS': 1},
                   num_cores=1, m_mem_free='2G', max_attempts=1)
    job_id = job_list_manager_sge.bind_task(a).job_id

    with app.app_context():
        job = DB.session.query(Job).filter_by(job_id=job_id).all()
        command = job[0].command
        num_cores = job[0].executor_parameter_set.num_cores
        m_mem_free = job[0].executor_parameter_set.m_mem_free
        max_attempts = job[0].max_attempts
        DB.session.commit()
        # check all job args
        assert command == 'OP_NUM_THREADS=1 {} ~/runme.py'.format(
            sys.executable)
        assert num_cores == 1
        assert m_mem_free == 2
        assert max_attempts == 1


def test_r_task_args(db_cfg, job_list_manager_sge):
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    a = RTask(script=sge.true_path(f"{path_to_file}/simple_R_script.r"),
              env_variables={'OP_NUM_THREADS': 1},
              num_cores=1, mem_free='2G', max_attempts=1)
    job_id = job_list_manager_sge.bind_task(a).job_id

    with app.app_context():
        job = DB.session.query(Job).filter_by(job_id=job_id).all()
        command = job[0].command
        num_cores = job[0].executor_parameter_set.num_cores
        m_mem_free = job[0].executor_parameter_set.m_mem_free
        max_attempts = job[0].max_attempts
        DB.session.commit()
        # check all job args
        assert command == ('OP_NUM_THREADS=1 Rscript {}'
                           .format(sge.true_path(f"{path_to_file}/simple_R_script.r")))
        assert num_cores == 1
        assert m_mem_free == 2
        assert max_attempts == 1


def test_stata_task_args(db_cfg, job_list_manager_sge):
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    a = StataTask(script=sge.true_path(f"{path_to_file}/simple_stata_script.do"),
                  env_variables={'OP_NUM_THREADS': 1},
                  num_cores=1, m_mem_free='2G', max_attempts=1)
    job_id = job_list_manager_sge.bind_task(a).job_id

    with app.app_context():
        job = DB.session.query(Job).filter_by(job_id=job_id).all()
        command = job[0].command
        num_cores = job[0].executor_parameter_set.num_cores
        m_mem_free = job[0].executor_parameter_set.m_mem_free
        max_attempts = job[0].max_attempts
        DB.session.commit()
        # check all job args
        assert command.startswith('OP_NUM_THREADS=1 ')
        assert num_cores == 1
        assert m_mem_free == 2
        assert max_attempts == 1
