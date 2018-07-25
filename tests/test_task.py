import pytest
import sys

from jobmon import sge
from jobmon.workflow.executable_task import ExecutableTask
from jobmon.workflow.bash_task import BashTask
from jobmon.workflow.python_task import PythonTask
from jobmon.workflow.r_task import RTask
from jobmon.workflow.stata_task import StataTask
from jobmon.models import Job


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


def test_bash_task_args(job_list_manager_sge):
    from jobmon.database import ScopedSession
    a = BashTask(command="echo 'Hello Jobmon'", slots=1, mem_free=2,
                 max_attempts=1)
    job_id = job_list_manager_sge.bind_task(a).job_id

    job = ScopedSession.query(Job).filter_by(job_id=job_id).all()
    slots = job[0].slots
    mem_free = job[0].mem_free
    max_attempts = job[0].max_attempts
    max_runtime = job[0].max_runtime
    ScopedSession.commit()
    # check all job args
    assert slots == 1
    assert mem_free == 2
    assert max_attempts == 1
    assert not max_runtime


def test_python_task_equality():
    a = PythonTask(script='~/runme.py', args=[1])
    a_again = PythonTask(script='~/runme.py', args=[1])
    assert a == a_again

    b = PythonTask(script='~/runme.py', args=[2], upstream_tasks=[a, a_again])
    assert b != a
    assert len(b.upstream_tasks) == 1


def test_python_task_args(job_list_manager_sge):
    from jobmon.database import ScopedSession
    a = PythonTask(script='~/runme.py', env_variables={'OP_NUM_THREADS': 1},
                   slots=1, mem_free=2, max_attempts=1)
    job_id = job_list_manager_sge.bind_task(a).job_id

    job = ScopedSession.query(Job).filter_by(job_id=job_id).all()
    command = job[0].command
    slots = job[0].slots
    mem_free = job[0].mem_free
    max_attempts = job[0].max_attempts
    max_runtime = job[0].max_runtime
    ScopedSession.commit()
    # check all job args
    assert command == 'OP_NUM_THREADS=1 {} ~/runme.py'.format(sys.executable)
    assert slots == 1
    assert mem_free == 2
    assert max_attempts == 1
    assert not max_runtime


def test_r_task_args(job_list_manager_sge):
    from jobmon.database import ScopedSession
    a = RTask(script=sge.true_path("tests/simple_R_script.r"),
              env_variables={'OP_NUM_THREADS': 1},
              slots=1, mem_free=2, max_attempts=1)
    job_id = job_list_manager_sge.bind_task(a).job_id

    job = ScopedSession.query(Job).filter_by(job_id=job_id).all()
    command = job[0].command
    slots = job[0].slots
    mem_free = job[0].mem_free
    max_attempts = job[0].max_attempts
    max_runtime = job[0].max_runtime
    ScopedSession.commit()
    # check all job args
    assert command == ('OP_NUM_THREADS=1 Rscript {}'
                       .format(sge.true_path("tests/simple_R_script.r")))
    assert slots == 1
    assert mem_free == 2
    assert max_attempts == 1
    assert not max_runtime


def test_stata_task_args(job_list_manager_sge):
    from jobmon.database import ScopedSession
    a = StataTask(script=sge.true_path("tests/simple_stata_script.do"),
                  env_variables={'OP_NUM_THREADS': 1},
                  slots=1, mem_free=2, max_attempts=1)
    job_id = job_list_manager_sge.bind_task(a).job_id

    job = ScopedSession.query(Job).filter_by(job_id=job_id).all()
    command = job[0].command
    slots = job[0].slots
    mem_free = job[0].mem_free
    max_attempts = job[0].max_attempts
    max_runtime = job[0].max_runtime
    ScopedSession.commit()
    # check all job args
    assert command == ('OP_NUM_THREADS=1 stata-mp -q -b {}'
                       .format(sge.true_path("tests/simple_stata_script.do")))
    assert slots == 1
    assert mem_free == 2
    assert max_attempts == 1
    assert not max_runtime
