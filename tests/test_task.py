import pytest
import sys

from jobmon.workflow.executable_task import ExecutableTask
from jobmon.workflow.bash_task import BashTask
from jobmon.workflow.python_task import PythonTask
from jobmon.database import session_scope
from jobmon.models import Job


def test_good_names():
    assert ExecutableTask.is_valid_sge_job_name("fred")
    assert ExecutableTask.is_valid_sge_job_name("fred123")
    assert ExecutableTask.is_valid_sge_job_name("fred_and-friends")


def test_bad_names():
    with pytest.raises(ValueError) as exc:
        ExecutableTask.is_valid_sge_job_name("")
    assert "None" in str(exc.value)

    with pytest.raises(ValueError) as exc:
        ExecutableTask.is_valid_sge_job_name("16")
    assert "digit" in str(exc.value)

    with pytest.raises(ValueError) as exc:
        ExecutableTask.is_valid_sge_job_name("bad/dog")
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
    assert a.is_valid_sge_job_name(a.name)


def test_bash_task_args(job_list_manager_sge):
    a = BashTask(command="echo 'Hello Jobmon'", slots=1, mem_free=2,
                 max_attempts=1)
    job_id = a.bind(job_list_manager_sge)

    with session_scope() as session:
        job = session.query(Job).filter_by(job_id=job_id).all()
        slots = job[0].slots
        mem_free = job[0].mem_free
        max_attempts = job[0].max_attempts
        max_runtime = job[0].max_runtime
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
    a = PythonTask(script='~/runme.py', slots=1, mem_free=2, max_attempts=1)
    job_id = a.bind(job_list_manager_sge)

    with session_scope() as session:
        job = session.query(Job).filter_by(job_id=job_id).all()
        command = job[0].command
        slots = job[0].slots
        mem_free = job[0].mem_free
        max_attempts = job[0].max_attempts
        max_runtime = job[0].max_runtime
    # check all job args
    assert command == '{} ~/runme.py'.format(sys.executable)
    assert slots == 1
    assert mem_free == 2
    assert max_attempts == 1
    assert not max_runtime
