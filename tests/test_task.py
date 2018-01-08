import pytest
import sys

from jobmon.workflow.executable_task import ExecutableTask
from jobmon.workflow.bash_task import BashTask
from jobmon.database import session_scope
from jobmon.models import Job


if sys.version_info < (3, 0):
    from functools32 import partial
else:
    from functools import partial


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
    def __init__(self, hash_name, upstream_tasks=[]):
        ExecutableTask.__init__(self, hash_name, upstream_tasks)


def test_equality():
    a = TheTestTask("a")
    a_again = TheTestTask("a")
    assert a == a_again

    b = TheTestTask("b", upstream_tasks=[a, a_again])
    assert b != a
    assert len(b.upstream_tasks) == 1


def test_bash_task_equality():
    a = BashTask(command="echo 'Hello World'")
    a_again = BashTask(command="echo 'Hello World'")
    assert a == a_again

    b = BashTask(command="echo 'Hello Jobmon'", upstream_tasks=[a, a_again])
    assert b != a
    assert len(b.upstream_tasks) == 1


def test_hashing_bash_characters():
    a = BashTask(command="touch ~/mytestfile")
    assert a.is_valid_sge_job_name(a.hash_name)


def test_bash_task_args(job_list_manager_sge):
    a = BashTask(command="echo 'Hello Jobmon'", slots=1, mem_free=2,
                 project='proj_jenkins', max_attempts=1)
    job_id = a.bind(job_list_manager_sge)

    with session_scope() as session:
        job = session.query(Job).filter_by(job_id=job_id).all()
        slots = job[0].slots
        mem_free = job[0].mem_free
        project = job[0].project
        max_attempts = job[0].max_attempts
        max_runtime = job[0].max_runtime
        stderr = job[0].stderr
        stdout = job[0].stdout
    # check all job args
    assert slots == 1
    assert mem_free == 2
    assert project == 'proj_jenkins'
    assert max_attempts == 1
    assert not max_runtime
    assert not stderr
    assert not stdout
