import pytest

from jobmon.workflow.abstract_task import AbstractTask
from jobmon.workflow.executable_task import ExecutableTask
from jobmon.workflow.bash_task import BashTask


def test_good_names():
    assert AbstractTask.is_valid_sge_job_name("fred")
    assert AbstractTask.is_valid_sge_job_name("fred123")
    assert AbstractTask.is_valid_sge_job_name("fred_and-friends")


def test_bad_names():
    with pytest.raises(ValueError) as exc:
        AbstractTask.is_valid_sge_job_name("")
    assert "None" in str(exc.value)

    with pytest.raises(ValueError) as exc:
        AbstractTask.is_valid_sge_job_name("16")
    assert "digit" in str(exc.value)

    with pytest.raises(ValueError) as exc:
        AbstractTask.is_valid_sge_job_name("bad/dog")
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
