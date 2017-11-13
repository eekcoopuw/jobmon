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


class TheBashTask(BashTask):
    def __init(self, hash_name, args=(), upstream_tasks=[]):
        BashTask.__init__(self, hash_name, args, upstream_tasks)


def test_bash_task_equality():
    a = TheBashTask("touch ~/mytestfile")
    a_again = TheBashTask("touch ~/mytestfile")
    assert a == a_again

    b = TheBashTask("ls ~", upstream_tasks=[a, a_again])
    assert b != a
    assert len(b.upstream_tasks) == 1


def test_bash_task_args_equality():
    a = TheBashTask("echo $1", args=("hello jobmon",))
    a_again = TheBashTask("echo $1", args=("hello jobmon",))
    assert a == a_again

    # BashTask will need to parse args into the hashname
    a_but_diff_args = TheBashTask("echo $1", args=("hello world",))
    assert a != a_but_diff_args

    b = TheBashTask("echo $1 $2", args=("hello jobmon", "and hello world"),
                    upstream_tasks=[a, a_but_diff_args])
    assert b != a
    assert b != a_but_diff_args
    assert len(b.upstream_tasks) == 2





