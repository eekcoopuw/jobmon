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
    def __init(self, hash_name, command, args=(), upstream_tasks=[]):
        BashTask.__init__(self, hash_name, command, args, upstream_tasks)


def test_bash_task_equality():
    a = TheBashTask(hash_name='a', command="touch ~/mytestfile")
    a_again = TheBashTask(hash_name='a', command="touch ~/mytestfile")
    assert a == a_again

    b = TheBashTask(hash_name='b', command="ls ~", upstream_tasks=[a, a_again])
    assert b != a
    assert len(b.upstream_tasks) == 1


def test_bash_task_equality_using_diff_args():
    # irrespective of args, tasks should be equal if they have same hash_name
    # See NOTE in next test.
    a = TheBashTask(hash_name='a', command="echo $1", args=("hello jobmon",))
    a_diff_args = TheBashTask(hash_name='a', command="echo $1",
                              args=("hello world",))
    assert a == a_diff_args

    b = TheBashTask(hash_name='b', command="echo $1 $2",
                    args=("hello jobmon", "and hello world"),
                    upstream_tasks=[a, a_diff_args])
    assert b != a
    assert b != a_diff_args
    assert len(b.upstream_tasks) == 1


def test_bash_task_equality_using_diff_commands():
    # irrespective of commands, tasks should be equal if they have same
    # hash_name
    # NOTE: in the future, we could intelligently create hash_name from command
    # + args, if the hash_name were to default to None and isn't passed in.
    # Either that or raise an error in task_dag.py if a repeat hash_name is
    # added?
    a = TheBashTask(hash_name='a', command="echo 'hi'")
    a_diff_comand = TheBashTask(hash_name='a', command="echo 'bye'")
    assert a == a_diff_comand

    b = TheBashTask(hash_name='b', command="echo $1 $2",
                    args=("hello jobmon", "and hello world"),
                    upstream_tasks=[a, a_diff_comand])
    assert b != a
    assert b != a_diff_comand
    assert len(b.upstream_tasks) == 1

