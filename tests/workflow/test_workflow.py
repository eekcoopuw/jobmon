import pytest

from jobmon.exceptions import TaskDependencyNotExistError


def test_add_tasks_dependencynotexist(client_env):

    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.api import BashTask

    t1 = BashTask("echo 1", executor_class="SequentialExecutor")
    t2 = BashTask("echo 2", executor_class="SequentialExecutor")
    t3 = BashTask("echo 3", executor_class="SequentialExecutor")
    t3.add_upstream(t2)
    with pytest.raises(TaskDependencyNotExistError) as excinfo:
        wf = UnknownWorkflow("wf1", name="TestWF1")
        wf.add_tasks([t1, t2])
    assert "Downstream" in str(excinfo.value)
    with pytest.raises(TaskDependencyNotExistError) as excinfo:
        wf = UnknownWorkflow("wf2", name="TestWF2")
        wf.add_tasks([t1, t3])
    assert "Upstream" in str(excinfo.value)
    wf = UnknownWorkflow("wf3", name="TestWF3")
    wf.add_tasks([t1, t2, t3])
    assert len(wf.tasks) == 3
