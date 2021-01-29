import pytest

from jobmon.exceptions import NodeDependencyNotExistError


def test_add_tasks_dependencynotexist(db_cfg, client_env):

    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.api import BashTask

    t1 = BashTask("echo 1", executor_class="SequentialExecutor")
    t2 = BashTask("echo 2", executor_class="SequentialExecutor")
    t3 = BashTask("echo 3", executor_class="SequentialExecutor")
    t3.add_upstream(t2)
    with pytest.raises(NodeDependencyNotExistError) as excinfo:
        wf = UnknownWorkflow("wf1", name="TestWF1")
        wf.add_tasks([t1, t2])
        wf._bind()
    assert "Downstream" in str(excinfo.value)
    with pytest.raises(NodeDependencyNotExistError) as excinfo:
        wf = UnknownWorkflow("wf2", name="TestWF2")
        wf.add_tasks([t1, t3])
        wf._bind()
    assert "Upstream" in str(excinfo.value)
    wf = UnknownWorkflow("wf3", name="TestWF3")
    wf.add_tasks([t1, t2, t3])
    wf.run()
    assert len(wf.tasks) == 3
    wf = UnknownWorkflow("wf4", name="TestWF4")
    wf.add_tasks([t1])
    wf.add_tasks([t2])
    wf.add_tasks([t3])
    wf._bind()
    assert len(wf.tasks) == 3
