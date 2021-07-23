from jobmon.exceptions import NodeDependencyNotExistError

import pytest

from jobmon.client.task import Task
from jobmon.client.tool import Tool


@pytest.fixture
def tool(db_cfg, client_env):
    tool = Tool()
    tool.set_default_compute_resources_from_dict(cluster_name="sequential",
                                                 compute_resources={"queue": "null.q"})
    return tool


@pytest.fixture
def task_template(tool):
    tt = tool.get_task_template(
        template_name="my_template",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[]
    )
    return tt


def test_add_tasks_dependencynotexist(db_cfg, client_env, task_template):

    tool = Tool()
    t1 = task_template.create_task(
         arg="echo 1")
    t2 = task_template.create_task(
         arg="echo 2")
    t3 = task_template.create_task(
         arg="echo 3")
    t3.add_upstream(t2)
    with pytest.raises(NodeDependencyNotExistError) as excinfo:
        wf = tool.create_workflow(name="TestWF1",
                                  default_cluster_name = "sequential",
                                  default_compute_resources_set = {"sequential": {"queue": "null.q"}})
        wf.add_tasks([t1, t2])
        wf.bind()
    assert "Downstream" in str(excinfo.value)
    with pytest.raises(NodeDependencyNotExistError) as excinfo:
        wf = tool.create_workflow(name="TestWF2",
                                  default_cluster_name = "sequential",
                                  default_compute_resources_set = {"sequential": {"queue": "null.q"}})
        wf.add_tasks([t1, t3])
        wf.bind()
    assert "Upstream" in str(excinfo.value)
    wf = tool.create_workflow(name="TestWF3",
                                  default_cluster_name = "sequential",
                                  default_compute_resources_set = {"sequential": {"queue": "null.q"}})
    wf.add_tasks([t1, t2, t3])
    wf.bind()
    wf.run()
    assert len(wf.tasks) == 3
    wf = tool.create_workflow(name="TestWF4",
                                  default_cluster_name = "sequential",
                                  default_compute_resources_set = {"sequential": {"queue": "null.q"}})
    wf.add_tasks([t1])
    wf.add_tasks([t2])
    wf.add_tasks([t3])
    wf.bind()
    assert len(wf.tasks) == 3
