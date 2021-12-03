import pytest
import os
import numpy as np

from jobmon.client.api import Tool
from jobmon.client.workflow_run import WorkflowRun
from jobmon.constants import WorkflowRunStatus, WorkflowStatus


@pytest.fixture
def tool(db_cfg, client_env):
    tool = Tool()
    tool.set_default_compute_resources_from_dict(
        cluster_name="sequential", compute_resources={"queue": "null.q"}
    )
    return tool


@pytest.fixture
def test_script():
    this_file = os.path.dirname(__file__)
    test_script = os.path.abspath(
        os.path.expanduser(f"{this_file}/../_scripts/node_arg_type_check.py")
    )
    return test_script


@pytest.mark.parametrize(
    "input, expect",
    [
        ("1", "1"),  # str
        (1, "1"),  # int
        (1.1, "1.1"),  # float
        (np.int_(1), "1"),  # numpy int
    ],
)
def test_node_args(db_cfg, tool, test_script, input, expect):
    tt = tool.get_task_template(
        template_name="simple_template",
        command_template="python {script} {arg1} {arg2}",
        node_args=["script", "arg1", "arg2"],
        task_args=[],
        op_args=[],
    )
    workflow1 = tool.create_workflow(name="node_arg_test")
    t1 = tt.create_task(
        name="a_task", max_attempts=1, script=test_script, arg1=input, arg2=expect
    )
    workflow1.add_tasks([t1])
    workflow1.bind()
    wfr = workflow1._create_workflow_run()
    assert wfr.status == WorkflowRunStatus.BOUND


@pytest.mark.parametrize(
    "input, expect",
    [
        ("1", "1"),  # str
        (1, "1"),  # int
        (1.1, "1.1"),  # float
        (np.int_(1), "1"),  # numpy int
    ],
)
def test_task_args(db_cfg, tool, test_script, input, expect):
    tt = tool.get_task_template(
        template_name="simple_template",
        command_template="python {script} {arg1} {arg2}",
        node_args=[],
        task_args=["script", "arg1", "arg2"],
        op_args=[],
    )
    workflow1 = tool.create_workflow(name="node_arg_test")
    t1 = tt.create_task(
        name="a_task", max_attempts=1, script=test_script, arg1=input, arg2=expect
    )
    workflow1.add_tasks([t1])
    workflow1.bind()
    wfr = workflow1._create_workflow_run()
    assert wfr.status == WorkflowRunStatus.BOUND


@pytest.mark.parametrize(
    "input, expect",
    [
        ("1", "1"),  # str
        (1, "1"),  # int
        (1.1, "1.1"),  # float
        (np.int_(1), "1"),  # numpy int
    ],
)
def test_op_args(db_cfg, tool, test_script, input, expect):
    tt = tool.get_task_template(
        template_name="simple_template",
        command_template="python {script} {arg1} {arg2}",
        node_args=[],
        task_args=[],
        op_args=["script", "arg1", "arg2"],
    )
    workflow1 = tool.create_workflow(name="node_arg_test")
    t1 = tt.create_task(
        name="a_task", max_attempts=1, script=test_script, arg1=input, arg2=expect
    )
    workflow1.add_tasks([t1])
    workflow1.bind()
    wfr = workflow1._create_workflow_run()
    assert wfr.status == WorkflowRunStatus.BOUND
