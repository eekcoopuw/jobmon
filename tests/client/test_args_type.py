import pytest
import os
import numpy as np

from jobmon.constants import WorkflowRunStatus


@pytest.fixture
def tool(client_env):
    from jobmon.client.api import Tool

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
def test_node_args(tool, test_script, input, expect):
    tt = tool.get_task_template(
        template_name="arg_template",
        command_template="python {script} {arg1} {arg2} {arg3}",
        node_args=["script", "arg1", "arg2", "arg3"],
        task_args=[],
        op_args=[],
    )
    arg3_val = "A" * 2048
    workflow1 = tool.create_workflow(name="node_arg_test")
    t1 = tt.create_task(
        name="a_task",
        max_attempts=1,
        script=test_script,
        arg1=input,
        arg2=expect,
        arg3=arg3_val,
    )
    workflow1.add_tasks([t1])
    workflow1.bind()
    workflow1._bind_tasks()


@pytest.mark.parametrize(
    "input, expect",
    [
        ("1", "1"),  # str
        (1, "1"),  # int
        (1.1, "1.1"),  # float
        (np.int_(1), "1"),  # numpy int
    ],
)
def test_task_args(tool, test_script, input, expect):
    tt = tool.get_task_template(
        template_name="arg_template",
        command_template="python {script} {arg1} {arg2} {arg3}",
        node_args=[],
        task_args=["script", "arg1", "arg2", "arg3"],
        op_args=[],
    )
    arg3_val = "A" * 2048
    workflow1 = tool.create_workflow(name="node_arg_test")
    t1 = tt.create_task(
        name="a_task",
        max_attempts=1,
        script=test_script,
        arg1=input,
        arg2=expect,
        arg3=arg3_val,
    )
    workflow1.add_tasks([t1])
    workflow1.bind()
    workflow1._bind_tasks()


@pytest.mark.parametrize(
    "input, expect",
    [
        ("1", "1"),  # str
        (1, "1"),  # int
        (1.1, "1.1"),  # float
        (np.int_(1), "1"),  # numpy int
    ],
)
def test_op_args(tool, test_script, input, expect):
    tt = tool.get_task_template(
        template_name="arg_template",
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
    workflow1._bind_tasks()
