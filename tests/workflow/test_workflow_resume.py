import os
import sys

import pytest

from jobmon.exceptions import WorkflowAlreadyExists
from jobmon.models.workflow_run_status import WorkflowRunStatus
from jobmon.models.task_status import TaskStatus

this_file = os.path.dirname(__file__)
remote_sleep_and_write = os.path.abspath(os.path.expanduser(
    f"{this_file}/../_scripts/remote_sleep_and_write.py"))


def test_fail_one_task_resume(db_cfg, client_env, tmpdir):
    from jobmon.client.api import Tool, ExecutorParameters
    from jobmon.client.execution.strategies.sequential import \
        SequentialExecutor

    unknown_tool = Tool()
    # set fail always as op args so it can be modified on resume without
    # changing the workflow hash
    tt = unknown_tool.get_task_template(
        template_name="foo",
        command_template=(
            "{python} "
            "{script} "
            "--sleep_secs {sleep_secs} "
            "--output_file_path {output_file_path} "
            "--name {name} "
            "{fail_always}"),
        node_args=["name"],
        task_args=["sleep_secs", "output_file_path"],
        op_args=["python", "script", "fail_always"])

    # create workflow and execute
    workflow1 = unknown_tool.create_workflow(name="fail_one_task_resume")
    workflow1.set_executor(SequentialExecutor())
    t1 = tt.create_task(
        executor_parameters=ExecutorParameters(
            executor_class="SequentialExecutor"),
        name="a_task",
        max_attempts=1,
        python=sys.executable,
        script=remote_sleep_and_write,
        sleep_secs=3,
        output_file_path=os.path.join(str(tmpdir), "a.out"),
        fail_always="--fail_always")  # fail bool is set
    workflow1.add_tasks([t1])
    wfr1 = workflow1.run()

    assert wfr1.status == WorkflowRunStatus.ERROR
    assert len(wfr1.all_error) == 1

    # set workflow args and name to be identical to previous workflow
    workflow2 = unknown_tool.create_workflow(
        name=workflow1.name, workflow_args=workflow1.workflow_args)
    workflow2.set_executor(SequentialExecutor())
    t2 = tt.create_task(
        executor_parameters=ExecutorParameters(
            executor_class="SequentialExecutor"),
        name="a_task",
        max_attempts=1,
        python=sys.executable,
        script=remote_sleep_and_write,
        sleep_secs=3,
        output_file_path=os.path.join(str(tmpdir), "a.out"),
        fail_always="")   # fail bool is not set. workflow should succeed
    workflow2.add_tasks([t2])

    with pytest.raises(WorkflowAlreadyExists):
        workflow2.run()

    wfr2 = workflow2.run(resume=True)

    assert wfr2.status == WorkflowRunStatus.DONE
    assert workflow1.workflow_id == workflow2.workflow_id
    assert wfr2.workflow_run_id != wfr1.workflow_run_id
