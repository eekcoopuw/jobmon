import getpass

import pytest

from jobmon.client.cli import ClientCLI as CLI


class MockSchedulerProc:

    def is_alive(self):
        return True


def mock_getuser():
    return "foo"


def test_workflow_status(db_cfg, client_env, monkeypatch):
    from jobmon.client.api import BashTask
    from jobmon.client.api import UnknownWorkflow
    from jobmon.client.status_commands import workflow_status

    monkeypatch.setattr(getpass, "getuser", mock_getuser)
    user = getpass.getuser()
    workflow = UnknownWorkflow(executor_class="SequentialExecutor")
    t1 = BashTask("sleep 10", executor_class="SequentialExecutor")
    t2 = BashTask("sleep 5", upstream_tasks=[t1],
                  executor_class="SequentialExecutor")
    workflow.add_tasks([t1, t2])
    workflow._bind()
    workflow._create_workflow_run()

    # we should have the column headers plus 2 tasks in pending
    command_str = f"workflow_status -u {user} -w {workflow.workflow_id}"
    cli = CLI()
    args = cli.parse_args(command_str)
    df = workflow_status(args.workflow_id, args.user)
    assert df["PENDING"][0] == "2 (100.0%)"

    # defaults should return an identical value
    command_str = "workflow_status"
    cli = CLI()
    args = cli.parse_args(command_str)
    df = workflow_status(args.workflow_id, args.user)
    assert df["PENDING"][0] == "2 (100.0%)"

    # Test the JSON flag
    command_str = f"workflow_status -u {user} -w {workflow.workflow_id} -n"
    cli = CLI()
    args = cli.parse_args(command_str)
    df = workflow_status(args.workflow_id, args.user, args.json)
    assert df == f'{{"WF_ID":{{"0":{workflow.workflow_id}}},"WF_NAME":{{"0":""}},"WF_STATUS":{{"0":' \
                 '"BOUND"},"TASKS":{"0":2},"PENDING":{"0":"2 (100.0%)"},' \
                 '"RUNNING":{"0":"0 (0.0%)"},"DONE":{"0":"0 (0.0%)"},"FATAL"' \
                 ':{"0":"0 (0.0%)"},"RETRIES":{"0":0.0}}'

    # add a second workflow
    t1 = BashTask("sleep 15", executor_class="SequentialExecutor")
    t2 = BashTask("sleep 1", upstream_tasks=[t1],
                  executor_class="SequentialExecutor")
    workflow = UnknownWorkflow(executor_class="SequentialExecutor")
    workflow.add_tasks([t1, t2])
    workflow._bind()
    workflow._create_workflow_run()

    # check that we get 2 rows now
    command_str = f"workflow_status -u {user}"
    cli = CLI()
    args = cli.parse_args(command_str)
    df = workflow_status(args.workflow_id, args.user)
    assert len(df) == 2

    # check that we can get values by workflow_id
    command_str = f"workflow_status -w {workflow.workflow_id}"
    cli = CLI()
    args = cli.parse_args(command_str)
    df = workflow_status(args.workflow_id, args.user)
    assert len(df) == 1
    assert df["WF_ID"][0] == workflow.workflow_id

    # check that we can get both
    command_str = "workflow_status -w 1 2"
    cli = CLI()
    args = cli.parse_args(command_str)
    df = workflow_status(args.workflow_id, args.user)
    assert len(df) == 2


def test_workflow_tasks(db_cfg, client_env):
    from jobmon.client.api import BashTask
    from jobmon.client.api import UnknownWorkflow
    from jobmon.client.status_commands import workflow_tasks
    from jobmon.client.execution.scheduler.task_instance_scheduler import \
        TaskInstanceScheduler
    workflow = UnknownWorkflow(executor_class="SequentialExecutor")
    t1 = BashTask("sleep 3", executor_class="SequentialExecutor",
                  max_runtime_seconds=10, resource_scales={})
    t2 = BashTask("sleep 4", executor_class="SequentialExecutor",
                  max_runtime_seconds=10, resource_scales={})

    workflow.add_tasks([t1, t2])
    workflow._bind()
    wfr = workflow._create_workflow_run()

    # we should get 2 tasks back in pending state
    command_str = f"workflow_tasks -w {workflow.workflow_id}"
    cli = CLI()
    args = cli.parse_args(command_str)
    df = workflow_tasks(args.workflow_id)
    assert len(df) == 2
    assert df.STATUS[0] == "PENDING"
    assert len(df.STATUS.unique()) == 1

    # execute the tasks
    scheduler = TaskInstanceScheduler(workflow.workflow_id, wfr.workflow_run_id,
                                      workflow._executor, requester_url=client_env)
    with pytest.raises(RuntimeError):
        wfr.execute_interruptible(MockSchedulerProc(),
                                  seconds_until_timeout=1)

    scheduler._get_tasks_queued_for_instantiation()
    scheduler.schedule()

    # we should get 0 tasks in pending
    command_str = f"workflow_tasks -w {workflow.workflow_id} -s PENDING"
    cli = CLI()
    args = cli.parse_args(command_str)
    df = workflow_tasks(args.workflow_id, args.status)
    assert len(df) == 0

    # we should get 0 tasks when requesting workflow -99
    command_str = "workflow_tasks -w -99"
    cli = CLI()
    args = cli.parse_args(command_str)
    df = workflow_tasks(args.workflow_id, args.status)
    assert len(df) == 0


def test_task_status(db_cfg, client_env):
    from jobmon.client.api import BashTask
    from jobmon.client.api import UnknownWorkflow
    from jobmon.client.status_commands import task_status

    t1 = BashTask("exit -9", executor_class="SequentialExecutor",
                  max_runtime_seconds=10, resource_scales={}, max_attempts=2)
    t2 = BashTask("exit -0", executor_class="SequentialExecutor",
                  max_runtime_seconds=10, resource_scales={}, max_attempts=1)
    workflow = UnknownWorkflow(executor_class="SequentialExecutor")
    workflow.add_tasks([t1, t2])
    workflow.run()

    # we should get 2 failed task instances and 1 successful
    command_str = f"task_status -t {t1.task_id} {t2.task_id}"
    cli = CLI()
    args = cli.parse_args(command_str)
    df = task_status(args.task_ids)
    assert len(df) == 3
    assert len(df.query("STATUS=='ERROR'")) == 2
    assert len(df.query("STATUS=='DONE'")) == 1

    # Test filters
    finished_cmd = command_str + f" -s done "
    done_args = cli.parse_args(finished_cmd)
    df_fin = task_status(done_args.task_ids, done_args.status)
    assert len(df_fin) == 1

    all_cmd = command_str + f" -s done fatal"
    all_args = cli.parse_args(all_cmd)
    df_all = task_status(all_args.task_ids, all_args.status)
    assert len(df_all) == 3


def test_dynamic_rate_limiting_cli(db_cfg, client_env):
    """ The server-side logic is checked in scheduler/test_instantiate.

    This test checks the logic of the CLI only
    """

    # Check that a valid ask returns error free
    cli = CLI()
    good_command = "rate_limit -w 5 -m 10"
    args = cli.parse_args(good_command)

    assert args.workflow_id == 5
    assert args.max_tasks == 10

    # Check that an invalid ask will be rejected
    bad_command = "rate_limit -w 5 -m {}"
    with pytest.raises(SystemExit):
        args = cli.parse_args(bad_command.format('foo'))

    with pytest.raises(SystemExit):
        args = cli.parse_args(bad_command.format(-59))
