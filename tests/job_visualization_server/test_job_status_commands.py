import getpass
from jobmon.client import BashTask
from jobmon.client import Workflow
from jobmon.cli import CLI
from jobmon.client.status_commands import (workflow_status, workflow_jobs,
                                           job_status)


def test_workflow_status(env_var, db_cfg):
    user = getpass.getuser()
    t1 = BashTask("sleep 10", executor_class="SequentialExecutor")
    t2 = BashTask("sleep 5", upstream_tasks=[t1],
                  executor_class="SequentialExecutor")
    workflow = Workflow(executor_class="SequentialExecutor")
    workflow.add_tasks([t1, t2])
    workflow._bind()
    workflow.task_dag.job_list_manager.disconnect()

    # we should have the column headers plus 2 jobs in pending
    command_str = f"workflow_status -u {user} -w 1"
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

    # add a second workflow
    t1 = BashTask("sleep 15", executor_class="SequentialExecutor")
    t2 = BashTask("sleep 1", upstream_tasks=[t1],
                  executor_class="SequentialExecutor")
    workflow = Workflow(executor_class="SequentialExecutor")
    workflow.add_tasks([t1, t2])
    workflow._bind()
    workflow.task_dag.job_list_manager.disconnect()

    # check that we get 2 rows now
    command_str = f"workflow_status -u {user}"
    cli = CLI()
    args = cli.parse_args(command_str)
    df = workflow_status(args.workflow_id, args.user)
    assert len(df) == 2

    # check that we can get values by workflow_id
    command_str = "workflow_status -w 2"
    cli = CLI()
    args = cli.parse_args(command_str)
    df = workflow_status(args.workflow_id, args.user)
    assert len(df) == 1
    assert df["WF_ID"][0] == 2

    # check that we can get both
    command_str = "workflow_status -w 1 2"
    cli = CLI()
    args = cli.parse_args(command_str)
    df = workflow_status(args.workflow_id, args.user)
    assert len(df) == 2


def test_workflow_jobs(env_var, db_cfg):
    t1 = BashTask("sleep 3", executor_class="SequentialExecutor",
                  max_runtime_seconds=10, resource_scales={})
    t2 = BashTask("sleep 4", upstream_tasks=[t1],
                  executor_class="SequentialExecutor", max_runtime_seconds=10,
                  resource_scales={})
    workflow = Workflow(executor_class="SequentialExecutor")
    workflow.add_tasks([t1, t2])
    workflow._bind()
    workflow.task_dag.job_list_manager.disconnect()

    # we should get 2 jobs back in pending state
    command_str = "workflow_jobs -w 1"
    cli = CLI()
    args = cli.parse_args(command_str)
    df = workflow_jobs(args.workflow_id, args.status)
    assert len(df) == 2
    assert df.STATUS[0] == "PENDING"
    assert len(df.STATUS.unique()) == 1

    workflow.run()

    # we should get 0 jobs in pending
    command_str = "workflow_jobs -w 1 -s PENDING"
    cli = CLI()
    args = cli.parse_args(command_str)
    df = workflow_jobs(args.workflow_id, args.status)
    assert len(df) == 0

    # we should get 0 jobs when requesting workflow 2
    command_str = "workflow_jobs -w 2"
    cli = CLI()
    args = cli.parse_args(command_str)
    df = workflow_jobs(args.workflow_id, args.status)
    assert len(df) == 0


def test_job_status(env_var, db_cfg):
    t1 = BashTask("exit -9", executor_class="SequentialExecutor",
                  max_runtime_seconds=10, resource_scales={}, max_attempts=2)
    workflow = Workflow(executor_class="SequentialExecutor")
    workflow.add_tasks([t1])
    workflow.run()

    # we should get 2 failed job instances
    command_str = "job_status -j 1"
    cli = CLI()
    args = cli.parse_args(command_str)
    args.func(args)
    state, df = job_status(args.job_id)
    assert state == "FATAL"
    assert len(df) == 2
