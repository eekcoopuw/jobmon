import getpass
import logging

from jobmon.constants import WorkflowRunStatus
from jobmon.client.cli import ClientCLI as CLI

import pytest

logger = logging.getLogger(__name__)


class MockSchedulerProc:

    def is_alive(self):
        return True


def mock_getuser():
    return "status_command_user"


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
    workflow.bind()
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
    assert df == f'{{"WF_ID":{{"0":{workflow.workflow_id}}},"WF_NAME":{{"0":""}},' \
                 f'"WF_STATUS":{{"0":' \
                 '"QUEUED"},"TASKS":{"0":2},"PENDING":{"0":"2 (100.0%)"},' \
                 '"RUNNING":{"0":"0 (0.0%)"},"DONE":{"0":"0 (0.0%)"},"FATAL"' \
                 ':{"0":"0 (0.0%)"},"RETRIES":{"0":0.0}}'

    # add a second workflow
    t1 = BashTask("sleep 15", executor_class="SequentialExecutor")
    t2 = BashTask("sleep 1", upstream_tasks=[t1],
                  executor_class="SequentialExecutor")
    workflow = UnknownWorkflow(executor_class="SequentialExecutor")
    workflow.add_tasks([t1, t2])
    workflow.bind()
    workflow._create_workflow_run()

    # check that we get 2 rows now
    command_str = f"workflow_status -u {user}"
    cli = CLI()
    args = cli.parse_args(command_str)
    df = workflow_status(args.workflow_id, args.user)
    print(df)
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

    # check that we can get both in alternative way
    command_str = "workflow_status -w 1 -w 2"
    cli = CLI()

    args = cli.parse_args(command_str)

    df = workflow_status(args.workflow_id, args.user)
    assert len(df) == 2

    # add 4 more wf to make it 6
    workflow1 = UnknownWorkflow(executor_class="SequentialExecutor")
    t1 = BashTask("sleep 1", executor_class="SequentialExecutor")
    workflow1.add_tasks([t1])
    workflow1.bind()
    workflow1._create_workflow_run()

    workflow2 = UnknownWorkflow(executor_class="SequentialExecutor")
    t2 = BashTask("sleep 2", executor_class="SequentialExecutor")
    workflow2.add_tasks([t2])
    workflow2.bind()
    workflow2._create_workflow_run()

    workflow3 = UnknownWorkflow(executor_class="SequentialExecutor")
    t3 = BashTask("sleep 3", executor_class="SequentialExecutor")
    workflow3.add_tasks([t3])
    workflow3.bind()
    workflow3._create_workflow_run()

    workflow4 = UnknownWorkflow(executor_class="SequentialExecutor")
    t4 = BashTask("sleep 4", executor_class="SequentialExecutor")
    workflow4.add_tasks([t4])
    workflow4.bind()
    workflow4._create_workflow_run()

    # check limit 1, ensure no rows were dropped
    command_str = f"workflow_status -u {user} -l 1"
    cli = CLI()
    args = cli.parse_args(command_str)
    df = workflow_status(user=args.user, limit=args.limit)
    assert len(df) == 1
    # Returned workflow should be the most recent
    assert df.loc[0, "WF_ID"] == workflow4.workflow_id
    wf4_values = df.loc[0, ["WF_STATUS", "TASKS", "PENDING", "RETRIES"]].values
    known_vals = ["QUEUED", 1, "1 (100.0%)", 0]
    for status, known_val in zip(wf4_values, known_vals):
        assert status == known_val

    # check limit 2
    command_str = f"workflow_status -u {user} -l 2"
    cli = CLI()
    args = cli.parse_args(command_str)
    df = workflow_status(user=args.user, limit=args.limit)
    assert len(df) == 2

    # check default (no limit)
    command_str = f"workflow_status -u {user}"
    cli = CLI()
    args = cli.parse_args(command_str)
    df = workflow_status(user=args.user)
    assert len(df) == 5

    # check over limit
    command_str = f"workflow_status -u {user}  -l 12"
    cli = CLI()
    args = cli.parse_args(command_str)
    df = workflow_status(user=args.user, limit=args.limit)
    assert len(df) == 6

    # Check that limit doesn't interfere with workflow_id results
    command_str = "workflow_status -w {} -l 1".format(
                    ' '.join([str(workflow4.workflow_id), str(workflow3.workflow_id)]))
    cli = CLI()
    args = cli.parse_args(command_str)
    df = workflow_status(user={user}, workflow_id=args.workflow_id, limit=args.limit)
    assert len(df) == 2
    wf4_values = df.loc[df.WF_ID == workflow4.workflow_id,
                      ["WF_STATUS", "TASKS", "PENDING", "RETRIES"]].values[0]
    known_vals = ["QUEUED", 1, "1 (100.0%)", 0]
    for status, known_val in zip(wf4_values, known_vals):
        assert status == known_val


def test_workflow_tasks(db_cfg, client_env):
    from jobmon.client.api import BashTask
    from jobmon.client.api import UnknownWorkflow
    from jobmon.client.status_commands import workflow_tasks
    from jobmon.client.execution.scheduler.task_instance_scheduler import \
        TaskInstanceScheduler
    from jobmon.requester import Requester
    workflow = UnknownWorkflow(executor_class="SequentialExecutor")
    t1 = BashTask("sleep 3", executor_class="SequentialExecutor",
                  max_runtime_seconds=10, resource_scales={})
    t2 = BashTask("sleep 4", executor_class="SequentialExecutor",
                  max_runtime_seconds=10, resource_scales={})

    workflow.add_tasks([t1, t2])
    workflow.bind()
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
    requester = Requester(client_env)
    scheduler = TaskInstanceScheduler(workflow.workflow_id, wfr.workflow_run_id,
                                      workflow._executor, requester=requester)
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
    finished_cmd = command_str + " -s done "
    done_args = cli.parse_args(finished_cmd)
    df_fin = task_status(done_args.task_ids, done_args.status)
    assert len(df_fin) == 1

    all_cmd = command_str + " -s done fatal"
    all_args = cli.parse_args(all_cmd)
    df_all = task_status(all_args.task_ids, all_args.status)
    assert len(df_all) == 3


def test_workflow_reset(db_cfg, client_env):
    from jobmon.client.api import Tool, BashTask
    from jobmon.client.execution.strategies.sequential import \
        SequentialExecutor
    from jobmon.requester import Requester
    from jobmon.client.status_commands import workflow_reset

    requester = Requester(client_env)

    unknown_tool = Tool()
    workflow1 = unknown_tool.create_workflow(name="workflow_to_be_reset")
    t1 = BashTask(name="task_wf_reset1", command="exit -9", executor_class="SequentialExecutor")
    workflow1.add_tasks([t1])
    workflow1.set_executor(SequentialExecutor())

    wfr1 = workflow1.run()

    assert wfr1.status == WorkflowRunStatus.ERROR

    command_str = f"workflow_reset -w {workflow1.workflow_id}"
    cli = CLI()
    args = cli.parse_args(command_str)

    wr_result: str = workflow_reset(workflow_id=args.workflow_id, requester=requester)
    assert wr_result == f'Workflow {workflow1.workflow_id} has been reset.'

    # Verify
    #   1.  workflow has status reset to 'G'
    #   2.  task has status reset to 'G'
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        # Verify the workflow status is not 'G'
        q1 = f"select status from workflow where id={workflow1.workflow_id}"
        r1 = DB.session.execute(q1).fetchone()
        assert r1[0] == 'G'

        # Verify there is a task associated with the current workflow, in status 'G'
        q2 = f"select count(t.id) from task t " \
             f"inner join workflow wf on t.workflow_id = wf.id " \
             f"where wf.id={workflow1.workflow_id} and t.status = 'G'"
        r2 = DB.session.execute(q2).fetchone()
        assert r2[0] == 1

    # now resume it, which would still result in Error as expected, but
    # with extra workflow_run and task_instances.
    workflow1 = unknown_tool.create_workflow(
        name="workflow_to_be_reset", workflow_args=workflow1.workflow_args)
    t1 = BashTask(name="task1", command="exit -9", executor_class="SequentialExecutor")
    workflow1.add_tasks([t1])
    workflow1.set_executor(SequentialExecutor())
    wfr2 = workflow1.run(resume=True)

    assert wfr2.status == WorkflowRunStatus.ERROR

    # Verify
    #   1.  there are 2 workflows associated with the workflow_id
    #   2.  there are 6 task_instances associated with the workflow_id
    with app.app_context():
        # Verify there are 2 wfrs
        q1 = f"select count(*) from workflow_run where workflow_id={workflow1.workflow_id}"
        r1 = DB.session.execute(q1).fetchone()
        assert r1[0] == 2

        # Verify there are 6 task_instances (3 for each wfr)
        q2 = f"select count(ti.id) from task_instance ti " \
             f"inner join workflow_run wfr on ti.workflow_run_id = wfr.id " \
             f"inner join workflow wf on wfr.workflow_id = wf.id " \
             f"where wf.id={workflow1.workflow_id}"
        r2 = DB.session.execute(q2).fetchone()
        assert r2[0] == 6

    assert wfr2.status == WorkflowRunStatus.ERROR


def test_get_yaml_data(db_cfg, client_env):
    # create a test workflow
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.api import BashTask
    # Create a test workflow with tasks
    import uuid
    from jobmon.client.execution.strategies.base import ExecutorParameters
    from jobmon.client.tool import Tool

    t = Tool.create_tool(name=str(uuid.uuid4()))
    wf = t.create_workflow(name="i_am_a_fake_wf")
    wf.set_executor(executor_class="SequentialExecutor")
    tt1 = t.get_task_template(
        template_name="tt1",
        command_template="sleep {arg}",
        node_args=["arg"])
    tt2 = t.get_task_template(
        template_name="tt2",
        command_template="echo {arg}",
        node_args=["arg"])
    ep1 = ExecutorParameters(executor_class="SequentialExecutor",
                             queue="all.q")
    ep2 = ExecutorParameters(executor_class="SequentialExecutor",
                             queue="long.q")
    t1 = tt1.create_task(executor_parameters=ep1, arg=1)
    t2 = tt2.create_task(executor_parameters=ep2, arg=2)

    wf = UnknownWorkflow("wf", name="WF_for_yaml_test_1", executor_class="SequentialExecutor")
    wf.add_tasks([t1, t2])
    wf.run()

    # manipulate data
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        query_1 = """
            UPDATE task_instance
            SET wallclock = 10, maxpss = 400
            WHERE task_id = :task_id"""
        DB.session.execute(query_1, {"task_id": t1.task_id})

        query_2 = """
            UPDATE task_instance
            SET wallclock = 20, maxpss = 600
            WHERE task_id = :task_id"""
        DB.session.execute(query_2, {"task_id": t2.task_id})
        DB.session.commit()
    # get data for the resource yaml
    from jobmon.client.status_commands import _get_yaml_data
    result = _get_yaml_data(wf.workflow_id, None, "avg", "avg", "max", wf.requester)
    assert len(result) == 2
    assert result[tt1.task_template_version.id] == ['tt1', 1, 400, 10, "all.q"]
    assert result[tt2.task_template_version.id] == ['tt2', 1, 600, 20, "long.q"]


def test_create_yaml():
    expected = """task_template_resources:
  tt1:
    ihme_slurm:
      num_cores: 1
      m_mem_free: "400B"
      max_runtime_seconds: 10
      queue: "all.q"
    ihme_uge:
      num_cores: 1
      m_mem_free: "400B"
      max_runtime_seconds: 10
      queue: "all.q"
  tt2:
    ihme_slurm:
      num_cores: 1
      m_mem_free: "600B"
      max_runtime_seconds: 20
      queue: "long.q"
    ihme_uge:
      num_cores: 1
      m_mem_free: "600B"
      max_runtime_seconds: 20
      queue: "long.q"
"""
    from jobmon.client.status_commands import _create_yaml
    input = {1: ['tt1', 1, 400, 10, "all.q"],
             2: ['tt2', 1, 600, 20, "long.q"]}
    result = _create_yaml(input, ["ihme_slurm", "ihme_uge"])
    assert result == expected

