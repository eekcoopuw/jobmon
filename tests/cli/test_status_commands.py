import getpass
import logging

from jobmon.client.cli import ClientCLI as CLI

import pytest

logger = logging.getLogger(__name__)


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


def test_task_reset(db_cfg, client_env, monkeypatch):
    from jobmon.client.api import BashTask
    from jobmon.client.api import UnknownWorkflow
    from jobmon.requester import Requester
    from jobmon.client.status_commands import validate_username

    monkeypatch.setattr(getpass, "getuser", mock_getuser)

    workflow = UnknownWorkflow(executor_class="SequentialExecutor")
    t1 = BashTask("sleep 3", executor_class="SequentialExecutor",
                  max_runtime_seconds=10, resource_scales={})
    t2 = BashTask("sleep 4", executor_class="SequentialExecutor",
                  max_runtime_seconds=10, resource_scales={})

    workflow.add_tasks([t1, t2])
    workflow.run()

    # Check that this user is allowed to update
    requester = Requester(client_env)
    validate_username(workflow.workflow_id, 'foo', requester)

    # Validation with a different user raises an error
    with pytest.raises(AssertionError):
        validate_username(workflow.workflow_id, 'notarealuser', requester)


def test_task_reset_wf_validation(db_cfg, client_env):
    from jobmon.client.api import BashTask
    from jobmon.client.api import UnknownWorkflow
    from jobmon.client.status_commands import update_task_status

    workflow1 = UnknownWorkflow(executor_class="SequentialExecutor")
    workflow2 = UnknownWorkflow(executor_class="SequentialExecutor")
    t1 = BashTask("sleep 3", executor_class="SequentialExecutor",
                  max_runtime_seconds=10, resource_scales={})
    t2 = BashTask("sleep 4", executor_class="SequentialExecutor",
                  max_runtime_seconds=10, resource_scales={})

    workflow1.add_tasks([t1])
    workflow1.run()
    workflow2.add_tasks([t2])
    workflow2.run()

    # Check that this user is allowed to update
    command_str = f"update_task_status -t {t1.task_id} {t2.task_id} " \
                  f"-w {workflow1.workflow_id} -s G"
    cli = CLI()
    args = cli.parse_args(command_str)

    # Validation with a task not in the workflow raises an error
    with pytest.raises(AssertionError):
        update_task_status([t1.task_id, t2.task_id], args.workflow_id, args.new_status)


def test_sub_dag(db_cfg, client_env):
    from jobmon.client.api import BashTask
    from jobmon.client.api import UnknownWorkflow
    from jobmon.client.status_commands import get_sub_task_tree

    """
    Dag:
                t1             t2             t3
            /    |     \                     /
           /     |      \                   /
          /      |       \                 /
         /       |        \               /
        t1_1   t1_2            t13_1
         \       |              /
          \      |             /
           \     |            /
              t1_11_213_1_1
    """ # noqa W605
    workflow = UnknownWorkflow(executor_class="SequentialExecutor")
    t1 = BashTask("echo 1", executor_class="SequentialExecutor",
                  max_runtime_seconds=10, resource_scales={})
    t1_1 = BashTask("echo 11", executor_class="SequentialExecutor",
                    max_runtime_seconds=10, resource_scales={})
    t1_2 = BashTask("echo 12", executor_class="SequentialExecutor",
                    max_runtime_seconds=10, resource_scales={})
    t1_11_213_1_1 = BashTask("echo 121", executor_class="SequentialExecutor",
                             max_runtime_seconds=10, resource_scales={})
    t2 = BashTask("echo 2", executor_class="SequentialExecutor",
                  max_runtime_seconds=10, resource_scales={})
    t3 = BashTask("echo 3", executor_class="SequentialExecutor",
                  max_runtime_seconds=10, resource_scales={})
    t13_1 = BashTask("echo 131", executor_class="SequentialExecutor",
                     max_runtime_seconds=10, resource_scales={})
    t1_11_213_1_1.add_upstream(t1_1)
    t1_11_213_1_1.add_upstream(t1_2)
    t1_11_213_1_1.add_upstream((t13_1))
    t1_2.add_upstream(t1)
    t1_1.add_upstream(t1)
    t13_1.add_upstream(t1)
    t13_1.add_upstream(t3)
    workflow.add_tasks([t1, t1_1, t1_2, t1_11_213_1_1, t2, t3, t13_1])
    workflow.bind()
    workflow._create_workflow_run()

    # test node with no sub nodes
    tree = get_sub_task_tree([t2.task_id])
    assert len(tree.items()) == 1
    assert str(t2.task_id) in tree.keys()

    # test node with two upstream
    tree = get_sub_task_tree([t3.task_id])
    assert len(tree.items()) == 3
    assert str(t3.task_id) in tree.keys()
    assert str(t13_1.task_id) in tree.keys()

    # test sub tree
    tree = get_sub_task_tree([t1.task_id])
    assert len(tree.items()) == 5
    assert str(t1.task_id) in tree.keys()
    assert str(t1_1.task_id) in tree.keys()
    assert str(t1_2.task_id) in tree.keys()
    assert str(t1_11_213_1_1.task_id) in tree.keys()
    assert str(t13_1.task_id) in tree.keys()

    # test sub tree with status G
    tree = get_sub_task_tree([t1.task_id], task_status=["G"])
    assert len(tree.items()) == 5
    assert str(t1.task_id) in tree.keys()
    assert str(t1_1.task_id) in tree.keys()
    assert str(t1_2.task_id) in tree.keys()
    assert str(t1_11_213_1_1.task_id) in tree.keys()
    assert str(t13_1.task_id) in tree.keys()

    # test no status match returns 0 nodes
    tree = get_sub_task_tree([t1.task_id], task_status=["F"])
    assert len(tree.items()) == 0

    # test >1 task id list
    # test node with two upstream
    tree = get_sub_task_tree([t3.task_id, t2.task_id])
    assert len(tree.items()) == 4
    assert str(t3.task_id) in tree.keys()
    assert str(t13_1.task_id) in tree.keys()
    assert str(t2.task_id) in tree.keys()


def test_dynamic_concurrency_limiting_cli(db_cfg, client_env):
    """ The server-side logic is checked in scheduler/test_instantiate.

    This test checks the logic of the CLI only
    """

    # Check that a valid ask returns error free
    cli = CLI()
    good_command = "concurrency_limit -w 5 -m 10"
    args = cli.parse_args(good_command)

    assert args.workflow_id == 5
    assert args.max_tasks == 10

    # Check that an invalid ask will be rejected
    bad_command = "concurrency_limit -w 5 -m {}"
    with pytest.raises(SystemExit):
        args = cli.parse_args(bad_command.format('foo'))

    with pytest.raises(SystemExit):
        args = cli.parse_args(bad_command.format(-59))


def test_update_task_status(db_cfg, client_env):
    from jobmon.client.api import BashTask
    from jobmon.client.tool import Tool
    from jobmon.client.status_commands import update_task_status

    # Create a 5 task DAG. Tasks 1-3 should finish, 4 should error out and block 5
    tool = Tool()

    def generate_workflow_and_tasks(tool):

        wf = tool.create_workflow(workflow_args='test_cli_update_workflow')
        wf.set_executor(executor_class="SequentialExecutor")
        tasks = []
        echo_str = 'echo {}'
        for i in range(5):
            if i != 2:
                command_str = echo_str.format(i)
            else:
                command_str = 'exit -9'
            task = BashTask(command=command_str, name=f'task{i}',
                            executor_class="SequentialExecutor",
                            max_runtime_seconds=25,
                            num_cores=1,
                            upstream_tasks=tasks, max_attempts=1)
            tasks.append(task)
        wf.add_tasks(tasks)
        return wf, tasks

    wf1, wf1_tasks = generate_workflow_and_tasks(tool)
    wfr1 = wf1.run()
    wfr1_statuses = [st.status for st in wfr1.swarm_tasks.values()]
    assert wfr1_statuses == ["D", "D", "F", "G", "G"]

    # Set the 'F' task to 'D' to allow progression
    cli = CLI()
    update_str = f'update_task_status -w {wf1.workflow_id} -t {wf1_tasks[2].task_id} -s D'
    args = cli.parse_args(update_str)
    update_task_status(task_ids=args.task_ids, workflow_id=args.workflow_id,
                       new_status=args.new_status)

    # Resume the workflow
    wf2, wf2_tasks = generate_workflow_and_tasks(tool)
    wfr2 = wf2.run(resume=True)

    # Check that wfr2 is done, and that all tasks are "D"
    assert wfr2.status == "D"
    assert all([st.status == "D" for st in wfr2.swarm_tasks.values()])

    # Try a reset of a "done" workflow to "G"
    update_task_status(task_ids=[wf2_tasks[3].task_id], workflow_id=wf2.workflow_id,
                       new_status="G")
    wf3, wf3_tasks = generate_workflow_and_tasks(tool)
    wf3.set_executor(executor_class="SequentialExecutor")
    wf3.bind()
    wf3._workflow_is_resumable()
    wfr3 = wf3._create_workflow_run(resume=True)
    assert len(wfr3._compute_fringe()) == 1
    assert [t.status for t in wfr3.swarm_tasks.values()] == ["D", "D", "D", "G", "G"]

    # Run the workflow
    scheduler_proc = wf3._start_task_instance_scheduler(wfr3.workflow_run_id, 180)
    wfr3.execute_interruptible(scheduler_proc, False, 360)
    scheduler_proc.terminate()

    assert wfr3.status == "D"
    assert all([st.status == "D" for st in wfr3.swarm_tasks.values()])


def test_400_cli_route(db_cfg, client_env):
    from jobmon.requester import Requester
    requester = Requester(client_env, logger)
    rc, resp = requester.send_request(
        app_route="/cli/task_status",
        message={},
        request_type='get',
        logger=logger
    )
    assert rc == 400


def test_bad_put_route(db_cfg, client_env):
    from jobmon.requester import Requester
    requester = Requester(client_env, logger)
    rc, resp = requester.send_request(
        app_route="/cli/task/update_statuses",
        message={},
        request_type='put',
        logger=logger
    )
    assert rc == 400
