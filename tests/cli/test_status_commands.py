import getpass
import logging


import pytest
from unittest.mock import patch

from jobmon.client.cli import ClientCLI as CLI
from jobmon.client.tool import Tool


@pytest.fixture
def tool(db_cfg, client_env):
    tool = Tool()
    tool.set_default_compute_resources_from_dict(
        cluster_name="sequential", compute_resources={"queue": "null.q"}
    )
    return tool


@pytest.fixture
def task_template(tool):
    tt = tool.get_task_template(
        template_name="my_template",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
    )
    return tt


logger = logging.getLogger(__name__)
cli = CLI()


class MockDistributorProc:
    def is_alive(self):
        return True


def mock_getuser():
    return "foo"


def test_workflow_status(db_cfg, client_env, task_template, monkeypatch):
    from jobmon.client.status_commands import workflow_status

    monkeypatch.setattr(getpass, "getuser", mock_getuser)
    user = getpass.getuser()

    tool = Tool()
    workflow = tool.create_workflow(
        default_cluster_name="sequential",
        default_compute_resources_set={"sequential": {"queue": "null.q"}},
    )
    t1 = task_template.create_task(arg="sleep 10")
    t2 = task_template.create_task(arg="sleep 5", upstream_tasks=[t1])
    workflow.add_tasks([t1, t2])
    workflow.bind()
    workflow._create_workflow_run()

    # we should have the column headers plus 2 tasks in pending
    command_str = f"workflow_status -u {user} -w {workflow.workflow_id}"
    args = cli.parse_args(command_str)
    df = workflow_status(args.workflow_id, args.user)
    assert df["PENDING"][0] == "2 (100.0%)"

    # defaults should return an identical value
    command_str = "workflow_status"
    args = cli.parse_args(command_str)
    df = workflow_status(args.workflow_id, args.user)
    assert df["PENDING"][0] == "2 (100.0%)"

    # Test the JSON flag
    command_str = f"workflow_status -u {user} -w {workflow.workflow_id} -n"
    args = cli.parse_args(command_str)
    df = workflow_status(args.workflow_id, args.user, args.json)
    assert (
        df == f'{{"WF_ID":{{"0":{workflow.workflow_id}}},"WF_NAME":{{"0":""}},'
        f'"WF_STATUS":{{"0":'
        '"QUEUED"},"TASKS":{"0":2},"PENDING":{"0":"2 (100.0%)"},'
        '"RUNNING":{"0":"0 (0.0%)"},"DONE":{"0":"0 (0.0%)"},"FATAL"'
        ':{"0":"0 (0.0%)"},"RETRIES":{"0":0.0}}'
    )

    # add a second workflow
    t1 = task_template.create_task(arg="sleep 15")
    t2 = task_template.create_task(arg="sleep 1", upstream_tasks=[t1])
    workflow = tool.create_workflow(
        default_cluster_name="sequential",
        default_compute_resources_set={"sequential": {"queue": "null.q"}},
    )
    workflow.add_tasks([t1, t2])
    workflow.bind()
    workflow._create_workflow_run()

    # check that we get 2 rows now
    command_str = f"workflow_status -u {user}"
    args = cli.parse_args(command_str)
    df = workflow_status(args.workflow_id, args.user)
    assert len(df) == 2

    # check that we can get values by workflow_id
    command_str = f"workflow_status -w {workflow.workflow_id}"
    args = cli.parse_args(command_str)
    df = workflow_status(args.workflow_id, args.user)
    assert len(df) == 1
    assert df["WF_ID"][0] == workflow.workflow_id

    # check that we can get both
    command_str = "workflow_status -w 1 2"
    args = cli.parse_args(command_str)
    df = workflow_status(args.workflow_id, args.user)
    assert len(df) == 2

    # add 4 more wf to make it 6
    workflow1 = tool.create_workflow(
        default_cluster_name="sequential",
        default_compute_resources_set={"sequential": {"queue": "null.q"}},
    )
    t1 = task_template.create_task(arg="sleep 1")
    workflow1.add_tasks([t1])
    workflow1.bind()
    workflow1._create_workflow_run()

    workflow2 = tool.create_workflow(
        default_cluster_name="sequential",
        default_compute_resources_set={"sequential": {"queue": "null.q"}},
    )
    t2 = task_template.create_task(arg="sleep 2")
    workflow2.add_tasks([t2])
    workflow2.bind()
    workflow2._create_workflow_run()

    workflow3 = tool.create_workflow(
        default_cluster_name="sequential",
        default_compute_resources_set={"sequential": {"queue": "null.q"}},
    )
    t3 = task_template.create_task(arg="sleep 3")
    workflow3.add_tasks([t3])
    workflow3.bind()
    workflow3._create_workflow_run()

    workflow4 = tool.create_workflow(
        default_cluster_name="sequential",
        default_compute_resources_set={"sequential": {"queue": "null.q"}},
    )
    t4 = task_template.create_task(arg="sleep 4")
    workflow4.add_tasks([t4])
    workflow4.bind()
    workflow4._create_workflow_run()

    # check limit 1
    command_str = f"workflow_status -u {user}  -l 1"
    args = cli.parse_args(command_str)
    df = workflow_status(user=args.user, limit=args.limit)
    assert len(df) == 1

    # check limit 2
    command_str = f"workflow_status -u {user}  -l 2"
    args = cli.parse_args(command_str)
    df = workflow_status(user=args.user, limit=args.limit)
    assert len(df) == 2

    # check default (no limit)
    command_str = f"workflow_status -u {user}"
    args = cli.parse_args(command_str)
    df = workflow_status(user=args.user)
    assert len(df) == 5

    # check default (limit without value)
    command_str = f"workflow_status -u {user} -l"
    args = cli.parse_args(command_str)
    df = workflow_status(user=args.user)
    assert len(df) == 5

    # check over limit
    command_str = f"workflow_status -u {user}  -l 12"
    args = cli.parse_args(command_str)
    df = workflow_status(user=args.user, limit=args.limit)
    assert len(df) == 6

    # check 0
    command_str = f"workflow_status -u {user}  -l 0"
    args = cli.parse_args(command_str)
    df = workflow_status(user=args.user, limit=args.limit)
    assert len(df) == 0

    # check negative
    command_str = f"workflow_status -u {user}  -l -1"
    args = cli.parse_args(command_str)
    df = workflow_status(user=args.user, limit=args.limit)
    assert len(df) == 6


def test_workflow_tasks(db_cfg, client_env, task_template):
    from jobmon.client.status_commands import workflow_tasks
    from jobmon.client.distributor.distributor_service import DistributorService
    from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun
    from jobmon.cluster_type.sequential.seq_distributor import SequentialDistributor
    from jobmon.requester import Requester
    from jobmon.constants import WorkflowRunStatus

    tool = Tool()
    workflow = tool.create_workflow(
        default_cluster_name="sequential",
        default_compute_resources_set={"sequential": {"queue": "null.q"}},
    )
    t1 = task_template.create_task(arg="sleep 3")
    t2 = task_template.create_task(arg="sleep 4")

    workflow.add_tasks([t1, t2])
    workflow.bind()
    client_wfr = workflow._create_workflow_run()
    wfr = SwarmWorkflowRun(
        workflow_id=workflow.workflow_id,
        workflow_run_id=client_wfr.workflow_run_id,
        tasks=workflow.tasks.values(),
    )

    # we should get 2 tasks back in pending state
    command_str = f"workflow_tasks -w {workflow.workflow_id}"
    args = cli.parse_args(command_str)
    df = workflow_tasks(args.workflow_id)
    assert len(df) == 2
    assert df.STATUS[0] == "PENDING"
    assert len(df.STATUS.unique()) == 1

    # execute the tasks
    requester = Requester(client_env)
    seq_distributor = SequentialDistributor()
    distributor = DistributorService(
        workflow.workflow_id, wfr.workflow_run_id, seq_distributor, requester=requester
    )
    with pytest.raises(RuntimeError):
        # Set the is_alive to always true
        workflow._distributor_alive = lambda: True
        workflow._run_swarm(swarm=wfr, seconds_until_timeout=1)

    distributor._get_tasks_queued_for_instantiation()
    distributor.distribute()

    # we should get 0 tasks in pending
    command_str = f"workflow_tasks -w {workflow.workflow_id} -s PENDING"
    args = cli.parse_args(command_str)
    df = workflow_tasks(args.workflow_id, args.status)
    assert len(df) == 0

    # we should get 0 tasks when requesting workflow -99
    command_str = "workflow_tasks -w -99"
    args = cli.parse_args(command_str)
    df = workflow_tasks(args.workflow_id, args.status)
    assert len(df) == 0

    # limit testing
    workflow = tool.create_workflow(
        name="test_100_tasks_with_limit_testing",
        default_cluster_name="multiprocess",
        default_compute_resources_set={"multiprocess": {"queue": "null.q"}},
    )

    for i in range(6):
        t = task_template.create_task(arg=f"echo {i}", upstream_tasks=[])
        workflow.add_task(t)

    workflow.bind()

    wfrs = workflow.run()
    assert wfrs == WorkflowRunStatus.DONE

    # check limit 1
    command_str = f"workflow_tasks -w {workflow.workflow_id} -l 1"
    args = cli.parse_args(command_str)
    df = workflow_tasks(args.workflow_id, limit=args.limit)
    assert len(df) == 1

    # check limit 2
    command_str = f"workflow_tasks -w {workflow.workflow_id} -l 2"
    args = cli.parse_args(command_str)
    df = workflow_tasks(args.workflow_id, limit=args.limit)
    assert len(df) == 2

    # check default (no limit)
    command_str = f"workflow_tasks -w {workflow.workflow_id}"
    args = cli.parse_args(command_str)
    df = workflow_tasks(args.workflow_id)
    assert len(df) == 5

    # check default (limit without value)
    command_str = f"workflow_tasks -w {workflow.workflow_id} -l"
    args = cli.parse_args(command_str)
    df = workflow_tasks(args.workflow_id)
    assert len(df) == 5

    # check over limit
    command_str = f"workflow_tasks -w {workflow.workflow_id} -l 12"
    args = cli.parse_args(command_str)
    df = workflow_tasks(args.workflow_id, limit=args.limit)
    assert len(df) == 6

    # check 0
    command_str = f"workflow_tasks -w {workflow.workflow_id} -l 0"
    args = cli.parse_args(command_str)
    df = workflow_tasks(args.workflow_id, limit=args.limit)
    assert len(df) == 0

    # check negative
    command_str = f"workflow_tasks -w {workflow.workflow_id} -l -1"
    args = cli.parse_args(command_str)
    df = workflow_tasks(args.workflow_id, limit=args.limit)
    assert len(df) == 6


def test_task_status(db_cfg, client_env, tool, task_template):
    from jobmon.client.status_commands import task_status

    t1 = task_template.create_task(arg="exit -9", max_attempts=2)
    t2 = task_template.create_task(arg="exit -0")
    workflow = tool.create_workflow()
    workflow.add_tasks([t1, t2])
    workflow.run()

    # we should get 2 failed task instances and 1 successful
    command_str = f"task_status -t {t1.task_id} {t2.task_id}"

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


def test_task_reset(db_cfg, client_env, tool, task_template, monkeypatch):
    from jobmon.requester import Requester
    from jobmon.client.status_commands import validate_username

    monkeypatch.setattr(getpass, "getuser", mock_getuser)

    workflow = tool.create_workflow()
    t1 = task_template.create_task(arg="sleep 3")
    t2 = task_template.create_task(arg="sleep 4")

    workflow.add_tasks([t1, t2])
    workflow.run()

    # Check that this user is allowed to update
    requester = Requester(client_env)
    validate_username(workflow.workflow_id, "foo", requester)

    # Validation with a different user raises an error
    with pytest.raises(AssertionError):
        validate_username(workflow.workflow_id, "notarealuser", requester)


def test_task_reset_wf_validation(db_cfg, client_env, tool, task_template):
    from jobmon.requester import Requester
    from jobmon.client.status_commands import update_task_status, validate_workflow

    workflow1 = tool.create_workflow()
    workflow2 = tool.create_workflow()
    t1 = task_template.create_task(arg="sleep 3")
    t2 = task_template.create_task(arg="sleep 4")

    workflow1.add_tasks([t1])
    workflow1.run()
    workflow2.add_tasks([t2])
    workflow2.run()

    # Check that this user is allowed to update
    command_str = (
        f"update_task_status -t {t1.task_id} {t2.task_id} "
        f"-w {workflow1.workflow_id} -s G"
    )

    args = cli.parse_args(command_str)

    # Validation with a task not in the workflow raises an error
    with pytest.raises(AssertionError):
        update_task_status([t1.task_id, t2.task_id], args.workflow_id, args.new_status)

    # Test that the number of resets requested doesn't break HTTP
    with pytest.raises(AssertionError):
        requester = Requester(client_env)
        task_ids = list(range(300))
        # AssertionError since we have 2 workflows, but no HTTP 502 returned
        validate_workflow(task_ids, requester)


def test_sub_dag(db_cfg, client_env, tool, task_template):
    from jobmon.client.status_commands import get_sub_task_tree

    """
    Dag:
                t1             t2             t3
            /    |     \\                     /
           /     |      \\                   /
          /      |       \\                 /
         /       |        \\               /
        t1_1   t1_2            t13_1
         \\       |              /
          \\      |             /
           \\     |            /
              t1_11_213_1_1
    """  # noqa W605
    workflow = tool.create_workflow()
    t1 = task_template.create_task(arg="echo 1")
    t1_1 = task_template.create_task(arg="echo 11")
    t1_2 = task_template.create_task(arg="echo 12")
    t1_11_213_1_1 = task_template.create_task(arg="echo 121")
    t2 = task_template.create_task(arg="echo 2")
    t3 = task_template.create_task(arg="echo 3")
    t13_1 = task_template.create_task(arg="echo 131")
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
    """The server-side logic is checked in distributor/test_instantiate.

    This test checks the logic of the CLI only
    """

    # Check that a valid ask returns error free

    good_command = "concurrency_limit -w 5 -m 10"
    args = cli.parse_args(good_command)

    assert args.workflow_id == 5
    assert args.max_tasks == 10

    # Check that an invalid ask will be rejected
    bad_command = "concurrency_limit -w 5 -m {}"
    with pytest.raises(SystemExit):
        cli.parse_args(bad_command.format("foo"))

    with pytest.raises(SystemExit):
        cli.parse_args(bad_command.format(-59))


def test_update_task_status(db_cfg, client_env, tool, task_template):
    from jobmon.client.status_commands import update_task_status
    from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun
    from jobmon.constants import WorkflowRunStatus

    # Create a 5 task DAG. Tasks 1-3 should finish, 4 should error out and block 5
    def generate_workflow_and_tasks(tool, template):

        wf = tool.create_workflow(workflow_args="test_cli_update_workflow")
        tasks = []
        echo_str = "echo {}"
        for i in range(5):
            if i != 2:
                command_str = echo_str.format(i)
            else:
                command_str = "exit -9"
            task = template.create_task(
                arg=command_str, name=f"task{i}", upstream_tasks=tasks, max_attempts=1
            )
            tasks.append(task)
        wf.add_tasks(tasks)
        return wf, tasks

    wf1, wf1_tasks = generate_workflow_and_tasks(tool, task_template)
    wf1.run()
    wfr1_statuses = [t.final_status for t in wf1_tasks]
    assert wfr1_statuses == ["D", "D", "F", "G", "G"]

    # Set the 'F' task to 'D' to allow progression

    update_str = (
        f"update_task_status -w {wf1.workflow_id} -t {wf1_tasks[2].task_id} -s D"
    )
    args = cli.parse_args(update_str)
    update_task_status(
        task_ids=args.task_ids, workflow_id=args.workflow_id, new_status=args.new_status
    )

    # Resume the workflow
    wf2, wf2_tasks = generate_workflow_and_tasks(tool, task_template)
    wfr2_status = wf2.run(resume=True)

    # Check that wfr2 is done, and that all tasks are "D"
    assert wfr2_status == "D"
    assert all([t.final_status == "D" for t in wf2_tasks])

    # Try a reset of a "done" workflow to "G"
    update_task_status(
        task_ids=[wf2_tasks[3].task_id], workflow_id=wf2.workflow_id, new_status="G"
    )
    wf3, wf3_tasks = generate_workflow_and_tasks(tool, task_template)
    wf3.bind()
    wf3._workflow_is_resumable()
    client_wfr3 = wf3._create_workflow_run(resume=True)

    wfr3 = SwarmWorkflowRun(
        workflow_id=wf3.workflow_id,
        workflow_run_id=client_wfr3.workflow_run_id,
        tasks=list(wf3.tasks.values()),
    )
    wf3._distributor_proc = wf3._start_distributor_service(wfr3.workflow_run_id)
    wfr3._compute_initial_fringe()
    assert len(wfr3.ready_to_run) == 1
    assert [t.status for t in wfr3.swarm_tasks.values()] == ["D", "D", "D", "G", "G"]

    # Run the workflow
    wf3._run_swarm(wfr3)

    assert wfr3.status == "D"
    assert all([st.status == "D" for st in wfr3.swarm_tasks.values()])


def test_400_cli_route(db_cfg, client_env):
    from jobmon.requester import Requester

    requester = Requester(client_env)
    rc, resp = requester.send_request(
        app_route="/task_status", message={}, request_type="get", logger=logger
    )
    assert rc == 400


def test_bad_put_route(db_cfg, client_env):
    from jobmon.requester import Requester

    requester = Requester(client_env, logger)
    rc, resp = requester.send_request(
        app_route="/task/update_statuses", message={}, request_type="put", logger=logger
    )
    assert rc == 400


def test_get_yaml_data(db_cfg, client_env):
    import jobmon.client.status_commands
    with patch("jobmon.client.status_commands._get_exclude_tt_list") as f:
        f.return_value = set()  # no exclude tt
        t = Tool()
        wf = t.create_workflow(name="i_am_a_fake_wf")
        tt1 = t.get_task_template(
            template_name="tt1", command_template="echo {arg}", node_args=["arg"]
        )
        tt2 = t.get_task_template(
            template_name="tt2", command_template="sleep {arg}", node_args=["arg"]
        )
        t1 = tt1.create_task(
            arg=1, cluster_name="sequential", compute_resources={"queue": "null.q"}
        )
        t2 = tt2.create_task(
            arg=2, cluster_name="sequential", compute_resources={"queue": "null2.q"}
        )

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
        assert result[tt1._active_task_template_version.id] == ["tt1", 1, 400, 10, "null.q"]
        assert result[tt2._active_task_template_version.id] == [
            "tt2",
            1,
            600,
            20,
            "null2.q",
        ]


def test_create_yaml():
    expected = """task_template_resources:
  tt1:
    ihme_slurm:
      cores: 1
      memory: "400B"
      runtime: 10
      queue: "all.q"
    ihme_uge:
      cores: 1
      memory: "400B"
      runtime: 10
      queue: "all.q"
  tt2:
    ihme_slurm:
      cores: 1
      memory: "600B"
      runtime: 20
      queue: "long.q"
    ihme_uge:
      cores: 1
      memory: "600B"
      runtime: 20
      queue: "long.q"
"""
    from jobmon.client.status_commands import _create_yaml

    input = {1: ["tt1", 1, 400, 10, "all.q"], 2: ["tt2", 1, 600, 20, "long.q"]}
    result = _create_yaml(input, ["ihme_slurm", "ihme_uge"])
    assert result == expected
