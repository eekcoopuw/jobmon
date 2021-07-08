import time

from jobmon.requester import Requester

import pytest

from jobmon.client.task import Task
from jobmon.client.tool import Tool


@pytest.fixture
def task_template(db_cfg, client_env):
    tool = Tool()
    tt = tool.get_task_template(
        template_name="my_template",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[]
    )
    return tt


class MockDistributorProc:

    def __init__(self):
        pass

    def is_alive(self) -> bool:
        return True


def test_unknown_state(db_cfg, client_env, task_template, monkeypatch):
    """Creates a job instance, gets an distributor id so it can be in submitted
    to the batch distributor state, and then it will never be run (it will miss
    its report by date and the reconciler will kill it)"""
    from jobmon.client.distributor.distributor_task_instance import DistributorTaskInstance
    from jobmon.client.distributor.distributor_service import DistributorService

    class MockDistributorTaskInstance(DistributorTaskInstance):
        def dummy_distributor_task_instance_run_and_done(self):
            # do nothing so job gets marked as Batch then Unknown
            """For all instances other than the DummyExecutor, the worker node task instance should
            be the one logging the done status. Since the DummyExecutor doesn't actually run
            anything, the task_instance_scheduler needs to mark it done so that execution can
            proceed through the DAG.
            """
            if self.executor.__class__.__name__ != "DummyExecutor":
                logger.error("Cannot directly log a task instance done unless using the Dummy "
                             "Executor")
            logger.debug("Moving the job to running, then done so that dependencies can proceed to"
                         " mock a successful dag traversal process")
            run_app_route = f'/worker/task_instance/{self.task_instance_id}/log_running'
            run_message = {'process_group_id': '0', 'next_report_increment': 60}
            return_code, response = self.requester.send_request(
                app_route=run_app_route,
                message=run_message,
                request_type='post',
                logger=logger
            )
            if return_code != StatusCodes.OK:
                raise InvalidResponse(f'Unexpected status code {return_code} from POST '
                                      f'request through route {run_app_route}. Expected '
                                      f'code 200. Response content: {response}')
            done_app_route = f"/worker/task_instance/{self.task_instance_id}/log_done"
            done_message = {'nodename': 'DummyNode', 'executor_id': self.executor_id}
            return_code, response = self.requester.send_request(
                app_route=done_app_route,
                message=done_message,
                request_type='post',
                logger=logger
            )
            if return_code != StatusCodes.OK:
                raise InvalidResponse(f'Unexpected status code {return_code} from POST '
                                      f'request through route {done_app_route}. Expected '
                                      f'code 200. Response content: {response}')

    monkeypatch.setattr(DistributorTaskInstance, "dummy_distributor_task_instance_run_and_done",
                        MockDistributorTaskInstance.dummy_distributor_task_instance_run_and_done)

    tool = Tool()

    # Queue a job
    task = task_template.create_task(
        arg="ls", name="dummyfbb", max_attempts=1,
        cluster_name="dummy")
    workflow = tool.create_workflow(name="foo")
    workflow.add_task(task)

    # add workflow info to db and then time out.
    workflow.bind(cluster_name="dummy", compute_resources={"queue": "null.q"})
    wfr = workflow._create_workflow_run()

    requester = Requester(client_env)
    distributor_service = DistributorService(workflow.workflow_id, wfr.workflow_run_id,
                                             "dummy", requester=requester,
                                             task_heartbeat_interval=5)
    with pytest.raises(RuntimeError):
        wfr.execute_interruptible(MockDistributorProc(), seconds_until_timeout=1)

    # How long we wait for a JI to report it is running before reconciler moves
    # it to error state.
    distributor_service.distribute()

    # Since we are using the 'dummy' distributor, we never actually do
    # any work. The job gets moved to lost_track during reconciliation
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    app.app_context().push()
    sql = """
        SELECT task_instance.status
        FROM task_instance
        JOIN task
            ON task_instance.task_id = task.id
        WHERE task.id = :task_id"""
    res = DB.session.execute(sql, {"task_id": str(task.task_id)}).fetchone()
    DB.session.commit()
    assert res[0] == "B"

    # sleep through the report by date
    time.sleep(distributor_service._task_heartbeat_interval * (distributor_service._report_by_buffer + 1))

    # job will move into lost track because it never logs a heartbeat
    distributor_service._get_lost_task_instances()
    assert len(distributor_service._to_reconcile) == 1

    # will check the distributor's return state and move the job to unknown
    distributor_service.distribute()
    res = DB.session.execute(sql, {"task_id": str(task.task_id)}).fetchone()
    DB.session.commit()
    assert res[0] == "U"

    # because we only allow 1 attempt job will move to E after job instance
    # moves to U
    wfr._parse_adjusting_done_and_errors(wfr._task_status_updates())
    assert len(wfr.all_error) > 0


def test_log_distributor_report_by(db_cfg, client_env, task_template, monkeypatch):
    """test that jobs that are queued by an distributor but not running still log
    heartbeats"""
    from jobmon.cluster_type import sequential
    from jobmon.client.distributor.distributor_service import DistributorService

    # patch unwrap from sequential so the command doesn't execute,
    # def mock_unwrap(*args, **kwargs):
    #     pass
    # monkeypatch.setattr(sequential, "unwrap", mock_unwrap)

    tool = Tool()

    task = task_template.create_task(
        arg="sleep 5", name="heartbeat_sleeper",
        compute_resources={"queue": "null.q", "cores": 1, "max_runtime_seconds": 500},
        cluster_name="sequential")
    workflow = tool.create_workflow(name="foo")
    workflow.add_task(task)

    # add workflow info to db and then time out.
    workflow.bind(cluster_name="sequential")
    wfr = workflow._create_workflow_run()

    requester = Requester(client_env)
    distributor_service = DistributorService(workflow.workflow_id, wfr.workflow_run_id,
                                             "sequential", requester=requester)
    with pytest.raises(RuntimeError):
        wfr.execute_interruptible(MockDistributorProc(), seconds_until_timeout=1)

    # instantiate the job and then log a report by
    distributor_service.distribute()
    distributor_service._log_distributor_report_by()

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        sql = """
        SELECT task_instance.submitted_date, task_instance.report_by_date
        FROM task_instance
        JOIN task
            ON task_instance.task_id = task.id
        WHERE task.id = :task_id"""
        res = DB.session.execute(sql, {"task_id": str(task.task_id)}).fetchone()
        DB.session.commit()
    start, end = res
    assert start < end  # indicating at least one heartbeat got logged
