import ast
import logging
import os
import time

from jobmon.cluster_type.sequential.seq_distributor import SequentialDistributor
from jobmon.constants import WorkflowRunStatus
from jobmon.exceptions import CallableReturnedInvalidObject
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.models.task_status import TaskStatus

import pytest


logger = logging.getLogger(__name__)


this_dir = os.path.dirname(os.path.abspath(__file__))
resource_file = os.path.join(this_dir, "resources.txt")


class MockDistributorProc:
    def is_alive(self):
        return True


def test_blocking_update_timeout(tool, task_template):
    """This test runs a 1 task workflow and confirms that the workflow_run
    will timeout with an appropriate error message if timeout is set
    """
    from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun

    task = task_template.create_task(arg="sleep 3", name="foobarbaz")
    workflow = tool.create_workflow(name="my_simple_dag")
    workflow.add_tasks([task])
    workflow.bind()
    workflow._distributor_proc = MockDistributorProc()
    wfr = workflow._create_workflow_run()

    # Move workflow and wfr through Instantiating -> Launched
    wfr._update_status(WorkflowRunStatus.INSTANTIATING)
    wfr._update_status(WorkflowRunStatus.LAUNCHED)

    # swarm calls
    swarm = SwarmWorkflowRun(
        workflow_id=workflow.workflow_id,
        workflow_run_id=wfr.workflow_run_id,
        tasks=list(workflow.tasks.values()),
        requester=workflow.requester,
    )

    with pytest.raises(RuntimeError) as error:
        workflow._run_swarm(swarm, seconds_until_timeout=2)

    expected_msg = (
        "Not all tasks completed within the given workflow "
        "timeout length (2 seconds). Submitted tasks will still"
        " run, but the workflow will need to be restarted."
    )
    assert expected_msg == str(error.value)


def test_sync_statuses(client_env, tool, task_template):
    """this test executes a single task workflow where the task fails. It
    is testing to confirm that the status updates are propagated into the
    swarm objects"""
    from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun
    from jobmon.client.distributor.distributor_service import DistributorService

    # client calls
    task = task_template.create_task(arg="fizzbuzz", name="bar", max_attempts=1)
    workflow = tool.create_workflow()
    workflow.add_tasks([task])
    workflow.bind()
    wfr = workflow._create_workflow_run()

    # move workflow to launched state
    distributor_service = DistributorService(
        workflow.workflow_id,
        wfr.workflow_run_id,
        SequentialDistributor(),
        requester=workflow.requester,
    )

    # swarm calls
    swarm = SwarmWorkflowRun(
        workflow_id=workflow.workflow_id,
        workflow_run_id=wfr.workflow_run_id,
        tasks=list(workflow.tasks.values()),
        requester=workflow.requester,
    )
    swarm.update_status(WorkflowRunStatus.RUNNING)
    swarm.compute_initial_dag_state()
    time.sleep(1)  # make sure some time passes
    list(swarm.queue_tasks())

    # test initial dag state updates last_sync
    now = swarm.last_sync
    assert now is not None

    # distribute the task
    distributor_service._get_tasks_queued_for_instantiation()
    distributor_service.distribute()

    swarm.block_until_newly_ready_or_all_done()

    new_now = swarm.last_sync
    assert new_now > now
    assert len(swarm.all_error) > 0


def test_wedged_dag(db_cfg, tool, task_template):
    """This test runs a 3 task dag where one of the tasks updates it status
    without updating its status date. This would cause the normal pathway of
    status collection in the workflow run to fail. Instead the test uses the
    wedged_workflow_sync_interval set to -1 second to force a full sync of
    the workflow tasks which resolves the wedge"""
    from jobmon.cluster_type.dummy import DummyDistributor
    from jobmon.worker_node.cli import WorkerNodeCLI
    from jobmon.client.distributor.distributor_service import DistributorService
    from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun

    class WedgedDistributor(DummyDistributor):

        wedged_task_id = None
        app = db_cfg["app"]
        DB = db_cfg["DB"]

        def submit_to_batch_distributor(
            self, command: str, name: str, requested_resources
        ) -> int:
            logger.info("Now entering WedgedExecutor execute")

            cli = WorkerNodeCLI()
            args = cli.parse_args(command)

            # need to get task id from task instance here to compare to wedged
            # task id that will be set later in the code
            with self.app.app_context():
                task_instance = (
                    self.DB.session.query(TaskInstance)
                    .filter_by(id=args.task_instance_id)
                    .one()
                )
                task_id = int(task_instance.task.id)

            if task_id == self.wedged_task_id:
                logger.info(
                    f"task instance is {self.wedged_task_id}, entering"
                    " first if statement"
                )
                task_inst_query = """
                    UPDATE task_instance
                    SET status = 'D'
                    WHERE task_instance.id = {task_instance_id}
                """.format(
                    task_instance_id=args.task_instance_id
                )
                task_query = """
                    UPDATE task
                    SET task.status = 'D',
                        task.status_date = SUBTIME(CURRENT_TIMESTAMP(),
                                                   SEC_TO_TIME(600))
                    WHERE task.id = {task_id}
                """.format(
                    task_id=task_id
                )

                with self.app.app_context():
                    self.DB.session.execute(task_inst_query)
                    self.DB.session.commit()
                    self.DB.session.execute(task_query)
                    self.DB.session.commit()

                exec_id = 123456789
            else:
                exec_id = super().submit_to_batch_distributor(
                    command, name, requested_resources
                )

            return exec_id

    workflow = tool.create_workflow()
    t1 = tool.active_task_templates["simple_template"].create_task(arg="sleep 3")
    t2 = tool.active_task_templates["simple_template"].create_task(arg="sleep 5")
    t3 = tool.active_task_templates["simple_template"].create_task(
        arg="sleep 7", upstream_tasks=[t2]
    )
    workflow.add_tasks([t1, t2, t3])

    # bind workflow to db
    workflow.bind()
    wfr = workflow._create_workflow_run()

    # create distributor with WedgedDistributor
    distributor = WedgedDistributor()
    distributor.wedged_task_id = t2.task_id
    distributor_service = DistributorService(
        workflow.workflow_id,
        wfr.workflow_run_id,
        distributor,
        requester=workflow.requester,
    )

    # queue first 2 tasks
    swarm = SwarmWorkflowRun(
        workflow_id=workflow.workflow_id,
        workflow_run_id=wfr.workflow_run_id,
        tasks=list(workflow.tasks.values()),
        requester=workflow.requester,
    )
    swarm.update_status(WorkflowRunStatus.RUNNING)
    swarm.compute_initial_dag_state()
    list(swarm.queue_tasks())

    # launch task on executor
    distributor_service._get_tasks_queued_for_instantiation()
    distributor_service.distribute()

    workflow._distributor_proc = MockDistributorProc()
    # run the normal workflow sync protocol. only t1 should be done
    with pytest.raises(RuntimeError):
        swarm.block_until_newly_ready_or_all_done(
            poll_interval=1, seconds_until_timeout=1
        )

    assert swarm.swarm_tasks[t1.task_id].status == TaskStatus.DONE
    assert swarm.swarm_tasks[t2.task_id].status == TaskStatus.QUEUED_FOR_INSTANTIATION
    assert swarm.swarm_tasks[t3.task_id].status == TaskStatus.REGISTERED

    # now run wedged dag route. make sure task 2 is now in done state
    with pytest.raises(RuntimeError):
        workflow._run_swarm(
            swarm, seconds_until_timeout=1, wedged_workflow_sync_interval=-1
        )
    assert swarm.swarm_tasks[t1.task_id].status == TaskStatus.DONE
    assert swarm.swarm_tasks[t2.task_id].status == TaskStatus.DONE

    list(swarm.queue_tasks())
    assert swarm.swarm_tasks[t3.task_id].status == TaskStatus.QUEUED_FOR_INSTANTIATION


def test_fail_fast(tool, task_template):
    """set up a dag where a middle job fails. The fail_fast parameter should
    ensure that not all tasks finish"""

    # The sleep for t3 must be long so that the swarm has time to notice that t2
    # died and react accordingly.
    workflow = tool.create_workflow(name="test_fail_fast")
    t1 = task_template.create_task(arg="sleep 1")
    t2 = task_template.create_task(arg="erroring_out 1", upstream_tasks=[t1])
    t3 = task_template.create_task(arg="sleep 20", upstream_tasks=[t1])
    t4 = task_template.create_task(arg="sleep 3", upstream_tasks=[t3])
    t5 = task_template.create_task(arg="sleep 4", upstream_tasks=[t4])

    workflow.add_tasks([t1, t2, t3, t4, t5])
    workflow.bind()

    with pytest.raises(RuntimeError):
        workflow.run(fail_fast=True)

    assert len(workflow.task_errors) == 1
    num_done = len(
        [
            task
            for task in workflow.tasks.values()
            if task.final_status == TaskStatus.DONE
        ]
    )
    assert num_done >= 1
    assert num_done <= 3


def test_propagate_result(tool, task_template):
    """set up workflow with 3 tasks on one layer and 3 tasks as dependant"""
    from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun

    workflow = tool.create_workflow(name="test_propagate_result")

    t1 = task_template.create_task(arg="echo 1")
    t2 = task_template.create_task(arg="echo 2")
    t3 = task_template.create_task(arg="echo 3")
    t4 = task_template.create_task(arg="echo 4", upstream_tasks=[t1, t2, t3])
    t5 = task_template.create_task(arg="echo 5", upstream_tasks=[t1, t2, t3])
    t6 = task_template.create_task(arg="echo 6", upstream_tasks=[t1, t2, t3])
    workflow.add_tasks([t1, t2, t3, t4, t5, t6])
    workflow.bind()
    wfr = workflow._create_workflow_run()

    # run the distributor
    workflow._distributor_proc = workflow._start_distributor_service(
        wfr.workflow_run_id
    )

    # swarm calls
    swarm = SwarmWorkflowRun(
        workflow_id=workflow.workflow_id,
        workflow_run_id=wfr.workflow_run_id,
        tasks=list(workflow.tasks.values()),
        requester=workflow.requester,
    )
    workflow._run_swarm(swarm)

    # stop the subprocess
    workflow._distributor_stop_event.set()

    assert swarm.status == WorkflowRunStatus.DONE
    assert len(swarm.all_done) == 6
    assert swarm.swarm_tasks[t4.task_id].num_upstreams_done >= 3
    assert swarm.swarm_tasks[t5.task_id].num_upstreams_done >= 3
    assert swarm.swarm_tasks[t6.task_id].num_upstreams_done >= 3


def test_callable_returns_valid_object(tool, task_template):
    """Test when the provided callable returns the correct parameters"""
    from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun

    def resource_file_does_exist(*args, **kwargs):
        # file contains dict with
        # {'m_mem_free': '2G', 'max_runtime_seconds': 30, 'num_cores': 1,
        # 'queue': 'all.q'}
        with open(resource_file, "r") as file:
            resources = file.read()
            resource_dict = ast.literal_eval(resources)
        return resource_dict

    workflow = tool.create_workflow(workflow_args="dynamic_resource_wf_good_file")
    task = task_template.create_task(
        arg="sleep 1",
        name="good_callable_task",
        compute_resources_callable=resource_file_does_exist,
    )
    workflow.add_task(task)
    workflow.bind()
    workflow._distributor_proc = MockDistributorProc()
    wfr = workflow._create_workflow_run()

    # Move workflow and wfr through Instantiating -> Launched
    wfr._update_status(WorkflowRunStatus.INSTANTIATING)
    wfr._update_status(WorkflowRunStatus.LAUNCHED)

    # swarm calls
    swarm = SwarmWorkflowRun(
        workflow_id=workflow.workflow_id,
        workflow_run_id=wfr.workflow_run_id,
        tasks=list(workflow.tasks.values()),
        requester=workflow.requester,
    )

    try:
        workflow._run_swarm(swarm, seconds_until_timeout=1)
    except RuntimeError:
        pass
    assert swarm.swarm_tasks[task.task_id].task_resources.id is not None


def test_callable_returns_wrong_object(tool, task_template):
    """test that the callable cannot return an invalid object"""

    def wrong_return_params(*args, **kwargs):
        wrong_format = ["1G", 60, 1]
        return wrong_format

    task = task_template.create_task(
        arg="sleep 1",
        name="good_callable_task",
        compute_resources_callable=wrong_return_params,
    )
    wf = tool.create_workflow(workflow_args="dynamic_resource_wf_wrong_param_obj")
    wf.add_task(task)
    with pytest.raises(CallableReturnedInvalidObject):
        wf.run()


def test_callable_fails_bad_filepath(tool, task_template):
    """test that an exception in the callable gets propagated up the call stack"""

    def resource_filepath_does_not_exist(*args, **kwargs):
        fp = os.path.join(this_dir, "file_that_does_not_exist.txt")
        file = open(fp, "r")
        file.read()

    task = task_template.create_task(
        name="bad_callable_wrong_file",
        arg="sleep 1",
        compute_resources_callable=resource_filepath_does_not_exist,
    )
    wf = tool.create_workflow(workflow_args="dynamic_resource_wf_bad_file")
    wf.add_task(task)
    with pytest.raises(FileNotFoundError):
        wf.run()
