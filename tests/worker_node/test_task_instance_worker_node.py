import os
import random

import pytest

from jobmon import __version__
from jobmon.requester import Requester
from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun
from jobmon.client.distributor.distributor_array import DistributorArray
from jobmon.client.distributor.distributor_service import DistributorService
from jobmon.client.distributor.distributor_task import DistributorTask
from jobmon.client.distributor.distributor_workflow_run import DistributorWorkflowRun
from jobmon.cluster_type.dummy import DummyDistributor
from jobmon.cluster_type.sequential.seq_distributor import SequentialDistributor
from jobmon.constants import TaskInstanceStatus
from jobmon.worker_node.worker_node_task_instance import WorkerNodeTaskInstance


def test_seq_kill_self_state():
    """
    mock the error status
    """
    expected_words = "job was in kill self state"
    executor = SequentialDistributor()
    executor._exit_info = {1: 199}
    r_value, r_msg = executor.get_remote_exit_info(1)
    assert r_value == TaskInstanceStatus.UNKNOWN_ERROR
    assert expected_words in r_msg


class DoNothingDistributor(DummyDistributor):
    def submit_to_batch_distributor(
        self, command: str, name: str, requested_resources
    ) -> int:
        distributor_id = random.randint(1, int(1e7))
        return distributor_id


@pytest.mark.parametrize(
    "ti_state", [TaskInstanceStatus.UNKNOWN_ERROR, TaskInstanceStatus.KILL_SELF]
)
def test_ti_kill_self_state(db_cfg, tool, ti_state):
    """should try to log a report by date after being set to the U or K state
    and fail"""

    workflow = tool.create_workflow(name=f"test_ti_kill_self_state_{ti_state}")
    task_a = tool.active_task_templates["simple_template"].create_task(arg="sleep 120")
    workflow.add_task(task_a)

    # bind workflow to db
    workflow.bind()
    wfr = workflow._create_workflow_run()

    # move workflow to launched state
    distributor = DoNothingDistributor()
    distributor_service = DistributorService(
        workflow.workflow_id,
        wfr.workflow_run_id,
        distributor,
        requester=workflow.requester,
    )

    # swarm calls
    swarm = SwarmWorkflowRun(
        workflow_id=workflow.workflow_id,
        workflow_run_id=wfr.workflow_run_id,
        tasks=list(workflow.tasks.values()),
        requester=workflow.requester,
    )
    swarm.compute_initial_dag_state()
    list(swarm.queue_tasks())

    # launch task on executor
    distributor_service.distributor.start()
    distributor_task = distributor_service._get_tasks_queued_for_instantiation()[0]
    distributor_task_instance = distributor_service._create_task_instance(
        distributor_task
    )

    # Bring in the worker node here since dummy executor is never run
    worker_node_task_instance = WorkerNodeTaskInstance(
        task_instance_id=distributor_task_instance.task_instance_id,
        expected_jobmon_version=__version__,
        cluster_type_name=distributor.cluster_type_name,
    )

    # Log running
    _, _, _ = worker_node_task_instance.log_running(1)

    # set task to kill self state. next heartbeat will fail and cause death
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        DB.session.execute(
            """
            UPDATE task_instance
            SET status = '{}'
            WHERE task_instance.task_id = {}
            """.format(
                ti_state, task_a.task_id
            )
        )
        DB.session.commit()

    assert worker_node_task_instance.in_kill_self_state()


def test_array_task_instance(tool, db_cfg, client_env, array_template):
    """Tests that the worker node is compatible with array task instances."""
    array1 = array_template.create_array(arg=[1, 2, 3], cluster_name="sequential",
                                         compute_resources={"queue": "null.q"})

    workflow = tool.create_workflow(name="test_array_ti_selection")

    workflow.add_array(array1)
    workflow.bind()
    wfr = workflow._create_workflow_run()

    # Create distributor tasks
    requester = Requester(client_env)
    dts = [
        DistributorTask(task_id=t.task_id,
                        array_id=array1.array_id,
                        name='array_ti',
                        command=t.command,
                        requested_resources=t.compute_resources,
                        requester=requester)
        for t in array1.tasks.values()
    ]

    # Move all tasks to Q state
    for tid in (t.task_id for t in array1.tasks.values()):
        _, _ = requester._send_request(
            app_route=f"/task/{tid}/queue",
            message={},
            request_type='post'
        )

    # Register TIs
    dtis = [
        dt.register_task_instance(workflow_run_id=wfr.workflow_run_id)
        for dt in dts
    ]

    # Append on batch number
    dist_array = DistributorArray(array1.array_id,
                                  array1.task_resources.id,
                                  array1.compute_resources,
                                  requester)
    dist_array.instantiated_array_task_instance_ids = [dti.task_instance_id for dti in dtis]
    assert dist_array.batch_number == 0
    dist_array.add_batch_number_to_task_instances()
    assert dist_array.batch_number == 1

    # Call the array method for all tasks and ensure full coverage of task instance IDs
    # Indices are offset by 1 since the clusters will submit using a 1:N range strategy
    task_instance_ids = set()
    for i in range(1, len(dtis) + 1):
        _, resp = requester._send_request(
            app_route=f"/get_array_task_instance_id/{array1.array_id}/0/{i}",
            message={},
            request_type='get'
        )
        assert resp['task_instance_id'] not in task_instance_ids
        task_instance_ids.add(resp['task_instance_id'])

    assert task_instance_ids == set([dti.task_instance_id for dti in dtis])

    # Test array worker node functionality
    from jobmon import __version__
    # Mock a value, doesn't matter. Worker node inherits from the environment.
    os.environ['JOB_ID'] = "1"
    w = WorkerNodeTaskInstance(task_instance_id=None,
                               expected_jobmon_version=__version__,
                               cluster_type_name='sequential',
                               array_id=array1.array_id,
                               batch_number=0)

    # Check that task instance ID property set correctly
    tid = w.task_instance_id
    assert tid == min(task_instance_ids)

    proc_return_code = w.run(heartbeat_interval=90, report_by_buffer=3.1)
    assert proc_return_code == 0

    app, DB = db_cfg['app'], db_cfg['DB']

    with app.app_context():
        q = f"""
        SELECT status
        FROM task_instance
        WHERE id = {tid}"""

        task_instance = DB.session.execute(q).one()
        DB.session.commit()
        assert task_instance.status == "D"

    # Instantiate a second worker node instance, should correspond to the second TID
    w2 = WorkerNodeTaskInstance(task_instance_id=None,
                                expected_jobmon_version=__version__,
                                cluster_type_name='sequential',
                                array_id=array1.array_id,
                                batch_number=0)
    assert w2.executor.array_step_id == 2
    assert w2.task_instance_id == sorted(list(task_instance_ids))[1]
