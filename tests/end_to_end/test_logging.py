import time

from jobmon.client.distributor.distributor_service import DistributorService
from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun
from jobmon.builtins.sequential.seq_distributor import SequentialDistributor
from jobmon.builtins.multiprocess.multiproc_distributor import MultiprocessDistributor

from jobmon.constants import TaskInstanceStatus


def test_sequential_logging(tool, task_template, tmp_path):
    """create a 1 task workflow and confirm it works end to end"""

    workflow = tool.create_workflow(name="test_sequential_logging",
                                    default_cluster_name="sequential")
    t1 = task_template.create_task(arg="echo 'hello world'", name="stdout_task",
                                   compute_resources={"stdout": f"{str(tmp_path)}"})
    t2 = task_template.create_task(arg="foobar", name="stderr_task",
                                   compute_resources={"stderr": f"{str(tmp_path)}"})
    workflow.add_tasks([t1, t2])
    workflow.bind()
    wfr = workflow._create_workflow_run()

    # create task instances
    swarm = SwarmWorkflowRun(
        workflow_run_id=wfr.workflow_run_id, requester=workflow.requester
    )
    swarm.from_workflow(workflow)
    swarm.set_initial_fringe()
    swarm.process_commands()

    # test that we can launch via the normal job pathway
    distributor_service = DistributorService(
        SequentialDistributor("sequential"),
        requester=workflow.requester,
        raise_on_error=True,
    )
    distributor_service.set_workflow_run(wfr.workflow_run_id)
    distributor_service.refresh_status_from_db(TaskInstanceStatus.QUEUED)
    distributor_service.process_status(TaskInstanceStatus.QUEUED)

    distributor_service.refresh_status_from_db(TaskInstanceStatus.INSTANTIATED)
    distributor_service.process_status(TaskInstanceStatus.INSTANTIATED)

    with open(tmp_path / "stdout_task.o1") as f:
        assert "hello world\n" in f.readlines()

    with open(tmp_path / "stderr_task.e2") as f:
        assert f.readline().rstrip() == "/bin/sh: foobar: command not found"


def test_multiprocess_logging(tool, task_template, tmp_path):
    """create a 1 task workflow and confirm it works end to end"""

    workflow = tool.create_workflow(
        name="test_multiprocess_logging",
        default_cluster_name="multiprocess",
        default_compute_resources_set={"multiprocess": {"queue": "null.q"}}
    )
    t1 = task_template.create_task(arg="echo 'hello world'", name="stdout_task",
                                   compute_resources={"stdout": f"{str(tmp_path)}"})
    t2 = task_template.create_task(arg="foobar", name="stderr_task",
                                   compute_resources={"stderr": f"{str(tmp_path)}"})
    workflow.add_tasks([t1, t2])
    workflow.bind()
    wfr = workflow._create_workflow_run()

    # create task instances
    swarm = SwarmWorkflowRun(
        workflow_run_id=wfr.workflow_run_id, requester=workflow.requester
    )
    swarm.from_workflow(workflow)
    swarm.set_initial_fringe()
    swarm.process_commands()

    # test that we can launch via the normal job pathway
    distributor_service = DistributorService(
        MultiprocessDistributor("multiprocess", parallelism=2),
        requester=workflow.requester,
        raise_on_error=True,
    )
    distributor_service.cluster_interface.start()
    distributor_service.set_workflow_run(wfr.workflow_run_id)
    distributor_service.refresh_status_from_db(TaskInstanceStatus.QUEUED)
    distributor_service.process_status(TaskInstanceStatus.QUEUED)

    distributor_service.refresh_status_from_db(TaskInstanceStatus.INSTANTIATED)
    distributor_service.process_status(TaskInstanceStatus.INSTANTIATED)

    counter = 0
    while distributor_service.cluster_interface.get_submitted_or_running():
        time.sleep(1)
        counter += 1
        if counter > 10:
            break

    distributor_service.cluster_interface.stop()

    with open(tmp_path / "stdout_task.o1_0") as f:
        assert "hello world\n" in f.readlines()

    with open(tmp_path / "stderr_task.e2_0") as f:
        assert f.readline().rstrip() == "/bin/sh: foobar: command not found"
