import os
import random
from typing import Dict, Tuple

from unittest.mock import patch
from sqlalchemy import select

from jobmon.constants import TaskInstanceStatus
from jobmon.client.distributor.distributor_service import DistributorService
from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun
from jobmon.cluster import Cluster
from jobmon.builtins.dummy import DummyDistributor
from jobmon.builtins.multiprocess.multiproc_distributor import MultiprocessDistributor
from jobmon.builtins.sequential.seq_distributor import SequentialDistributor
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.worker_node.worker_node_task_instance import WorkerNodeTaskInstance
from jobmon.worker_node.start import get_worker_node_task_instance


class DoNothingDistributor(DummyDistributor):
    def submit_to_batch_distributor(
        self, command: str, name: str, requested_resources
    ) -> Tuple[str, str, str]:
        distributor_id = random.randint(1, int(1e7))
        return str(distributor_id), "/foo", "/bar"


class DoNothingArrayDistributor(MultiprocessDistributor):
    def submit_array_to_batch_distributor(
        self,
        command: str,
        name: str,
        requested_resources,
        array_length: int,
    ) -> Dict[int, Tuple[str, str, str]]:
        job_id = random.randint(1, int(1e7))
        mapping: Dict[int, Tuple[str, str, str]] = {}
        for array_step_id in range(0, array_length):
            distributor_id = self._get_subtask_id(job_id, array_step_id)
            mapping[array_step_id] = distributor_id, "/foo", "/bar"

        return mapping


def test_task_instance(db_cfg, tool):
    """should try to log a report by date after being set to the U or K state
    and fail"""
    from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun
    from jobmon.client.distributor.distributor_service import DistributorService

    workflow = tool.create_workflow(name="test_ti_kill_self_state")
    task_a = tool.active_task_templates["simple_template"].create_task(arg="echo 1")
    workflow.add_task(task_a)
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
        DoNothingDistributor("dummy"), requester=workflow.requester, raise_on_error=True
    )
    distributor_service.set_workflow_run(wfr.workflow_run_id)
    distributor_service.refresh_status_from_db(TaskInstanceStatus.QUEUED)
    distributor_service.process_status(TaskInstanceStatus.QUEUED)
    distributor_service.refresh_status_from_db(TaskInstanceStatus.INSTANTIATED)
    distributor_service.process_status(TaskInstanceStatus.INSTANTIATED)

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        task_instance_id_query = select(TaskInstance.id).where(
            TaskInstance.task_id == task_a.task_id
        )

        task_instance_id = DB.session.execute(task_instance_id_query).scalar()
        DB.session.commit()

    worker_node_task_instance = get_worker_node_task_instance(
        task_instance_id=task_instance_id,
        cluster_name="dummy"
    )
    worker_node_task_instance.run()
    assert worker_node_task_instance.status == TaskInstanceStatus.DONE
    assert worker_node_task_instance.command_return_code == 0


def test_array_task_instance(tool, db_cfg, client_env, array_template, monkeypatch):
    """Tests that the worker node is compatible with array task instances."""

    array1 = array_template.create_array(
        arg=[1, 2, 3], cluster_name="sequential", compute_resources={"queue": "null.q"}
    )
    workflow = tool.create_workflow(name="test_array_ti_selection")
    workflow.add_array(array1)
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
        DoNothingArrayDistributor("multiprocess"), requester=workflow.requester,
        raise_on_error=True
    )
    distributor_service.set_workflow_run(wfr.workflow_run_id)
    distributor_service.refresh_status_from_db(TaskInstanceStatus.QUEUED)
    distributor_service.process_status(TaskInstanceStatus.QUEUED)
    distributor_service.refresh_status_from_db(TaskInstanceStatus.INSTANTIATED)
    distributor_service.process_status(TaskInstanceStatus.INSTANTIATED)

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        task_instance_id_query = select(
            TaskInstance.distributor_id, TaskInstance.array_batch_num,
            TaskInstance.stdout, TaskInstance.stderr
        ).where(TaskInstance.array_id == array1.array_id)
        distributor_ids = DB.session.execute(task_instance_id_query).all()
        DB.session.commit()

    # Check the filepaths are logged correctly
    for *_, stdout, stderr in distributor_ids:
        assert stdout == '/foo'
        assert stderr == '/bar'

    for distributor_id, array_batch_num, *_ in distributor_ids:

        job_id, step_id = distributor_id.split(".")
        monkeypatch.setenv("JOB_ID", job_id)
        monkeypatch.setenv("ARRAY_STEP_ID", step_id)

        wnti = get_worker_node_task_instance(
            cluster_name="multiprocess",
            array_id=array1.array_id,
            batch_number=array_batch_num,
        )
        wnti.run()

        assert wnti.status == TaskInstanceStatus.DONE
        assert wnti.command_return_code == 0


def test_ti_kill_self_state(db_cfg, tool):
    """should try to log a report by date after being set to the U or K state
    and fail"""

    workflow = tool.create_workflow(name="test_ti_kill_self_state")
    task_a = tool.active_task_templates["simple_template"].create_task(arg="sleep 120")
    workflow.add_task(task_a)
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
        DoNothingDistributor("dummy"), requester=workflow.requester, raise_on_error=True
    )
    distributor_service.set_workflow_run(wfr.workflow_run_id)
    distributor_service.refresh_status_from_db(TaskInstanceStatus.QUEUED)
    distributor_service.process_status(TaskInstanceStatus.QUEUED)
    distributor_service.refresh_status_from_db(TaskInstanceStatus.INSTANTIATED)
    distributor_service.process_status(TaskInstanceStatus.INSTANTIATED)

    # Bring in the worker node here since dummy executor is never run

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        task_instance_id_query = select(TaskInstance.id).where(
            TaskInstance.task_id == task_a.task_id
        )

        task_instance_id = DB.session.execute(task_instance_id_query).scalar()
        DB.session.commit()

    cluster = Cluster.get_cluster("dummy")
    worker_node_task_instance = WorkerNodeTaskInstance(
        task_instance_id=task_instance_id,
        cluster_interface=cluster.get_worker_node(),
        heartbeat_interval=5,
    )

    # Log running
    worker_node_task_instance.log_running()

    # patch so log_running does nothing and then put in K state. we should fail into error
    # error state
    worker_node = "jobmon.worker_node."
    WNTI = "worker_node_task_instance.WorkerNodeTaskInstance."
    with patch(worker_node + WNTI + "log_running") as m_run:
        m_run.return_value = None

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
                    TaskInstanceStatus.KILL_SELF, task_a.task_id
                )
            )
            DB.session.commit()

        worker_node_task_instance.run()

    assert worker_node_task_instance.status == TaskInstanceStatus.ERROR_FATAL


def test_limited_error_log(tool, db_cfg):

    thisdir = os.path.dirname(os.path.realpath(os.path.expanduser(__file__)))

    wf = tool.create_workflow(name="random_workflow")
    template = tool.get_task_template(
        template_name="some_template",
        command_template="python {node_arg}",
        node_args=["node_arg"],
    )
    task_resources = {
        "num_cores": 1,
        "mem": "1G",
        "max_runtime_seconds": 600,
        "queue": "null.q",
    }
    task = template.create_task(
        name="task1",
        node_arg=os.path.join(thisdir, "fill_pipe.py"),
        compute_resources=task_resources,
        cluster_name="sequential",
        max_attempts=1,
    )
    wf.add_tasks([task])
    wf.bind()
    wfr = wf._create_workflow_run()

    # create task instances
    swarm = SwarmWorkflowRun(
        workflow_run_id=wfr.workflow_run_id,
        requester=wf.requester,
    )
    swarm.from_workflow(wf)
    swarm.set_initial_fringe()
    swarm.process_commands()

    distributor_service = DistributorService(
        SequentialDistributor("sequential"), requester=wf.requester, raise_on_error=True
    )
    distributor_service.set_workflow_run(wfr.workflow_run_id)
    distributor_service.refresh_status_from_db(TaskInstanceStatus.QUEUED)
    distributor_service.process_status(TaskInstanceStatus.QUEUED)
    distributor_service.refresh_status_from_db(TaskInstanceStatus.INSTANTIATED)
    distributor_service.process_status(TaskInstanceStatus.INSTANTIATED)

    # check db
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        query = (
            "SELECT description "
            "FROM task_instance_error_log t1, task_instance t2, workflow_run t3 "
            "WHERE t1.task_instance_id=t2.id "
            "AND t2.workflow_run_id=t3.id "
            "AND t3.workflow_id={}".format(wf.workflow_id)
        )
        res = DB.session.execute(query).fetchone()
        DB.session.commit()

    error = res[0]
    assert error == (("a" * 2**10 + "\n") * (2**8))[-10000:]
