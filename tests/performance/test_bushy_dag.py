import copy
import getpass
import time
import uuid
from typing import List

# for monkey patch
from jobmon.client.swarm.swarm_task import SwarmTask
from jobmon.constants import TaskStatus, WorkflowRunStatus

import pytest

import structlog as logging

logger = logging.getLogger(__name__)


def mock_all_upstreams_done(self):
    """Return a bool of if upstreams are done or not"""
    logger.debug("Using monkeypatch for all_upstreams_done.")
    return all([u.is_done for u in self.upstream_tasks])


def mock_propagate_results(self, swarm_task: SwarmTask) -> List[SwarmTask]:
    new_fringe: List[SwarmTask] = []
    logger.debug(f"Propagate {swarm_task}")
    for downstream in swarm_task.downstream_swarm_tasks:
        logger.debug(f"downstream {downstream}")
        downstream_done = downstream.status == TaskStatus.DONE
        downstream.num_upstreams_done += 1
        if not downstream_done and downstream.status == TaskStatus.REGISTERED:
            time_start = time.time()
            all_upstreams_done = downstream.all_upstreams_done
            time_end = time.time()
            logger.debug(f"all_upstreams_done time: {time_end - time_start}")
            if all_upstreams_done:
                logger.debug(" and add to fringe")
                new_fringe += [downstream]  # make sure there's no dups
            else:
                # don't do anything, task not ready yet
                logger.debug(" not ready yet")
        else:
            logger.debug(
                f" not ready yet or already queued, Status is {downstream.status}"
            )
    return new_fringe


def mock_execute(
    self,
    fail_fast: bool = False,
    seconds_until_timeout: int = 36000,
    wedged_workflow_sync_interval: int = 600,
):

    self.update_status(WorkflowRunStatus.RUNNING)
    self.last_sync = self._get_current_time()
    self._parse_adjusting_done_and_errors(list(self.swarm_tasks.values()))
    previously_completed = copy.copy(self.all_done)  # for reporting
    fringe = self._compute_fringe()
    logger.debug(f"fail_after_n_executions is {self._val_fail_after_n_executions}")
    n_executions = 0
    logger.info(f"Executing Workflow Run {self.workflow_run_id}")
    while fringe or self.active_tasks:
        while fringe:
            swarm_task = fringe.pop()
            if swarm_task.is_done:
                raise RuntimeError("Invalid DAG. Encountered a DONE node")
            else:
                logger.debug(
                    f"Instantiating resources for newly ready  task and "
                    f"changing it to the queued state. Task: {swarm_task},"
                    f" id: {swarm_task.task_id}"
                )
                self._adjust_resources_and_queue(swarm_task)
        completed, failed = self._block_until_any_done_or_error(
            timeout=seconds_until_timeout,
            wedged_workflow_sync_interval=wedged_workflow_sync_interval,
        )
        for swarm_task in completed:
            n_executions += 1
        if failed and fail_fast:
            break  # fail out early
        logger.debug(
            f"Return from blocking call, completed: "
            f"{[t.task_id for t in completed]}, "
            f"failed:{[t.task_id for t in failed]}"
        )
        for swarm_task in completed:
            start_time = time.time()  # added for test
            task_to_add = self._propagate_results(swarm_task)
            end_time = time.time()  # added for test
            logger.debug(
                f"propagate results took: {end_time - start_time}"
            )  # added for test
            fringe = list(set(fringe + task_to_add))
        if (
            self._val_fail_after_n_executions is not None
            and n_executions >= self._val_fail_after_n_executions
        ):
            raise ValueError(
                f"Dag asked to fail after {n_executions} " f"executions. Failing now"
            )

    all_completed = self.all_done
    num_new_completed = len(all_completed) - len(previously_completed)
    all_failed = self.all_error
    if all_failed:
        if fail_fast:
            logger.info("Failing after first failure, as requested")
        logger.info(f"DAG execute ended, failed {all_failed}")
        self.update_status(WorkflowRunStatus.ERROR)
        self._completed_report = (num_new_completed, len(previously_completed))
    else:
        logger.info(f"DAG execute finished successfully, " f"{num_new_completed} jobs")
        self.update_status(WorkflowRunStatus.DONE)
        self._completed_report = (num_new_completed, len(previously_completed))


@pytest.mark.performance_tests
def test_bushy_dag(db_cfg, client_env, monkeypatch):
    """
    create workflow with 1000 task with 1000 dependant tasks to get a perfomance metrics.
    """
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow as Workflow
    from jobmon.client.templates.bash_task import BashTask
    from jobmon.client.distributor.strategies.base import ExecutorParameters
    from jobmon.client.swarm.workflow_run import WorkflowRun

    monkeypatch.setattr(WorkflowRun, "_execute", mock_execute)
    monkeypatch.setattr(WorkflowRun, "_propagate_results", mock_propagate_results)

    # declaring app to enforce loading db config
    app = db_cfg["app"]  # noqa
    n_jobs = 1000
    wfid = uuid.uuid4()
    user = getpass.getuser()
    wf = Workflow(
        f"bushy_dag_{wfid}",
        "bushy_dag_test",
        executor_class="SGEExecutor",
        stderr=f"/ihme/scratch/users/{user}/tests/bushy_dag_test/{wfid}",
        stdout=f"/ihme/scratch/users/{user}/tests/bushy_dag_test/{wfid}",
        project="proj_scicomp",
    )

    params = ExecutorParameters(
        m_mem_free="128M", num_cores=1, queue="all.q", max_runtime_seconds=20
    )

    tier1 = []
    # First Tier
    for i in range(n_jobs):
        uid = str(uuid.uuid4())
        tier_1_task = BashTask(
            f"echo {uid}", executor_parameters=params, executor_class="SGEExecutor"
        )
        tier1.append(tier_1_task)

    tier2 = []
    # Second Tier, depend on 1 tier 1 task
    for i in range(n_jobs):
        uid = str(uuid.uuid4())
        tier_2_task = BashTask(
            f"echo {uid}",
            upstream_tasks=tier1,
            executor_parameters=params,
            executor_class="SGEExecutor",
        )
        tier2.append(tier_2_task)

    wf.add_tasks(tier1 + tier2)
    wfr = wf.run()

    assert len(wfr.all_error) == 0


@pytest.mark.performance_tests
def test_bushy_dag_prev(db_cfg, client_env, monkeypatch):
    """
    create workflow with 1000 task with 1000 dependant tasks to get a perfomance metrics.
    """
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow as Workflow
    from jobmon.client.templates.bash_task import BashTask
    from jobmon.client.distributor.strategies.base import ExecutorParameters
    from jobmon.client.swarm.workflow_run import WorkflowRun
    from jobmon.client.swarm.swarm_task import SwarmTask

    monkeypatch.setattr(WorkflowRun, "_execute", mock_execute)
    monkeypatch.setattr(WorkflowRun, "_propagate_results", mock_propagate_results)
    monkeypatch.setattr(
        SwarmTask, "all_upstreams_done", property(mock_all_upstreams_done)
    )

    # declaring app to enforce loading db config
    app = db_cfg["app"]  # noqa
    n_jobs = 1000
    wfid = uuid.uuid4()
    user = getpass.getuser()
    wf = Workflow(
        f"bushy_dag_{wfid}",
        "bushy_dag_test",
        executor_class="SGEExecutor",
        stderr=f"/ihme/scratch/users/{user}/tests/bushy_dag_test/{wfid}",
        stdout=f"/ihme/scratch/users/{user}/tests/bushy_dag_test/{wfid}",
        project="proj_scicomp",
    )

    params = ExecutorParameters(
        m_mem_free="128M", num_cores=1, queue="all.q", max_runtime_seconds=20
    )

    tier1 = []
    # First Tier
    for i in range(n_jobs):
        uid = str(uuid.uuid4())
        tier_1_task = BashTask(
            f"echo {uid}", executor_parameters=params, executor_class="SGEExecutor"
        )
        tier1.append(tier_1_task)

    tier2 = []
    # Second Tier, depend on 1 tier 1 task
    for i in range(n_jobs):
        uid = str(uuid.uuid4())
        tier_2_task = BashTask(
            f"echo {uid}",
            upstream_tasks=tier1,
            executor_parameters=params,
            executor_class="SGEExecutor",
        )
        tier2.append(tier_2_task)

    wf.add_tasks(tier1 + tier2)
    wfr = wf.run()

    assert len(wfr.all_error) == 0
