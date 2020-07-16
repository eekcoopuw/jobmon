import getpass
import pytest
import uuid

from jobmon.client import ClientLogging as logging
logger = logging.getLogger(__name__)


@pytest.mark.integration_tests
def test_bushy_dag(db_cfg, client_env):
    """
    create workflow with 1000 task with 1000 dependant tasks to get a perfomance metrics.
    """
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow as Workflow
    from jobmon.client.templates.bash_task import BashTask
    from jobmon.client.execution.strategies.base import ExecutorParameters
    # declaring app to enforce loading db config
    app = db_cfg["app"]
    n_jobs = 1000
    wfid = uuid.uuid4()
    user = getpass.getuser()
    wf = Workflow(f"bushy_dag_{wfid}", "bushy_dag_test",
                  executor_class = 'SGEExecutor',
                  stderr=f"/ihme/scratch/users/{user}/tests/bushy_dag_test/{wfid}",
                  stdout=f"/ihme/scratch/users/{user}/tests/bushy_dag_test/{wfid}",
                  project="proj_scicomp")

    params = ExecutorParameters(m_mem_free="128M", 
                                num_cores=1,
                                queue="all.q",
                                max_runtime_seconds=20)

    tier1 = []
    # First Tier
    for i in range(n_jobs):
        uid = str(uuid.uuid4())
        tier_1_task = BashTask(f"echo {uid}", executor_parameters=params, executor_class = 'SGEExecutor')
        tier1.append(tier_1_task)

    tier2 = []
    # Second Tier, depend on 1 tier 1 task
    for i in range(n_jobs):
        uid = str(uuid.uuid4())
        tier_2_task = BashTask(f"echo {uid}",
                               upstream_tasks=tier1,
                               executor_parameters=params,
                               executor_class = 'SGEExecutor')
        tier2.append(tier_2_task)

    wf.add_tasks(tier1 + tier2)
    wfr = wf.run()
    
    assert len(wfr.all_error) == 0



# # monkey patching with previous code (without counter)
# from jobmon.client.swarm.swarm_task import SwarmTask
# from jobmon.client.swarm.workflow_run import WorkflowRun
# class MockWorkflowRun(WorkflowRun):
#     def _propagate_results(self, swarm_task: SwarmTask):
#         new_fringe = []
#         logger.debug(f"Propagate {swarm_task}")
#         for downstream in swarm_task.downstream_swarm_tasks:
#             logger.debug(f"downstream {downstream}")
#             downstream_done = (downstream.status == TaskStatus.DONE)
#             if (not downstream_done and
#                     downstream.status == TaskStatus.REGISTERED):
#                 if downstream.all_upstreams_done:
#                     logger.debug(" and add to fringe")
#                     new_fringe += [downstream]  # make sure there's no dups
#                 else:
#                     # don't do anything, task not ready yet
#                     logger.debug(" not ready yet")
#             else:
#                 logger.debug(f" not ready yet or already queued, Status is "
#                              f"{downstream.status}")
#         return new_fringe

from jobmon.client.swarm.swarm_task import SwarmTask
class MockSwarmTask(SwarmTask):
    @property
    def all_upstreams_done(self):
        """Return a bool of if upstreams are done or not"""
        return all([u.is_done for u in self.upstream_tasks])
        

@pytest.mark.integration_tests
def test_bushy_dag_prev(db_cfg, client_env, monkeypatch):
    """
    create workflow with 1000 task with 1000 dependant tasks to get a perfomance metrics.
    """
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow as Workflow
    from jobmon.client.templates.bash_task import BashTask
    # import jobmon.client.swarm.workflow_run
    import jobmon.client.swarm.swarm_task

    from jobmon.client.execution.strategies.base import ExecutorParameters

    # monkey patching
    # monkeypatch.setattr(
    #     jobmon.client.swarm.workflow_run,
    #     "WorkflowRun",
    #     MockWorkflowRun)
    monkeypatch.setattr(
        jobmon.client.swarm.swarm_task,
        "SwarmTask",
        MockSwarmTask)

    # declaring app to enforce loading db config
    app = db_cfg["app"]
    n_jobs = 1000
    wfid = uuid.uuid4()
    user = getpass.getuser()
    wf = Workflow(f"bushy_dag_{wfid}", "bushy_dag_test",
                  executor_class = 'SGEExecutor',
                  stderr=f"/ihme/scratch/users/{user}/tests/bushy_dag_test/{wfid}",
                  stdout=f"/ihme/scratch/users/{user}/tests/bushy_dag_test/{wfid}",
                  project="proj_scicomp")

    params = ExecutorParameters(m_mem_free="128M", 
                                num_cores=1,
                                queue="all.q",
                                max_runtime_seconds=20)

    tier1 = []
    # First Tier
    for i in range(n_jobs):
        uid = str(uuid.uuid4())
        tier_1_task = BashTask(f"echo {uid}", executor_parameters=params, executor_class = 'SGEExecutor')
        tier1.append(tier_1_task)

    tier2 = []
    # Second Tier, depend on 1 tier 1 task
    for i in range(n_jobs):
        uid = str(uuid.uuid4())
        tier_2_task = BashTask(f"echo {uid}",
                               upstream_tasks=tier1,
                               executor_parameters=params,
                               executor_class = 'SGEExecutor')
        tier2.append(tier_2_task)

    wf.add_tasks(tier1 + tier2)
    wfr = wf.run()
    
    assert len(wfr.all_error) == 0
