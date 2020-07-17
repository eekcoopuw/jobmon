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


# for monkey patch
def mock_all_upstreams_done(self):
    """Return a bool of if upstreams are done or not"""
    logger.debug("Using monkeypatch for all_upstreams_done.")
    return all([u.is_done for u in self.upstream_tasks])
        

@pytest.mark.integration_tests
def test_bushy_dag_prev(db_cfg, client_env, monkeypatch):
    """
    create workflow with 1000 task with 1000 dependant tasks to get a perfomance metrics.
    """
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow as Workflow
    from jobmon.client.templates.bash_task import BashTask
    from jobmon.client.swarm.swarm_task import SwarmTask
    from jobmon.client.execution.strategies.base import ExecutorParameters

    monkeypatch.setattr(
        SwarmTask,
        "all_upstreams_done",
        property(mock_all_upstreams_done)
    )

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
