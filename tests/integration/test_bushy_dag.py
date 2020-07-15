import getpass
import pytest
import uuid


@pytest.mark.integration_tests
def test_bushy_dag(db_cfg, client_env):
    """
    create workflow with 1000 task with 1000 dependant tasks to get a perfomance metrics.
    """
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow as Workflow
    from jobmon.client.templates.bash_task import BashTask
    # declaring app to enforce loading db config
    app = db_cfg["app"]
    n_jobs = 10
    wfid = uuid.uuid4()
    user = getpass.getuser()
    wf = Workflow(f"bushy_dag_{wfid}", "bushy_dag_test",
                  executor_class = 'DummyExecutor',
                  stderr=f"/ihme/scratch/users/{user}/tests/bushy_dag_test/{wfid}",
                  stdout=f"/ihme/scratch/users/{user}/tests/bushy_dag_test/{wfid}",
                  project="proj_scicomp")

    tier1 = []
    # First Tier
    for i in range(n_jobs):
        uid = str(uuid.uuid4())
        tier_1_task = BashTask(f"echo {uid}", num_cores=1)
        tier1.append(tier_1_task)

    tier2 = []
    # Second Tier, depend on 1 tier 1 task
    for i in range(n_jobs):
        uid = str(uuid.uuid4())
        tier_2_task = BashTask(f"echo {uid}",
                               upstream_tasks=tier1, num_cores=1)
        tier2.append(tier_2_task)

    wf.add_tasks(tier1 + tier2)
    wfr = wf.run()
    
    import pdb
    pdb.set_trace()

    assert len(wfr.all_error) == 0
