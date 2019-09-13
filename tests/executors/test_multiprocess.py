import time

import pytest

from jobmon.client.workflow.bash_task import BashTask
from jobmon.client.workflow.workflow import Workflow
from jobmon.execution.scheduler import JobInstanceScheduler
from jobmon.execution.strategies.multiprocess import MultiprocessExecutor


def run_scheduler(host, port):
    from jobmon.requester import shared_requester
    from jobmon.connection_config import ConnectionConfig

    cc = ConnectionConfig(host=host, port=port)
    shared_requester.url = cc.url

    executor = MultiprocessExecutor(parallelism=2)
    scheduler = JobInstanceScheduler(executor)
    scheduler.run_scheduler()


@pytest.fixture(scope='function')
def mp_scheduler(test_session_config):
    import multiprocessing as mp
    ctx = mp.get_context('spawn')
    p1 = ctx.Process(target=run_scheduler,
                     args=(test_session_config["JOBMON_HOST"],
                           test_session_config["JOBMON_PORT"]))
    p1.start()
    time.sleep(10)
    yield

    p1.terminate()


def test_foo(real_jsm_jqs, db_cfg, mp_scheduler, ephemera_conn_str):
    t1 = BashTask("sleep 1", executor_class="MultiprocessExecutor")
    t2 = BashTask("erroring_out 1", upstream_tasks=[t1],
                  executor_class="MultiprocessExecutor",
                  max_attempts=1)
    t3 = BashTask("sleep 10", upstream_tasks=[t1],
                  executor_class="MultiprocessExecutor")
    t4 = BashTask("sleep 11", upstream_tasks=[t3],
                  executor_class="MultiprocessExecutor")
    t5 = BashTask("sleep 12", upstream_tasks=[t4],
                  executor_class="MultiprocessExecutor")

    workflow = Workflow("foo", executor_class="MultiprocessExecutor")
    workflow.add_tasks([t1, t2, t3, t4, t5])
    workflow.execute()

    assert len(workflow.task_dag.job_list_manager.all_error) == 1
    assert len(workflow.task_dag.job_list_manager.all_done) == 4
