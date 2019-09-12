import pytest

from jobmon.client.swarm.workflow.bash_task import BashTask
from jobmon.client.swarm.workflow.workflow import Workflow

from jobmon.scheduler import Scheduler
from jobmon.scheduler.executors.sequential import SequentialExecutor


def test_scheduler_up(real_jsm_jqs, db_cfg, ephemera_conn_str):
    executor = SequentialExecutor()
    scheduler = Scheduler(executor=executor)

    t1 = BashTask("echo 1", executor_class="SequentialExecutor")
    workflow = Workflow("my_simple_dag", executor_class="SequentialExecutor",
                        seconds_until_timeout=1)
    workflow.add_tasks([t1])

    with pytest.raises(RuntimeError):
        workflow.run()

    scheduler.instantiate_queued_jobs()
    import pdb; pdb.set_trace()
