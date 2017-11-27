import pytest

from jobmon.workflow.bash_task import BashTask
from jobmon.workflow.task_dag import TaskDag
from jobmon.workflow.workflow import Workflow


def test_simple(db_cfg, jsm_jqs):
    dag = TaskDag()
    t1 = BashTask("sleep 1")
    t2 = BashTask("sleep 2", upstream_tasks=[t1])
    t3 = BashTask("sleep 3", upstream_tasks=[t2])
    dag.add_tasks([t1, t2, t3])

    wfa = ""
    workflow = Workflow(dag, wfa)
    workflow.execute()
