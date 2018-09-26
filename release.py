import os

from jobmon.client.swarm.workflow.workflow import Workflow
from jobmon.client.swarm.workflow.bash_task import BashTask
from jobmon.client.swarm.workflow.python_task import PythonTask
from jobmon.client.swarm.workflow.r_task import RTask
from jobmon.client.swarm.workflow.task_dag import DagExecutionStatus


def test_complete_workflow():
    t1 = BashTask("""echo "ls()" > test_r.R""")
    t2 = BashTask("""echo "print(1)" > test_py.py""")
    t3 = RTask(script='test_r.R', upstream_tasks=[t1])
    t4 = PythonTask(script='test_py.py', upstream_tasks=[t2])
    t5 = BashTask("rm test_r.R", upstream_tasks=[t3])
    t6 = BashTask("rm test_py.py", upstream_tasks=[t4])

    wfa = "test_complete_workflow"
    workflow = Workflow(workflow_args=wfa, executor_class='SequentialExecutor')
    workflow.add_tasks([t1, t2, t3, t4, t5, t6])
    status = workflow.execute()
    assert status == DagExecutionStatus.SUCCEEDED


if __name__ == '__main__':
    test_complete_workflow()
