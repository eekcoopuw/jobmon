from jobmon.client.workflow.bash_task import BashTask
from jobmon.client.workflow.workflow import Workflow


def test_foo(db_cfg, client_env, multiprocess_scheduler):
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


def test_bar(db_cfg, client_env, sequential_scheduler):
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
