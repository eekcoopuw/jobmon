import pytest
import time

DAG_SIZE = 100000


def large_dag():
    from jobmon.client.api import BashTask
    from jobmon.client.api import UnknownWorkflow
    workflow = UnknownWorkflow(executor_class="SequentialExecutor")

    task_count = 1
    task_stack = []
    t = BashTask(f"echo {task_count}", executor_class="SequentialExecutor",
                 max_runtime_seconds=10, resource_scales={})
    task_stack.append(t)

    while len(task_stack) > 0:
        layer_base_task = task_stack[0]
        task_stack = task_stack[1:]
        workflow.add_tasks([layer_base_task])
        if task_count < DAG_SIZE:
            for i in range(10):
                task_count += 1
                task = BashTask(f"echo {task_count}", executor_class="SequentialExecutor",
                                max_runtime_seconds=10, resource_scales={})
                task.add_upstream(layer_base_task)
                task_stack.append(task)
    # only need to bind, no need to run
    workflow._bind()
    workflow._bind_tasks()
    return t.task_id


@pytest.mark.performance_tests
def test_subdag(db_cfg, client_env):
    from jobmon.client.status_commands import get_sub_task_tree
    base_id = large_dag()
    t2 = time.time()
    r = get_sub_task_tree([int(base_id)])
    t3 = time.time()

    print("\n\n\n")
    print("**************************************")
    print(r)
    print("**************************************")
    print("Dag size: {}".format(DAG_SIZE))
    print("Sub dag time: {}s".format(int(t3-t2)))
    print("**************************************")
