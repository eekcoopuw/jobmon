from typing import Optional

from jobmon.client.execution.strategies.base import Executor, TaskInstanceExecutorInfo


def get_scheduling_executor_by_name(self, executor_class: Optional[str] = None, *args,
                                    **kwargs) -> Executor:
    """Return an instance of the scheduling executor

    Args:
        executor_class: Name of executor class run your tasks on.
    """

    if executor_class == "SequentialExecutor":
        from jobmon.client.execution.strategies.sequential import \
            SequentialExecutor as SchedulingExecutor
    elif executor_class == "SGEExecutor":
        from jobmon.client.execution.strategies.sge.sge_executor import \
            SGEExecutor as SchedulingExecutor
    elif executor_class == "DummyExecutor":
        from jobmon.client.execution.strategies.dummy import \
            DummyExecutor as SchedulingExecutor
    elif executor_class == "MultiprocessExecutor":
        from jobmon.client.execution.strategies.multiprocess import \
            MultiprocessExecutor as SchedulingExecutor
    else:
        raise ValueError(f"{executor_class} is not a valid ExecutorClass")
    return SchedulingExecutor(*args, **kwargs)


def get_task_instance_executor_by_name(self, executor_class: str) -> TaskInstanceExecutorInfo:
    """Return an instance of the task instance executor

    Args:
        executor_class: Name of executor class run your tasks on.
    """
    # identify executor class
    if executor_class == "SequentialExecutor":
        from jobmon.client.execution.strategies.sequential import \
            TaskInstanceSequentialInfo as TaskInstanceExecutorInfo
    elif executor_class == "SGEExecutor":
        from jobmon.client.execution.strategies.sge.sge_executor import \
            TaskInstanceSGEInfo as TaskInstanceExecutorInfo
    elif executor_class == "DummyExecutor":
        from jobmon.client.execution.strategies.base import \
            TaskInstanceExecutorInfo
    elif executor_class == "MultiprocessExecutor":
        from jobmon.client.execution.strategies.multiprocess import \
            TaskInstanceMultiprocessInfo as TaskInstanceExecutorInfo
    else:
        raise ValueError("{} is not a valid ExecutorClass".format(executor_class))
    return TaskInstanceExecutorInfo()
