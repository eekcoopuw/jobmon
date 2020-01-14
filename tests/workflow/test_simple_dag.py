import pytest

from jobmon.models.workflow_run_status import WorkflowRunStatus
from jobmon.models.task_status import TaskStatus


def test_empty_workflow(db_cfg, client_env):
    """
    Create a real_dag with no Tasks. Call all the creation methods and check
    that it raises no Exceptions.
    """
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow

    workflow = UnknownWorkflow("test_empty_real_dag",
                               executor_class="SequentialExecutor")

    with pytest.raises(RuntimeError):
        workflow.run()


def test_one_task(db_cfg, client_env):
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.api import BashTask

    workflow = UnknownWorkflow("test_one_task",
                               executor_class="SequentialExecutor")
    t1 = BashTask("echo 1", executor_class="SequentialExecutor")
    workflow.add_tasks([t1])
    wfr = workflow.run()

    assert wfr.status == WorkflowRunStatus.DONE
    assert wfr.completed_report[0] == 1
    assert wfr.completed_report[1] == 0
    assert len(wfr.all_error) == 0


def test_two_tasks_same_command_error(db_cfg, client_env):
    """
    Create a Workflow with two Tasks, with the second task having the same
    hash_name as the first. Make sure that, upon adding the second task to the
    dag, Workflow raises a ValueError
    """

    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.api import BashTask

    workflow = UnknownWorkflow("test_two_tasks_same_command_error",
                               executor_class="SequentialExecutor")
    t1 = BashTask("echo 1", executor_class="SequentialExecutor")
    workflow.add_task(t1)

    t1_again = BashTask("echo 1", executor_class="SequentialExecutor")
    with pytest.raises(ValueError):
        workflow.add_task(t1_again)


@pytest.mark.qsubs_jobs
def test_three_linear_tasks(db_cfg, client_env):
    """
    Create and execute a real_dag with three Tasks, one after another:
    a->b->c
    """

    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.api import BashTask

    workflow = UnknownWorkflow("test_three_linear_tasks",
                               executor_class="SequentialExecutor")

    task_a = BashTask(
        "echo a", executor_class="SequentialExecutor",
        upstream_tasks=[]  # To be clear
    )
    workflow.add_task(task_a)

    task_b = BashTask(
        "echo b", executor_class="SequentialExecutor",
        upstream_tasks=[task_a]
    )
    workflow.add_task(task_b)

    task_c = BashTask("echo c", executor_class="SequentialExecutor")
    workflow.add_task(task_c)
    task_c.add_upstream(task_b)  # Exercise add_upstream post-instantiation
    wfr = workflow.run()

    assert wfr.status == WorkflowRunStatus.DONE
    assert wfr.completed_report[0] == 3
    assert wfr.completed_report[1] == 0
    assert len(wfr.all_error) == 0


@pytest.mark.qsubs_jobs
def test_fork_and_join_tasks(db_cfg, client_env):
    """
    Create a small fork and join real_dag with four phases:
     a->b[0..2]->c[0..2]->d
     and execute it
    """
    from jobmon.client.workflow import Workflow
    from jobmon.client.api import BashTask, Tool
    from jobmon.client.execution.strategies.multiprocess import \
        MultiprocessExecutor

    tool = Tool()
    workflow = Workflow(tool_version_id=tool.active_tool_version_id,
                        name="test_fork_and_join_tasks")
    executor = MultiprocessExecutor(parallelism=3)
    workflow.set_executor(executor)

    task_a = BashTask("sleep 1 && echo a",
                      executor_class="MultiprocessExecutor")
    workflow.add_task(task_a)

    # The B's all have varying runtimes,
    task_b = {}
    for i in range(3):
        sleep_secs = 5 + i
        task_b[i] = BashTask(f"sleep {sleep_secs} && echo b",
                             executor_class="MultiprocessExecutor",
                             upstream_tasks=[task_a])
        workflow.add_task(task_b[i])

    # Each c[i] depends exactly and only on b[i]
    # The c[i] runtimes invert the b's runtimes, hoping to smoke-out any race
    # conditions by creating a collision near d
    task_c = {}
    for i in range(3):
        sleep_secs = 5 - i
        task_c[i] = BashTask(f"sleep {sleep_secs} && echo c",
                             executor_class="MultiprocessExecutor",
                             upstream_tasks=[task_b[i]])
        workflow.add_task(task_c[i])

    task_d = BashTask("sleep 3 && echo d",
                      executor_class="MultiprocessExecutor",
                      upstream_tasks=[task_c[i] for i in range(3)])
    workflow.add_task(task_d)

    wfr = workflow.run()
    assert wfr.status == WorkflowRunStatus.DONE
    assert wfr.completed_report[0] == 1 + 3 + 3 + 1
    assert wfr.completed_report[1] == 0
    assert len(wfr.all_error) == 0

    assert wfr.swarm_tasks[task_a.task_id].status == TaskStatus.DONE

    assert wfr.swarm_tasks[task_b[0].task_id].status == TaskStatus.DONE
    assert wfr.swarm_tasks[task_b[1].task_id].status == TaskStatus.DONE
    assert wfr.swarm_tasks[task_b[2].task_id].status == TaskStatus.DONE

    assert wfr.swarm_tasks[task_c[0].task_id].status == TaskStatus.DONE
    assert wfr.swarm_tasks[task_c[1].task_id].status == TaskStatus.DONE
    assert wfr.swarm_tasks[task_c[2].task_id].status == TaskStatus.DONE

    assert wfr.swarm_tasks[task_d.task_id].status == TaskStatus.DONE


@pytest.mark.qsubs_jobs
def test_fork_and_join_tasks_with_fatal_error(db_cfg, client_env):
    """
    Create the same small fork and join real_dag.
    One of the b-tasks (#1) fails consistently, so c[1] will never be ready.
    """
    from jobmon.client.workflow import Workflow
    from jobmon.client.api import BashTask, Tool
    from jobmon.client.execution.strategies.multiprocess import \
        MultiprocessExecutor

    tool = Tool()
    workflow = Workflow(tool_version_id=tool.active_tool_version_id,
                        name="test_fork_and_join_tasks_with_fatal_error")
    executor = MultiprocessExecutor(parallelism=3)
    workflow.set_executor(executor)


    task_a = SleepAndWriteFileMockTask(
        command=("python {cs} --sleep_secs 1 --output_file_path {ofn} "
                 "--name {n}".format(cs=command_script, ofn=output_file_name,
                                     n=output_file_name))
    )
    real_dag.add_task(task_a)

    task_b = {}
    for i in range(3):
        output_file_name = "{}/b-{}.out".format(root_out_dir, i)
        # task b[1] will fail always
        task_b[i] = SleepAndWriteFileMockTask(
            command=("python {cs} --sleep_secs 1 --output_file_path {ofn} "
                     "--name {n}".format(cs=command_script,
                                         ofn=output_file_name,
                                         n=output_file_name)),
            upstream_tasks=[task_a],
            fail_always=(i == 1)
        )
        real_dag.add_task(task_b[i])

    task_c = {}
    for i in range(3):
        output_file_name = "{}/c-{}.out".format(root_out_dir, i)
        task_c[i] = SleepAndWriteFileMockTask(
            command=("python {cs} --sleep_secs 1 --output_file_path {ofn} "
                     "--name {n}".format(cs=command_script,
                                         ofn=output_file_name,
                                         n=output_file_name)),
            upstream_tasks=[task_b[i]]
        )
        real_dag.add_task(task_c[i])

    output_file_name = "{}/d.out".format(root_out_dir)
    task_d = SleepAndWriteFileMockTask(
        command=("python {cs} --sleep_secs 1 --output_file_path {ofn} "
                 "--name {n}".format(cs=command_script, ofn=output_file_name,
                                     n=output_file_name)),
        upstream_tasks=[task_c[i] for i in range(3)]
    )
    real_dag.add_task(task_d)

    logger.info("real_dag: {}".format(real_dag))

    (rc, num_completed, num_previously_complete, num_failed) = \
        real_dag._execute()

    assert rc == DagExecutionStatus.FAILED
    # a, b[0], b[2], c[0], c[2],  but not b[1], c[1], d
    assert num_completed == 1 + 2 + 2
    assert num_previously_complete == 0
    assert num_failed == 1  # b[1]

    assert task_status(real_dag, task_a) == JobStatus.DONE

    assert task_status(real_dag, task_b[0]) == JobStatus.DONE
    assert task_status(real_dag, task_b[1]) == JobStatus.ERROR_FATAL
    assert task_status(real_dag, task_b[2]) == JobStatus.DONE

    assert task_status(real_dag, task_c[0]) == JobStatus.DONE
    assert task_status(real_dag, task_c[1]) == JobStatus.REGISTERED
    assert task_status(real_dag, task_c[2]) == JobStatus.DONE

    assert task_status(real_dag, task_d) == JobStatus.REGISTERED

