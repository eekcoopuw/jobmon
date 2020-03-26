import os

import pytest

from jobmon.models.workflow_run_status import WorkflowRunStatus
from jobmon.models.task_status import TaskStatus

this_file = os.path.dirname(__file__)
remote_sleep_and_write = os.path.abspath(os.path.expanduser(
    f"{this_file}/../_scripts/remote_sleep_and_write.py"))


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
    """create a 1 task workflow and confirm it works end to end"""
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


def test_fork_and_join_tasks_with_fatal_error(db_cfg, client_env, tmpdir):
    """
    Create the same small fork and join real_dag.
    One of the b-tasks (#1) fails consistently, so c[1] will never be ready.
    """
    from jobmon.client.workflow import Workflow
    from jobmon.client.api import PythonTask, Tool
    from jobmon.client.execution.strategies.multiprocess import \
        MultiprocessExecutor

    tool = Tool()
    workflow = Workflow(tool_version_id=tool.active_tool_version_id,
                        name="test_fork_and_join_tasks_with_fatal_error")
    executor = MultiprocessExecutor(parallelism=3)
    workflow.set_executor(executor)

    a_path = os.path.join(str(tmpdir), "a.out")
    task_a = PythonTask(
        script=remote_sleep_and_write,
        args=["--sleep_secs", "1",
              "--output_file_path", a_path,
              "--name", a_path],
        upstream_tasks=[])
    workflow.add_task(task_a)

    task_b = {}
    for i in range(3):
        b_output_file_name = os.path.join(str(tmpdir), f"b-{i}.out")
        # task b[1] will fail always
        if i == 1:
            fail_always = " --fail_always"
        else:
            fail_always = ""

        task_b[i] = PythonTask(
            script=remote_sleep_and_write,
            args=["--sleep_secs", "1",
                  "--output_file_path", b_output_file_name,
                  "--name", b_output_file_name,
                  fail_always],
            upstream_tasks=[task_a]
        )
        workflow.add_task(task_b[i])

    task_c = {}
    for i in range(3):
        c_output_file_name = os.path.join(str(tmpdir), f"c-{i}.out")
        task_c[i] = PythonTask(
            script=remote_sleep_and_write,
            args=["--sleep_secs", "1",
                  "--output_file_path", c_output_file_name,
                  "--name", c_output_file_name],
            upstream_tasks=[task_b[i]]
        )
        workflow.add_task(task_c[i])

    d_path = os.path.join(str(tmpdir), "d.out")
    task_d = PythonTask(
        script=remote_sleep_and_write,
        args=["--sleep_secs", "1",
              "--output_file_path", d_path,
              "--name", d_path],
        upstream_tasks=[task_c[i] for i in range(3)]
    )
    workflow.add_task(task_d)

    wfr = workflow.run()

    assert wfr.status == WorkflowRunStatus.ERROR
    # a, b[0], b[2], c[0], c[2],  but not b[1], c[1], d
    assert wfr.completed_report[0] == 1 + 2 + 2
    assert wfr.completed_report[1] == 0
    assert len(wfr.all_error) == 1  # b[1]

    assert wfr.swarm_tasks[task_a.task_id].status == TaskStatus.DONE

    assert wfr.swarm_tasks[task_b[0].task_id].status == TaskStatus.DONE
    assert wfr.swarm_tasks[task_b[1].task_id].status == TaskStatus.ERROR_FATAL
    assert wfr.swarm_tasks[task_b[2].task_id].status == TaskStatus.DONE

    assert wfr.swarm_tasks[task_c[0].task_id].status == TaskStatus.DONE
    assert wfr.swarm_tasks[task_c[1].task_id].status == TaskStatus.REGISTERED
    assert wfr.swarm_tasks[task_c[2].task_id].status == TaskStatus.DONE

    assert wfr.swarm_tasks[task_d.task_id].status == TaskStatus.REGISTERED


def test_fork_and_join_tasks_with_retryable_error(db_cfg, client_env, tmpdir):
    """
    Create the same fork and join real_dag with three Tasks a->b[0..3]->c and
    execute it.
    One of the b-tasks fails once, so the retry handler should cover that, and
    the whole real_dag should complete
    """
    from jobmon.client.workflow import Workflow
    from jobmon.client.api import PythonTask, Tool
    from jobmon.client.execution.strategies.multiprocess import \
        MultiprocessExecutor

    tool = Tool()
    workflow = Workflow(tool_version_id=tool.active_tool_version_id,
                        name="test_fork_and_join_tasks_with_retryable_error")
    executor = MultiprocessExecutor(parallelism=3)
    workflow.set_executor(executor)

    a_path = os.path.join(str(tmpdir), "a.out")
    task_a = PythonTask(
        script=remote_sleep_and_write,
        args=["--sleep_secs", "1",
              "--output_file_path", a_path,
              "--name", a_path],
        upstream_tasks=[])
    workflow.add_task(task_a)

    task_b = {}
    for i in range(3):
        b_output_file_name = os.path.join(str(tmpdir), f"b-{i}.out")
        # task b[1] will fail always
        if i == 1:
            fail_count = "1"
        else:
            fail_count = "0"

        # task b[1] will fail
        task_b[i] = PythonTask(
            script=remote_sleep_and_write,
            args=["--sleep_secs", "1",
                  "--output_file_path", b_output_file_name,
                  "--name", b_output_file_name,
                  "--fail_count", fail_count],
            upstream_tasks=[task_a]
        )
        workflow.add_task(task_b[i])

    task_c = {}
    for i in range(3):
        c_output_file_name = os.path.join(str(tmpdir), f"c-{i}.out")
        task_c[i] = PythonTask(
            script=remote_sleep_and_write,
            args=["--sleep_secs", "1",
                  "--output_file_path", c_output_file_name,
                  "--name", c_output_file_name],
            upstream_tasks=[task_b[i]]
        )
        workflow.add_task(task_c[i])

    d_path = os.path.join(str(tmpdir), "d.out")
    task_d = PythonTask(
        script=remote_sleep_and_write,
        args=["--sleep_secs", "1",
              "--output_file_path", d_path,
              "--name", d_path,
              "--fail_count", "2"],
        upstream_tasks=[task_c[i] for i in range(3)]
    )
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
def test_bushy_real_dag(db_cfg, client_env, tmpdir):
    """
    Similar to the a small fork and join real_dag but with connections between
    early and late phases:
       a->b[0..2]->c[0..2]->d
    And also:
       c depends on a
       d depends on b
    """
    from jobmon.client.workflow import Workflow
    from jobmon.client.api import PythonTask, Tool
    from jobmon.client.execution.strategies.multiprocess import \
        MultiprocessExecutor

    tool = Tool()
    workflow = Workflow(tool_version_id=tool.active_tool_version_id,
                        name="test_fork_and_join_tasks_with_fatal_error")
    executor = MultiprocessExecutor(parallelism=3)
    workflow.set_executor(executor)

    a_path = os.path.join(str(tmpdir), "a.out")
    task_a = PythonTask(
        script=remote_sleep_and_write,
        args=["--sleep_secs", "1",
              "--output_file_path", a_path,
              "--name", a_path],
        upstream_tasks=[])
    workflow.add_task(task_a)

    # The B's all have varying runtimes,
    task_b = {}
    for i in range(3):
        b_output_file_name = os.path.join(str(tmpdir), f"b-{i}.out")
        sleep_secs = 5 + i

        # task b[1] will fail
        task_b[i] = PythonTask(
            script=remote_sleep_and_write,
            args=["--sleep_secs", sleep_secs,
                  "--output_file_path", b_output_file_name,
                  "--name", b_output_file_name],
            upstream_tasks=[task_a]
        )
        workflow.add_task(task_b[i])

    # Each c[i] depends exactly and only on b[i]
    # The c[i] runtimes invert the b's runtimes, hoping to smoke-out any race
    # conditions by creating a collision near d
    task_c = {}
    for i in range(3):
        sleep_secs = 5 - i
        c_output_file_name = os.path.join(str(tmpdir), f"c-{i}.out")
        task_c[i] = PythonTask(
            script=remote_sleep_and_write,
            args=["--sleep_secs", "1",
                  "--output_file_path", c_output_file_name,
                  "--name", c_output_file_name],
            upstream_tasks=[task_b[i], task_a]
        )
        workflow.add_task(task_c[i])

    b_and_c = [task_b[i] for i in range(3)]
    b_and_c += [task_c[i] for i in range(3)]
    sleep_secs = 3

    d_path = os.path.join(str(tmpdir), "d.out")
    task_d = PythonTask(
        script=remote_sleep_and_write,
        args=["--sleep_secs", "1",
              "--output_file_path", d_path,
              "--name", d_path],
        upstream_tasks=b_and_c
    )
    workflow.add_task(task_d)

    wfr = workflow.run()

    # TODO: How to check that nothing was started before its upstream were
    # done?
    # Could we read database? Unfortunately not - submitted_date is initial
    # creation, not qsub status_date is date of last change.
    # Could we listen to job-instance state transitions?

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
