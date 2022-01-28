import os

from jobmon.constants import TaskStatus, WorkflowRunStatus

import pytest

from jobmon.client.tool import Tool


@pytest.fixture
def tool(db_cfg, client_env):
    tool = Tool()
    tool.set_default_compute_resources_from_dict(
        cluster_name="sequential", compute_resources={"queue": "null.q"}
    )
    return tool


@pytest.fixture
def task_template(tool):
    tt = tool.get_task_template(
        template_name="my_template",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
    )
    return tt


this_file = os.path.dirname(__file__)
remote_sleep_and_write = os.path.abspath(
    os.path.expanduser(f"{this_file}/../_scripts/remote_sleep_and_write.py")
)


def test_one_task(tool, task_template):
    """create a 1 task workflow and confirm it works end to end"""

    workflow = tool.create_workflow(name="test_one_task")
    t1 = task_template.create_task(arg="echo 1")
    workflow.add_tasks([t1])
    workflow_run_status = workflow.run()
    assert workflow_run_status == WorkflowRunStatus.DONE
    assert workflow._num_newly_completed == 1
    assert workflow._num_previously_completed == 0
    assert len(workflow.task_errors) == 0


def test_two_tasks_same_command_error(tool, task_template):
    """
    Create a Workflow with two Tasks, with the second task having the same
    hash_name as the first. Make sure that, upon adding the second task to the
    dag, Workflow raises a ValueError
    """

    workflow = tool.create_workflow(name="test_two_tasks_same_command_error")
    t1 = task_template.create_task(arg="echo 1")
    workflow.add_task(t1)

    t1_again = task_template.create_task(arg="echo 1")
    with pytest.raises(ValueError):
        workflow.add_task(t1_again)


def test_three_linear_tasks(tool, task_template):
    """
    Create and execute a real_dag with three Tasks, one after another:
    a->b->c
    """

    workflow = tool.create_workflow(name="test_three_linear_tasks")

    task_a = task_template.create_task(arg="echo a", upstream_tasks=[])  # To be clear
    workflow.add_task(task_a)

    task_b = task_template.create_task(arg="echo b", upstream_tasks=[task_a])
    workflow.add_task(task_b)

    task_c = task_template.create_task(arg="echo c")
    workflow.add_task(task_c)
    task_c.add_upstream(task_b)  # Exercise add_upstream post-instantiation
    workflow_run_status = workflow.run()

    assert workflow_run_status == WorkflowRunStatus.DONE
    assert workflow._num_newly_completed == 3
    assert workflow._num_previously_completed == 0
    assert len(workflow.task_errors) == 0


def test_fork_and_join_tasks(task_template, tmpdir):
    """
    Create a small fork and join real_dag with four phases:
     a->b[0..2]->c[0..2]->d
     and execute it
    """

    tool = Tool()
    workflow = tool.create_workflow(
        name="test_fork_and_join_tasks",
        default_cluster_name="multiprocess",
        default_compute_resources_set={"multiprocess": {"queue": "null.q"}},
    )
    task_a = task_template.create_task(arg="sleep 1 && echo a")
    workflow.add_task(task_a)

    # The B's all have varying runtimes,
    task_b = {}
    for i in range(3):
        sleep_secs = 5 + i
        task_b[i] = task_template.create_task(
            arg=f"sleep {sleep_secs} && echo b", upstream_tasks=[task_a]
        )
        workflow.add_task(task_b[i])

    # Each c[i] depends exactly and only on b[i]
    # The c[i] runtimes invert the b's runtimes, hoping to smoke-out any race
    # conditions by creating a collision near d
    task_c = {}
    for i in range(3):
        sleep_secs = 5 - i
        task_c[i] = task_template.create_task(
            arg=f"sleep {sleep_secs} && echo c", upstream_tasks=[task_b[i]]
        )
        workflow.add_task(task_c[i])

    task_d = task_template.create_task(
        arg="sleep 3 && echo d", upstream_tasks=[task_c[i] for i in range(3)]
    )
    workflow.add_task(task_d)

    workflow_run_status = workflow.run()

    assert workflow_run_status == WorkflowRunStatus.DONE
    assert workflow._num_newly_completed == 1 + 3 + 3 + 1
    assert workflow._num_previously_completed == 0
    assert len(workflow.task_errors) == 0

    assert workflow.tasks[hash(task_a)].final_status == TaskStatus.DONE

    assert workflow.tasks[hash(task_b[0])].final_status == TaskStatus.DONE
    assert workflow.tasks[hash(task_b[1])].final_status == TaskStatus.DONE
    assert workflow.tasks[hash(task_b[2])].final_status == TaskStatus.DONE

    assert workflow.tasks[hash(task_c[0])].final_status == TaskStatus.DONE
    assert workflow.tasks[hash(task_c[1])].final_status == TaskStatus.DONE
    assert workflow.tasks[hash(task_c[2])].final_status == TaskStatus.DONE

    assert workflow.tasks[hash(task_d)].final_status == TaskStatus.DONE


def test_fork_and_join_tasks_with_fatal_error(task_template, tmpdir):
    """
    Create the same small fork and join real_dag.
    One of the b-tasks (#1) fails consistently, so c[1] will never be ready.
    """

    tool = Tool()
    workflow = tool.create_workflow(
        name="test_fork_and_join_tasks_with_fatal_error",
        default_cluster_name="multiprocess",
        default_compute_resources_set={"multiprocess": {"queue": "null.q"}},
    )

    a_path = os.path.join(str(tmpdir), "a.out")
    task_a = task_template.create_task(
        arg=f"python {remote_sleep_and_write} --sleep_secs 1 --output_file_path {a_path} --name {a_path}",
        upstream_tasks=[],
    )
    workflow.add_task(task_a)

    task_b = {}
    for i in range(3):
        b_output_file_name = os.path.join(str(tmpdir), f"b-{i}.out")
        # task b[1] will fail always
        if i == 1:
            fail_always = " --fail_always"
        else:
            fail_always = ""

        task_b[i] = task_template.create_task(
            arg=f"python {remote_sleep_and_write} --sleep_secs 1 "
            f"--output_file_path {b_output_file_name} --name {b_output_file_name} "
            f"{fail_always}",
            upstream_tasks=[task_a],
        )
        workflow.add_task(task_b[i])

    task_c = {}
    for i in range(3):
        c_output_file_name = os.path.join(str(tmpdir), f"c-{i}.out")
        task_c[i] = task_template.create_task(
            arg=f"python {remote_sleep_and_write} --sleep_secs 1 "
            f"--output_file_path {c_output_file_name} --name {c_output_file_name}",
            upstream_tasks=[task_b[i]],
        )
        workflow.add_task(task_c[i])

    d_path = os.path.join(str(tmpdir), "d.out")
    task_d = task_template.create_task(
        arg=f"python {remote_sleep_and_write} --sleep_secs 1 "
        f"--output_file_path {d_path} --name {d_path}",
        upstream_tasks=[task_c[i] for i in range(3)],
    )
    workflow.add_task(task_d)

    workflow_run_status = workflow.run()

    assert workflow_run_status == WorkflowRunStatus.ERROR
    # a, b[0], b[2], c[0], c[2],  but not b[1], c[1], d
    assert workflow._num_newly_completed == 1 + 2 + 2
    assert workflow._num_previously_completed == 0
    assert len(workflow.task_errors) == 1  # b[1]

    assert workflow.tasks[hash(task_a)].final_status == TaskStatus.DONE

    assert workflow.tasks[hash(task_b[0])].final_status == TaskStatus.DONE
    assert workflow.tasks[hash(task_b[1])].final_status == TaskStatus.ERROR_FATAL
    assert workflow.tasks[hash(task_b[2])].final_status == TaskStatus.DONE

    assert workflow.tasks[hash(task_c[0])].final_status == TaskStatus.DONE
    assert workflow.tasks[hash(task_c[1])].final_status == TaskStatus.REGISTERING
    assert workflow.tasks[hash(task_c[2])].final_status == TaskStatus.DONE

    assert workflow.tasks[hash(task_d)].final_status == TaskStatus.REGISTERING


def test_fork_and_join_tasks_with_retryable_error(task_template, tmpdir):
    """
    Create the same fork and join real_dag with three Tasks a->b[0..3]->c and
    execute it.
    One of the b-tasks fails once, so the retry handler should cover that, and
    the whole real_dag should complete
    """
    tool = Tool()
    workflow = tool.create_workflow(
        name="test_fork_and_join_tasks_with_retryable_error",
        default_cluster_name="multiprocess",
        default_compute_resources_set={"multiprocess": {"queue": "null.q"}},
    )

    a_path = os.path.join(str(tmpdir), "a.out")
    task_a = task_template.create_task(
        arg=f"python {remote_sleep_and_write} --sleep_secs 1 "
        f"--output_file_path {a_path} --name {a_path}",
        upstream_tasks=[],
    )
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
        task_b[i] = task_template.create_task(
            arg=f"python {remote_sleep_and_write} --sleep_secs 1 "
            f"--output_file_path {b_output_file_name} --name {b_output_file_name} "
            f"--fail_count {fail_count}",
            upstream_tasks=[task_a],
        )
        workflow.add_task(task_b[i])

    task_c = {}
    for i in range(3):
        c_output_file_name = os.path.join(str(tmpdir), f"c-{i}.out")
        task_c[i] = task_template.create_task(
            arg=f"python {remote_sleep_and_write} --sleep_secs 1 "
            f"--output_file_path {c_output_file_name} --name {c_output_file_name}",
            upstream_tasks=[task_b[i]],
        )
        workflow.add_task(task_c[i])

    d_path = os.path.join(str(tmpdir), "d.out")
    task_d = task_template.create_task(
        arg=f"python {remote_sleep_and_write} --sleep_secs 1 "
        f"--output_file_path {d_path} --name {d_path} "
        f"--fail_count 2",
        upstream_tasks=[task_c[i] for i in range(3)],
    )
    workflow.add_task(task_d)

    workflow_run_status = workflow.run()

    assert workflow_run_status == WorkflowRunStatus.DONE
    assert workflow._num_newly_completed == 1 + 3 + 3 + 1
    assert workflow._num_previously_completed == 0
    assert len(workflow.task_errors) == 0

    assert workflow.tasks[hash(task_a)].final_status == TaskStatus.DONE

    assert workflow.tasks[hash(task_b[0])].final_status == TaskStatus.DONE
    assert workflow.tasks[hash(task_b[1])].final_status == TaskStatus.DONE
    assert workflow.tasks[hash(task_b[2])].final_status == TaskStatus.DONE

    assert workflow.tasks[hash(task_c[0])].final_status == TaskStatus.DONE
    assert workflow.tasks[hash(task_c[1])].final_status == TaskStatus.DONE
    assert workflow.tasks[hash(task_c[2])].final_status == TaskStatus.DONE

    assert workflow.tasks[hash(task_d)].final_status == TaskStatus.DONE


@pytest.mark.qsubs_jobs
def test_bushy_real_dag(task_template, tmpdir):
    """
    Similar to the a small fork and join real_dag but with connections between
    early and late phases:
       a->b[0..2]->c[0..2]->d
    And also:
       c depends on a
       d depends on b
    """

    tool = Tool()
    workflow = tool.create_workflow(
        name="test_fork_and_join_tasks_with_fatal_error",
        default_cluster_name="multiprocess",
        default_compute_resources_set={"multiprocess": {"queue": "null.q"}},
    )

    a_path = os.path.join(str(tmpdir), "a.out")
    task_a = task_template.create_task(
        arg=f"python {remote_sleep_and_write} --sleep_secs 1 "
        f"--output_file_path {a_path} --name {a_path}",
        upstream_tasks=[],
    )
    workflow.add_task(task_a)

    # The B's all have varying runtimes,
    task_b = {}
    for i in range(3):
        b_output_file_name = os.path.join(str(tmpdir), f"b-{i}.out")
        sleep_secs = 5 + i

        # task b[1] will fail
        task_b[i] = task_template.create_task(
            arg=f"python {remote_sleep_and_write} --sleep_secs {sleep_secs} "
            f"--output_file_path {b_output_file_name} --name {b_output_file_name}",
            upstream_tasks=[task_a],
        )
        workflow.add_task(task_b[i])

    # Each c[i] depends exactly and only on b[i]
    # The c[i] runtimes invert the b's runtimes, hoping to smoke-out any race
    # conditions by creating a collision near d
    task_c = {}
    for i in range(3):
        sleep_secs = 5 - i
        c_output_file_name = os.path.join(str(tmpdir), f"c-{i}.out")
        task_c[i] = task_template.create_task(
            arg=f"python {remote_sleep_and_write} --sleep_secs {sleep_secs} "
            f"--output_file_path {c_output_file_name} --name {c_output_file_name}",
            upstream_tasks=[task_b[i], task_a],
        )
        workflow.add_task(task_c[i])

    b_and_c = [task_b[i] for i in range(3)]
    b_and_c += [task_c[i] for i in range(3)]
    sleep_secs = 3
    d_path = os.path.join(str(tmpdir), "d.out")
    task_d = task_template.create_task(
        arg=f"python {remote_sleep_and_write} --sleep_secs {sleep_secs} "
        f"--output_file_path {d_path} --name {d_path}",
        upstream_tasks=b_and_c,
    )
    workflow.add_task(task_d)

    workflow_run_status = workflow.run()

    # TODO: How to check that nothing was started before its upstream were
    # done?
    # Could we read database? Unfortunately not - submitted_date is initial
    # creation, not qsub status_date is date of last change.
    # Could we listen to job-instance state transitions?

    assert workflow_run_status == WorkflowRunStatus.DONE
    assert workflow._num_newly_completed == 1 + 3 + 3 + 1
    assert workflow._num_previously_completed == 0
    assert len(workflow.task_errors) == 0

    assert workflow.tasks[hash(task_a)].final_status == TaskStatus.DONE

    assert workflow.tasks[hash(task_b[0])].final_status == TaskStatus.DONE
    assert workflow.tasks[hash(task_b[1])].final_status == TaskStatus.DONE
    assert workflow.tasks[hash(task_b[2])].final_status == TaskStatus.DONE

    assert workflow.tasks[hash(task_c[0])].final_status == TaskStatus.DONE
    assert workflow.tasks[hash(task_c[1])].final_status == TaskStatus.DONE
    assert workflow.tasks[hash(task_c[2])].final_status == TaskStatus.DONE

    assert workflow.tasks[hash(task_d)].final_status == TaskStatus.DONE
