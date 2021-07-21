import os

from jobmon.constants import TaskStatus, WorkflowRunStatus

import pytest

from jobmon.client.task import Task
from jobmon.client.tool import Tool

@pytest.fixture
def tool(db_cfg, client_env):
    tool = Tool()
    tool.set_default_compute_resources_from_dict(cluster_name="sequential",
                                                 compute_resources={"queue": "null.q"})
    return tool


@pytest.fixture
def task_template(tool):
    tt = tool.get_task_template(
        template_name="my_template",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[]
    )
    return tt


this_file = os.path.dirname(__file__)
remote_sleep_and_write = os.path.abspath(os.path.expanduser(
    f"{this_file}/../_scripts/remote_sleep_and_write.py"))


def test_empty_workflow(task_template):
    """
    Create a real_dag with no Tasks. Call all the creation methods and check
    that it raises no Exceptions.
    """

    tool = Tool()
    workflow = tool.create_workflow(name="test_empty_real_dag",
                                    default_cluster_name="sequential",
                                    default_compute_resources_set={"sequential": {"queue": "null.q"}})

    with pytest.raises(RuntimeError):
        workflow.bind()
        workflow.run()


def test_one_task(task_template):
    """create a 1 task workflow and confirm it works end to end"""

    tool = Tool()
    workflow = tool.create_workflow(name="test_one_task",
                                    default_cluster_name="sequential",
                                    default_compute_resources_set={"sequential": {"queue": "null.q"}}
                                    )
    t1 = task_template.create_task(arg="echo 1")
    workflow.add_tasks([t1])
    workflow.bind()
    wfrs = workflow.run()
    assert wfrs == WorkflowRunStatus.DONE
    assert workflow._last_workflowrun.completed_report[0] == 1
    assert workflow._last_workflowrun.completed_report[1] == 0
    assert len(workflow._last_workflowrun.all_error) == 0


def test_two_tasks_same_command_error(task_template):
    """
    Create a Workflow with two Tasks, with the second task having the same
    hash_name as the first. Make sure that, upon adding the second task to the
    dag, Workflow raises a ValueError
    """

    tool = Tool()
    workflow = tool.create_workflow(name="test_two_tasks_same_command_error")
    t1 = task_template.create_task(
         arg="echo 1")
    workflow.add_task(t1)

    t1_again = task_template.create_task(
         arg="echo 1")
    with pytest.raises(ValueError):
        workflow.add_task(t1_again)


def test_three_linear_tasks(task_template):
    """
    Create and execute a real_dag with three Tasks, one after another:
    a->b->c
    """

    tool = Tool()
    workflow = tool.create_workflow(name="test_three_linear_tasks",
                                    default_cluster_name = "sequential",
                                    default_compute_resources_set = {"sequential": {"queue": "null.q"}}
                                    )

    task_a = task_template.create_task(
             arg="echo a",
             upstream_tasks=[]  # To be clear
    )
    workflow.add_task(task_a)

    task_b = task_template.create_task(
             arg="echo b",
             upstream_tasks=[task_a]
    )
    workflow.add_task(task_b)

    task_c = task_template.create_task(
             arg="echo c")
    workflow.add_task(task_c)
    task_c.add_upstream(task_b)  # Exercise add_upstream post-instantiation
    workflow.bind()
    wfrs = workflow.run()

    assert wfrs == WorkflowRunStatus.DONE
    assert workflow._last_workflowrun.completed_report[0] == 3
    assert workflow._last_workflowrun.completed_report[1] == 0
    assert len(workflow._last_workflowrun.all_error) == 0


def test_fork_and_join_tasks(task_template, tmpdir):
    """
    Create a small fork and join real_dag with four phases:
     a->b[0..2]->c[0..2]->d
     and execute it
    """

    tool = Tool()
    workflow = tool.create_workflow(name="test_fork_and_join_tasks",
                                    default_cluster_name="multiprocess",
                                    default_compute_resources_set={"multiprocess": {"queue": "null.q"}}
                                    )
    task_a = task_template.create_task(arg="sleep 1 && echo a")
    workflow.add_task(task_a)

    # The B's all have varying runtimes,
    task_b = {}
    for i in range(3):
        sleep_secs = 5 + i
        task_b[i] = task_template.create_task(
                    arg=f"sleep {sleep_secs} && echo b",
                    upstream_tasks=[task_a])
        workflow.add_task(task_b[i])

    # Each c[i] depends exactly and only on b[i]
    # The c[i] runtimes invert the b's runtimes, hoping to smoke-out any race
    # conditions by creating a collision near d
    task_c = {}
    for i in range(3):
        sleep_secs = 5 - i
        task_c[i] = task_template.create_task(
                    arg=f"sleep {sleep_secs} && echo c",
                    upstream_tasks=[task_b[i]])
        workflow.add_task(task_c[i])

    task_d = task_template.create_task(
             arg=f"sleep 3 && echo d",
             upstream_tasks=[task_c[i] for i in range(3)])
    workflow.add_task(task_d)

    workflow.bind()

    wfrs = workflow.run()

    assert wfrs == WorkflowRunStatus.DONE
    assert workflow._last_workflowrun.completed_report[0] == 1 + 3 + 3 + 1
    assert workflow._last_workflowrun.completed_report[1] == 0
    assert len(workflow._last_workflowrun.all_error) == 0

    assert workflow._last_workflowrun.swarm_tasks[task_a.task_id].status == TaskStatus.DONE

    assert workflow._last_workflowrun.swarm_tasks[task_b[0].task_id].status == TaskStatus.DONE
    assert workflow._last_workflowrun.swarm_tasks[task_b[1].task_id].status == TaskStatus.DONE
    assert workflow._last_workflowrun.swarm_tasks[task_b[2].task_id].status == TaskStatus.DONE

    assert workflow._last_workflowrun.swarm_tasks[task_c[0].task_id].status == TaskStatus.DONE
    assert workflow._last_workflowrun.swarm_tasks[task_c[1].task_id].status == TaskStatus.DONE
    assert workflow._last_workflowrun.swarm_tasks[task_c[2].task_id].status == TaskStatus.DONE

    assert workflow._last_workflowrun.swarm_tasks[task_d.task_id].status == TaskStatus.DONE


def test_fork_and_join_tasks_with_fatal_error(task_template, tmpdir):
    """
    Create the same small fork and join real_dag.
    One of the b-tasks (#1) fails consistently, so c[1] will never be ready.
    """
    from jobmon.client.workflow import Workflow

    tool = Tool()
    workflow = tool.create_workflow(name="test_fork_and_join_tasks_with_fatal_error",
                                    default_cluster_name="multiprocess",
                                    default_compute_resources_set={"multiprocess": {"queue": "null.q"}}
                                    )

    a_path = os.path.join(str(tmpdir), "a.out")
    task_a = task_template.create_task(
             arg=f"python {remote_sleep_and_write} --sleep_secs 1 --output_file_path {a_path} --name {a_path}",
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

        task_b[i] = task_template.create_task(
             arg=f"python {remote_sleep_and_write} --sleep_secs 1 "
                 f"--output_file_path {b_output_file_name} --name {b_output_file_name} "
                 f"{fail_always}",
             upstream_tasks=[task_a]
        )
        workflow.add_task(task_b[i])

    task_c = {}
    for i in range(3):
        c_output_file_name = os.path.join(str(tmpdir), f"c-{i}.out")
        task_c[i] = task_template.create_task(
             arg=f"python {remote_sleep_and_write} --sleep_secs 1 "
                 f"--output_file_path {c_output_file_name} --name {c_output_file_name}",
             upstream_tasks=[task_b[i]]
        )
        workflow.add_task(task_c[i])

    d_path = os.path.join(str(tmpdir), "d.out")
    task_d = task_template.create_task(
             arg=f"python {remote_sleep_and_write} --sleep_secs 1 "
                 f"--output_file_path {d_path} --name {d_path}",
             upstream_tasks=[task_c[i] for i in range(3)]
    )
    workflow.add_task(task_d)

    workflow.bind()

    wfrs = workflow.run()

    assert wfrs == WorkflowRunStatus.ERROR
    # a, b[0], b[2], c[0], c[2],  but not b[1], c[1], d
    assert workflow._last_workflowrun.completed_report[0] == 1 + 2 + 2
    assert workflow._last_workflowrun.completed_report[1] == 0
    assert len(workflow._last_workflowrun.all_error) == 1  # b[1]

    assert workflow._last_workflowrun.swarm_tasks[task_a.task_id].status == TaskStatus.DONE

    assert workflow._last_workflowrun.swarm_tasks[task_b[0].task_id].status == TaskStatus.DONE
    assert workflow._last_workflowrun.swarm_tasks[task_b[1].task_id].status == TaskStatus.ERROR_FATAL
    assert workflow._last_workflowrun.swarm_tasks[task_b[2].task_id].status == TaskStatus.DONE

    assert workflow._last_workflowrun.swarm_tasks[task_c[0].task_id].status == TaskStatus.DONE
    assert workflow._last_workflowrun.swarm_tasks[task_c[1].task_id].status == TaskStatus.REGISTERED
    assert workflow._last_workflowrun.swarm_tasks[task_c[2].task_id].status == TaskStatus.DONE

    assert workflow._last_workflowrun.swarm_tasks[task_d.task_id].status == TaskStatus.REGISTERED


def test_fork_and_join_tasks_with_retryable_error(task_template, tmpdir):
    """
    Create the same fork and join real_dag with three Tasks a->b[0..3]->c and
    execute it.
    One of the b-tasks fails once, so the retry handler should cover that, and
    the whole real_dag should complete
    """
    from jobmon.client.workflow import Workflow

    tool = Tool()
    workflow = tool.create_workflow(name="test_fork_and_join_tasks_with_retryable_error",
                                    default_cluster_name="multiprocess",
                                    default_compute_resources_set={"multiprocess": {"queue": "null.q"}}
                                    )

    a_path = os.path.join(str(tmpdir), "a.out")
    task_a = task_template.create_task(
             arg=f"python {remote_sleep_and_write} --sleep_secs 1 "
                 f"--output_file_path {a_path} --name {a_path}",
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
        task_b[i] = task_template.create_task(
             arg=f"python {remote_sleep_and_write} --sleep_secs 1 "
                 f"--output_file_path {b_output_file_name} --name {b_output_file_name} "
                 f"--fail_count {fail_count}",
             upstream_tasks=[task_a]
        )
        workflow.add_task(task_b[i])

    task_c = {}
    for i in range(3):
        c_output_file_name = os.path.join(str(tmpdir), f"c-{i}.out")
        task_c[i] = task_template.create_task(
             arg=f"python {remote_sleep_and_write} --sleep_secs 1 "
                 f"--output_file_path {c_output_file_name} --name {c_output_file_name}",
             upstream_tasks=[task_b[i]]
        )
        workflow.add_task(task_c[i])

    d_path = os.path.join(str(tmpdir), "d.out")
    task_d = task_template.create_task(
             arg=f"python {remote_sleep_and_write} --sleep_secs 1 "
                 f"--output_file_path {d_path} --name {d_path} "
                 f"--fail_count 2",
        upstream_tasks=[task_c[i] for i in range(3)]
    )
    workflow.add_task(task_d)

    workflow.bind()

    wfrs = workflow.run()
    assert wfrs == WorkflowRunStatus.DONE
    assert workflow._last_workflowrun.completed_report[0] == 1 + 3 + 3 + 1
    assert workflow._last_workflowrun.completed_report[1] == 0
    assert len(workflow._last_workflowrun.all_error) == 0

    assert workflow._last_workflowrun.swarm_tasks[task_a.task_id].status == TaskStatus.DONE

    assert workflow._last_workflowrun.swarm_tasks[task_b[0].task_id].status == TaskStatus.DONE
    assert workflow._last_workflowrun.swarm_tasks[task_b[1].task_id].status == TaskStatus.DONE
    assert workflow._last_workflowrun.swarm_tasks[task_b[2].task_id].status == TaskStatus.DONE

    assert workflow._last_workflowrun.swarm_tasks[task_c[0].task_id].status == TaskStatus.DONE
    assert workflow._last_workflowrun.swarm_tasks[task_c[1].task_id].status == TaskStatus.DONE
    assert workflow._last_workflowrun.swarm_tasks[task_c[2].task_id].status == TaskStatus.DONE

    assert workflow._last_workflowrun.swarm_tasks[task_d.task_id].status == TaskStatus.DONE


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
    from jobmon.client.workflow import Workflow

    tool = Tool()
    workflow = tool.create_workflow(name="test_fork_and_join_tasks_with_fatal_error",
                                    default_cluster_name="multiprocess",
                                    default_compute_resources_set={"multiprocess": {"queue": "null.q"}}
                                    )

    a_path = os.path.join(str(tmpdir), "a.out")
    task_a = task_template.create_task(
             arg=f"python {remote_sleep_and_write} --sleep_secs 1 "
                 f"--output_file_path {a_path} --name {a_path}",
             upstream_tasks=[])
    workflow.add_task(task_a)

    # The B's all have varying runtimes,
    task_b = {}
    for i in range(3):
        b_output_file_name = os.path.join(str(tmpdir), f"b-{i}.out")
        sleep_secs = 5 + i

        # task b[1] will fail
        task_b[i] = task_template.create_task(
             arg=f"python {remote_sleep_and_write} --sleep_secs 1 "
                 f"--output_file_path {b_output_file_name} --name {b_output_file_name}",
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
        task_c[i] = task_template.create_task(
             arg=f"python {remote_sleep_and_write} --sleep_secs 1 "
                 f"--output_file_path {c_output_file_name} --name {c_output_file_name}",
             upstream_tasks=[task_b[i], task_a]
        )
        workflow.add_task(task_c[i])

    b_and_c = [task_b[i] for i in range(3)]
    b_and_c += [task_c[i] for i in range(3)]
    sleep_secs = 3

    d_path = os.path.join(str(tmpdir), "d.out")
    task_d = task_template.create_task(
             arg=f"python {remote_sleep_and_write} --sleep_secs 1 "
                 f"--output_file_path {d_path} --name {d_path}",
             upstream_tasks=b_and_c
    )
    workflow.add_task(task_d)

    workflow.bind()

    wfrs = workflow.run()

    # TODO: How to check that nothing was started before its upstream were
    # done?
    # Could we read database? Unfortunately not - submitted_date is initial
    # creation, not qsub status_date is date of last change.
    # Could we listen to job-instance state transitions?

    assert wfrs == WorkflowRunStatus.DONE
    assert workflow._last_workflowrun.completed_report[0] == 1 + 3 + 3 + 1
    assert workflow._last_workflowrun.completed_report[1] == 0
    assert len(workflow._last_workflowrun.all_error) == 0

    assert workflow._last_workflowrun.swarm_tasks[task_a.task_id].status == TaskStatus.DONE

    assert workflow._last_workflowrun.swarm_tasks[task_b[0].task_id].status == TaskStatus.DONE
    assert workflow._last_workflowrun.swarm_tasks[task_b[1].task_id].status == TaskStatus.DONE
    assert workflow._last_workflowrun.swarm_tasks[task_b[2].task_id].status == TaskStatus.DONE

    assert workflow._last_workflowrun.swarm_tasks[task_c[0].task_id].status == TaskStatus.DONE
    assert workflow._last_workflowrun.swarm_tasks[task_c[1].task_id].status == TaskStatus.DONE
    assert workflow._last_workflowrun.swarm_tasks[task_c[2].task_id].status == TaskStatus.DONE

    assert workflow._last_workflowrun.swarm_tasks[task_d.task_id].status == TaskStatus.DONE


def test_workflow_attribute(db_cfg, client_env, task_template):
    """Test the workflow attributes feature"""
    from jobmon.server.web.models.workflow_attribute import WorkflowAttribute
    from jobmon.server.web.models.workflow_attribute_type import WorkflowAttributeType

    tool = Tool()
    wf1 = tool.create_workflow(name="test_wf_attributes",
                                    default_cluster_name="sequential",
                                    default_compute_resources_set={"sequential": {"queue": "null.q"}},
                                    workflow_attributes={'location_id': 5, 'year': 2019, 'sex': 1})

    t1 = task_template.create_task(
         arg="exit -0")
    wf1.add_task(t1)
    wf1.bind()
    wf1.run()

    # check database entries are populated correctly
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        wf_attributes = DB.session.query(WorkflowAttributeType.name, WorkflowAttribute.value).\
            join(WorkflowAttribute,
                 WorkflowAttribute.workflow_attribute_type_id == WorkflowAttributeType.id).\
            filter(WorkflowAttribute.workflow_id == wf1.workflow_id).all()
    assert set(wf_attributes) == set([('location_id', '5'), ('year', '2019'), ('sex', '1')])

    # Add and update attributes
    wf1.add_attributes({'age_group_id': 1, 'sex': 2})

    with app.app_context():
        wf_attributes = DB.session.query(WorkflowAttributeType.name, WorkflowAttribute.value).\
            join(WorkflowAttribute,
                 WorkflowAttribute.workflow_attribute_type_id == WorkflowAttributeType.id).\
            filter(WorkflowAttribute.workflow_id == wf1.workflow_id).all()
    assert set(wf_attributes) == set([('location_id', '5'), ('year', '2019'), ('sex', '2'),
                                      ('age_group_id', '1')])

    # Test workflow w/o attributes
    wf2 = tool.create_workflow(name="test_empty_wf_attributes",
                               default_cluster_name="sequential",
                               default_compute_resources_set={"sequential": {"queue": "null.q"}})
    wf2.add_task(t1)
    wf2.bind()
    wf2.run()

    with app.app_context():
        wf_attributes = DB.session.query(WorkflowAttribute).filter_by(
            workflow_id=wf2.workflow_id).all()

    assert wf_attributes == []


def test_chunk_size(db_cfg, client_env, task_template):

    tool = Tool()
    wf_a = tool.create_workflow(name="test_wf_chunks_a",
                                    default_cluster_name="sequential",
                                    default_compute_resources_set={"sequential": {"queue": "null.q"}},
                                    chunk_size=3)

    task_a = task_template.create_task(
         arg="echo a",
         upstream_tasks=[]  # To be clear
    )
    wf_a.add_task(task_a)
    wf_a.bind()

    wf_b = tool.create_workflow(name="test_wf_chunks_b",
                                default_cluster_name="sequential",
                                default_compute_resources_set={"sequential": {"queue": "null.q"}},
                                chunk_size=10)
    task_b = task_template.create_task(
        arg="echo b",
        upstream_tasks=[]  # To be clear
    )
    wf_b.add_task(task_b)
    wf_b.bind()

    assert wf_a._chunk_size == 3
    assert wf_b._chunk_size == 10
