import pytest

from jobmon.client.execution.strategies.base import ExecutorParameters
from jobmon.models.task_instance import TaskInstance
from jobmon.client.execution.strategies.sge.sge_executor import SGEExecutor

from sqlalchemy.sql import text


@pytest.mark.jenkins_skip
@pytest.mark.integration_sge
def test_resource_scaling(db_cfg, client_env):
    """test that resources get scaled up on a resource kill"""
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.api import BashTask

    my_wf = UnknownWorkflow(workflow_args="resource starved workflow",
                            executor_class="SGEExecutor",
                            project="proj_scicomp")

    # specify SGE specific parameters
    sleepy_params = ExecutorParameters(
        num_cores=1,
        m_mem_free="1G",
        max_runtime_seconds=50,  # set max runtime to be shorter than task
        queue="all.q",
        executor_class="SGEExecutor")
    sleepy_task = BashTask(
        # set sleep to be longer than max runtime, forcing a retry
        "sleep 90",
        # job should succeed on second try. runtime will 150s on try 2
        max_attempts=3,
        executor_parameters=sleepy_params)
    my_wf.add_task(sleepy_task)

    # job will time out and get killed by the cluster. After a few minutes
    # jobmon will notice that it has disappeared and ask SGE for exit status.
    # SGE will show a resource kill. Jobmon will scale all resources by 30% and
    # retry the job at which point it will succeed.
    my_wf.run()

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        query = """
        SELECT
            *
        FROM
            task_instance
        WHERE
            task_instance.task_id = :task_id"""

        task_instances = DB.session.query(TaskInstance).from_statement(
            text(query)).params(task_id=sleepy_task.task_id).all()
        DB.session.commit()
        assert len(task_instances) == 3
        assert task_instances[0].status == "Z"
        assert task_instances[1].status == "Z"
        assert task_instances[2].status == "D"


@pytest.mark.jenkins_skip
@pytest.mark.integration_sge
def test_workflow_resume_new_resources(db_cfg, client_env):
    """test that new executor parameters get used on a resume"""
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.api import BashTask

    sge_params = ExecutorParameters(
        max_runtime_seconds=8,
        resource_scales={'m_mem_free': 0.2,
                         'max_runtime_seconds': 0.3})
    task = BashTask(name="rerun_task", command="sleep 10", max_attempts=1,
                    executor_parameters=sge_params)
    wf = UnknownWorkflow(workflow_args="rerun_w_diff_resources",
                         project="proj_scicomp")
    wf.add_task(task)

    wfr = wf.run()
    assert wfr.status == 'E'

    sge_params2 = ExecutorParameters(
        max_runtime_seconds=40,
        resource_scales={'m_mem_free': 0.4,
                         'max_runtime_seconds': 0.5})

    task2 = BashTask(name="rerun_task", command="sleep 10",
                     max_attempts=2, executor_parameters=sge_params2)
    wf2 = UnknownWorkflow(workflow_args="rerun_w_diff_resources",
                          project="proj_scicomp", resume=True)
    wf2.add_task(task2)

    wfr = wf2.run()
    assert wfr.status == 'D'

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        query = """
        SELECT
            *
        FROM
            task_instance
        WHERE
            task_instance.task_id = :task_id
        ORDER BY task_instance.id DESC"""

        ti = DB.session.query(TaskInstance).from_statement(
            text(query)).params(task_id=task2.task_id).first()

        assert ti.executor_parameter_set.max_runtime_seconds == 40
        assert ti.task.max_attempts == 2
        DB.session.commit()
