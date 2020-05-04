from getpass import getuser
import os
from time import sleep
import pytest

thisdir = os.path.dirname(os.path.realpath(os.path.expanduser(__file__)))


def wait_for_file(filepath: str) -> bool:
    """Waits a few times to see if it appears, asserts if it takes too long"""
    num_tries = 0
    while not os.path.exists(filepath):
        sleep(10)
        num_tries += 1
        assert num_tries < 20
    return True


def test_sequential(db_cfg, client_env):
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.templates.bash_task import BashTask

    t1 = BashTask(command=f"python {os.path.join(thisdir, 'fill_pipe.py')}",
                  executor_class="SequentialExecutor",
                  name="foo",
                  num_cores=1,
                  max_runtime_seconds=600,
                  m_mem_free='1G',
                  max_attempts=1)

    workflow = UnknownWorkflow("sequential_simple_workflow", project='proj_scicomp',
                               executor_class="SequentialExecutor",
                               seconds_until_timeout=300)
    workflow.add_tasks([t1])
    workflow.run()

    # check db
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        query = "SELECT description " \
                "FROM task_instance_error_log t1, task_instance t2, workflow_run t3 " \
                "WHERE t1.task_instance_id=t2.id " \
                "AND t2.workflow_run_id=t3.id " \
                "AND t3.workflow_id={}".format(workflow.workflow_id)
        res = DB.session.execute(query).fetchone()
        DB.session.commit()

    error = res[0]
    assert error == (("a" * 2**10 + "\n") * (2**8))[-10000:]


@pytest.mark.integration_sge
def test_sge_cli(db_cfg, client_env):
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.templates.bash_task import BashTask

    job_name = "foo"
    log_dir = f'/ihme/scratch/users/{getuser()}'
    t1 = BashTask(command=f"python {os.path.join(thisdir, 'fill_pipe.py')}",
                  executor_class="SGEExecutor",
                  name=job_name,
                  num_cores=1,
                  max_runtime_seconds=600,
                  m_mem_free='1G',
                  max_attempts=1)

    workflow = UnknownWorkflow(name="sge_cli_simple_workflow", project='proj_scicomp',
                               stderr=log_dir, stdout=log_dir,
                               executor_class="SGEExecutor",
                               seconds_until_timeout=300)
    workflow.add_tasks([t1])
    workflow.run()

    # check db
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        query = """
        SELECT tiel.description
        FROM task_instance_error_log tiel
        JOIN task_instance ti
            ON tiel.task_instance_id = ti.id
        JOIN task
            ON ti.task_id = task.id
        WHERE task_id = {}""".format(t1.task_id)
        res = DB.session.execute(query).fetchone()
        DB.session.commit()

    error = res[0]
    assert error == (("a" * 2**10 + "\n") * (2**8))[-10000:]

    # check logs
    with app.app_context():
        query = """
        SELECT executor_id
        FROM task_instance ti
        JOIN task
            ON ti.task_id = task.id
        WHERE task_id = {}""".format(t1.task_id)
        executor_id = DB.session.execute(query).fetchone()[0]
        DB.session.commit()

    # check stderr
    # Be careful, it can take a little while to appear
    stderr_name = os.path.join(log_dir, f"{job_name}.e{executor_id}")
    wait_for_file(stderr_name)
    with open(stderr_name, "r") as f:
        content = f.read()
    assert ("a" * 2**10 + "\n") * (2**8) in content

    # check stdout
    stdout_name = os.path.join(log_dir, f"{job_name}.o{executor_id}")
    wait_for_file(stdout_name)
    with open(stdout_name, "r") as f:
        content = f.read()
    assert ("a" * 2 ** 10 + "\n") * (2 ** 8) in content
