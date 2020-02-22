from getpass import getuser
import os
from time import sleep
from unittest import mock

from jobmon.client.templates.unknown_workflow import UnknownWorkflow as Workflow
from jobmon.client.templates.bash_task import BashTask

thisdir = os.path.dirname(os.path.realpath(os.path.expanduser(__file__)))


def wait_for_file(filepath: str) -> bool:
    """Waits a few times to see if it appears, asserts if it takes too long"""
    num_tries = 0
    while not os.path.exists(filepath):
        sleep(5)
        num_tries += 1
        assert num_tries < 8
    return True


def test_sge_cli(db_cfg, client_env):
    job_name = "foo"
    log_dir = f'/ihme/scratch/users/{getuser()}'
    t1 = BashTask(command=f"python {os.path.join(thisdir, 'fill_pipe.py')}",
                  executor_class="SGEExecutor",
                  name=job_name,
                  num_cores=1,
                  max_runtime_seconds=600,
                  m_mem_free='1G',
                  max_attempts=1)

    workflow = Workflow("simple_workflow", project='proj_scicomp',
                        stderr=log_dir, stdout=log_dir)
    workflow.add_tasks([t1])
    workflow.run()

    # check db
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        query = """
        SELECT description
        FROM task_instance_error_log"""
        res = DB.session.execute(query).fetchone()
        DB.session.commit()

    error = res[0]
    assert error == (("a" * 2**10 + "\n") * (2**8))[-10000:]

    # check logs
    with app.app_context():
        query = """
        SELECT executor_id
        FROM task_instance"""
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



