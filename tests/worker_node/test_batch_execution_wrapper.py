from getpass import getuser
import os


from jobmon import PythonTask
from jobmon import Workflow


thisdir = os.path.dirname(os.path.realpath(os.path.expanduser(__file__)))


def test_sge_cli(real_jsm_jqs, db_cfg):

    job_name = "foo"
    log_dir = f'/ihme/scratch/users/{getuser()}'
    t1 = PythonTask(script=os.path.join(thisdir, 'fill_pipe.py'),
                    name=job_name, num_cores=1, max_runtime_seconds=600,
                    max_attempts=1)

    workflow = Workflow("my_simple_dag", project='proj_tools',
                        stderr=log_dir, stdout=log_dir)
    workflow.add_tasks([t1])
    workflow.run()

    # check db
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        query = """
        SELECT description
        FROM job_instance_error_log"""
        res = DB.session.execute(query).fetchone()
        DB.session.commit()
    error = res[0]
    assert error == (("a" * 2**10 + "\n") * (2**8))[-10000:]

    # check logs
    with app.app_context():
        query = """
        SELECT executor_id
        FROM job_instance"""
        executor_id = DB.session.execute(query).fetchone()[0]
        DB.session.commit()

    # check stderr
    with open(os.path.join(log_dir, f"{job_name}.e{executor_id}"), "r") as f:
        content = f.read()
    assert content == ("a" * 2**10 + "\n") * (2**8)

    # check stdout
    with open(os.path.join(log_dir, f"{job_name}.o{executor_id}"), "r") as f:
        content = f.read()
    assert content == ("a" * 2**10 + "\n") * (2**8)
