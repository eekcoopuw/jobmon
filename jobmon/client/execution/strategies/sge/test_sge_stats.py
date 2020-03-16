import os
import pytest
from unittest.mock import patch

from jobmon.client.execution.strategies.sge.sge_executor import SGEExecutor
from jobmon.client.execution.strategies.sge.sge_executor import ERROR_CODE_SET_KILLED_FOR_INSUFFICIENT_RESOURCES as ecir
from jobmon.exceptions import RemoteExitInfoNotAvailable, ReturnCodes
from jobmon.models.task_instance_status import TaskInstanceStatus
from jobmon.client.templates.bash_task import BashTask


@pytest.mark.integration_sge
def test_real_dag_logging(db_cfg, tmp_out_dir, real_dag):
    """
    Create a real_dag with one Task and execute it, and make sure logs show up
    in db

    This is in a separate test from the jsm-specifc logging test, as this test
    runs the jobmon pipeline as it would be run from the client perspective,
    and makes sure the qstat usage details are automatically updated in the db,
    as well as the created_date for the real_dag
    """
    root_out_dir = "{}/mocks/test_real_dag_logging".format(tmp_out_dir)
    makedirs_safely(root_out_dir)
    command_script = sge.true_path(f"{path_to_file}/remote_sleep_and_write.py")

    output_file_name = "{}/test_real_dag_logging/mock.out".format(tmp_out_dir)
    task = SleepAndWriteFileMockTask(
        command=("python {cs} --sleep_secs 1 --output_file_path {ofn} "
                 "--name {n}" .format(cs=command_script, ofn=output_file_name,
                                      n=output_file_name)))
    real_dag.add_task(task)
    os.makedirs("{}/test_real_dag_logging".format(tmp_out_dir))
    (rc, num_completed, num_previously_complete, num_failed) = \
        real_dag._execute()

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        ji = DB.session.query(JobInstance).first()
        assert ji.usage_str  # all these should exist and not be empty
        assert ji.maxrss
        assert ji.cpu
        assert ji.io
        assert ji.nodename
        assert ':' not in ji.wallclock  # wallclock should be in seconds

        td = DB.session.query(TaskDagMeta).first()
        print(td.created_date)
        assert td.created_date  # this should not be empty