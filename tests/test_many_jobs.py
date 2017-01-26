import json
import os
import subprocess
import time

import pandas as pd
import pytest

from jobmon import qmaster, central_job_monitor
from jobmon.models import Status

here = os.path.dirname(os.path.abspath(__file__))
monitor_dir = "/ihme/scratch/tmp/tests"

@pytest.mark.cluster
def test_many_jobs(central_jobmon):

        conda_info = json.loads(
            subprocess.check_output(['conda', 'info', '--json']))
        path_to_conda_bin_on_target_vm = '{}/bin'.format(conda_info['root_prefix'])
        conda_env = conda_info['default_prefix'].split("/")[-1]
        q = qmaster.MonitoredQ(
            central_jobmon.out_dir,
            path_to_conda_bin_on_target_vm,
            conda_env)

        for name in ["_" + str(num) for num in range(1, 101)]:
            q.qsub(
                os.path.join(here, "waiter.py"),
                name,
                parameters=[10],
                stderr=monitor_dir,
                stdout=monitor_dir)

        q.qblock(poll_interval=20)
        r = q.request_sender.send_request(
            {"action": "query",
             "args": ["select * from job "
                      "where current_status != {}".format(Status.COMPLETE)]
            }
        )
        assert len(pd.DataFrame(r[1])) == 0
