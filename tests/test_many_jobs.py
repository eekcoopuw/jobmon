import json
import os
import subprocess

import pandas as pd
import pytest

from jobmon import qmaster, executors
from jobmon.models import Status

here = os.path.dirname(os.path.abspath(__file__))


@pytest.mark.cluster
def test_many_jobs(central_jobmon):

    conda_info = json.loads(
        subprocess.check_output(['conda', 'info', '--json']).decode())
    path_to_conda_bin_on_target_vm = '{}/bin'.format(conda_info['root_prefix'])
    conda_env = conda_info['default_prefix'].split("/")[-1]

    # construct executor
    sgexec = executors.SGEExecutor(
        central_jobmon.out_dir, 3, 30000, path_to_conda_bin_on_target_vm,
        conda_env,
        parallelism=50)

    q = qmaster.MonitoredQ(sgexec)

    runfile = os.path.join(here, "waiter.py")
    for name in ["_" + str(num) for num in range(1, 101)]:
        j = q.create_job(
            jobname=name,
            runfile=runfile,
            parameters=[30])
        q.queue_job(j)

    q.check_pulse()

    r = q.request_sender.send_request(
        {"action": "query",
         "args": ["select * from sge_job "
                  "where current_status != {}".format(Status.COMPLETE)]
         }
    )
    assert len(pd.DataFrame(r[1])) == 0
