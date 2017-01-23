import os
import time
import pytest
import pandas as pd
from jobmon import qmaster, central_job_monitor

here = os.path.dirname(os.path.abspath(__file__))
monitor_dir = "/ihme/scratch/tmp/tests"

@pytest.mark.cluster
def test_many_jobs():

    try:
        cjm = central_job_monitor.CentralJobMonitor(monitor_dir)
        time.sleep(20)
        q = qmaster.MonitoredQ(
            cjm.out_dir,
            "/ihme/code/central_comp/miniconda/bin",
            "tasker")

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
             "args": ["select * from job where current_status != 4"]})
        assert len(pd.DataFrame(r[1])) == 0
    finally:
        cjm.stop_responder()
