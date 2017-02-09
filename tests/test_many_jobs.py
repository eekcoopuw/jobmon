import os
import pandas as pd
from shutil import copy
from jobmon import qmaster
from jobmon.executors import local_exec
from jobmon.models import Status

here = os.path.dirname(os.path.abspath(__file__))


def test_many_jobs(central_jobmon_nopersist):

    # construct executor
    localexec = local_exec.LocalExecutor(
        central_jobmon_nopersist.out_dir, 3, 30000, parallelism=20)

    q = qmaster.MonitoredQ(localexec)

    runfile = os.path.join(here, "waiter.py")
    for name in ["_" + str(num) for num in range(1, 2)]:
        j = q.create_job(
            jobname=name,
            runfile=runfile,
            parameters=[10])
        q.queue_job(j)

    q.block_till_done(poll_interval=7)

    r = q.request_sender.send_request(
        {"action": "query",
         "args": ["select * from job_instance "
                  "where current_status != {}".format(Status.COMPLETE)]
         }
    )
    assert len(pd.DataFrame(r[1])) == 0

    q.request_sender.send_request({"action": "generate_report"})
    copy(os.path.join(central_jobmon_nopersist.out_dir, "job_report.csv"),
         "/Users/mlsandar/temp/job_report.csv")
