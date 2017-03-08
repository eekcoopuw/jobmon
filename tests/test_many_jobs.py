import os
import pandas as pd
from jobmon import qmaster
from jobmon.executors import local_exec
from jobmon.models import Status

here = os.path.dirname(os.path.abspath(__file__))


def test_many_jobs(central_jobmon):

    # construct executor
    localexec = local_exec.LocalExecutor(
        central_jobmon.out_dir, 3, 30000, parallelism=20)

    q = qmaster.MonitoredQ(localexec)

    runfile = os.path.join(here, "waiter.py")
    for name in ["_" + str(num) for num in range(1, 50)]:
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

    central_jobmon.generate_report()
    assert os.path.exists(
        os.path.join(central_jobmon.out_dir, "job_report.csv"))
    os.path.exists(
        os.path.join(central_jobmon.out_dir, "job_status_report.csv")
    )
