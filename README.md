# Job Monitor
The package intends to provide simple, central monitoring of statuses and errors encountered by distributed tasks.
It seeks to easily drop-in to existing code bases without significant refactoring.

This discussion deliberately avoids the words _Client_ and _Server_ because those words were used in different ways
in the original implementation.

This is the thrid iteration of the job mon design. The change betwee neach iteration is how the central job monitor is
launched. In the first iteration it was started manually in a separate shell window. In the second iteration it was
started by the clint calling MonitoredQ. That appeared to be simpler but in practise was too complex, especially in
controlling which python environment was used for which process. The third diteration therefore went back to manually
staing the monitor.

There is one central python process (CentralJobMonitor) that starts and monitors the individual application jobs.
This process should be started in a separte qlogin, using a python 3 environment (which will be part of this project
as soon as we have the central envrinemnt library epic completed.).  The command is:

```sh
python bin/launch_central_monitor.py <directory to hold monitor_info.json>
```

More usefully:

```sh
rm -f monitor_info.json && python bin/launch_central_monitor.py `pwd`
```

Alternatively, the (CentralJobMonitor) may be launched in a thread of the main controller process. This approach
allows the user to run a single process that is responsible for state tracking of the distributed processes. However,
since python does not allow concurrent execution of different threads, the main controller process that launches the
(CentralJobMonitor) should not do any computation. The command is:

```python
import time
from jobmon import central_job_monitor


try:
    cjm = central_job_monitor.CentralJobMonitor("foodir", persistent=False)
    time.sleep(5)
except:
    pass
else:
    # schedule some remote jobs and block execution of the finally till
    # they are done
finally:
    cjm.stop_responder()
    cjm.stop_publisher()
```

This process uses sqlite. sqlite can only write to an NFS file system in python 3, so this python process must be run
using a python 3 environment or use an in memory sqlite database (CentralJobMonitor(persistent=False)).
The response codes were moved to exceptions.py so that the client would be decoupled from the responder.

# Environments
The distributed tasks are generally executed in the same environment as the controller process though this is not
required.

# Job Queueing and Automatic Relaunching
Job Monitor comes with high level convenience packages for launching SGE jobs, monitoring/logging their progress, and
relaunching failed jobs. The functionality is encompassed in the (qmaster.JobQueue) class. JobQueue is an attempt to
abstract away state tracking and logging of distributed tasks across different distributed execution platforms
(SGE, Multiprocessing). It provides 2 useful abstractions: 1) executor 2) scheduler. The executor is responsible for
queueing jobs on a specified distribution platfor (eg. SGE). The scheduler is responsible for polling for job state
updates about the swarm jobs and exposing that information to the controller process. The scheduler is run in a separate
thread so don't run heavy computation in the same process as the scheduler is running. A useful pattern for a controller
process is to here:

```python
import time
from jobmon import qmaster, central_job_monitor
from jobmon.executors.sge_exec import SGEExecutor


try:
    cjm = central_job_monitor.CentralJobMonitor("/foo/dir", persistent=False)
    time.sleep(3)
except:
    pass
else:
    try:
        q = qmaster.JobQueue(cjm.out_dir, executor=SGEExecutor)
        j = q.create_job(
            runfile="bar.py",
            jobname="mock",
            parameters=["--baz", "1"])
        q.queue_job(j, slots=2, memory=4, project="ihme_general")
        q.block_till_done()  # monitor them
    except:
        cjm.generate_report()
finally:
    cjm.stop_responder()
    cjm.stop_publisher()
```

# Job logging
The job_monitor.sqlite file is created in your logging directory if you used persistent=True. You can open
it up and look for errors, completion times, and other statistics

    -bash-4.1$ sqlite3 job_monitor.sqlite
    SQLite version 3.9.2 2015-11-02 18:31:45
    Enter ".help" for usage hints.
    sqlite> .tables
    error       job         job_status  sgejob      status
    sqlite> select count(*) from error;
    1
    sqlite> select * from error;
    1|1|2016-03-10 01:23:08|Traceback (most recent call last):
      File "/share/code/test/joewag/under_development/format_dalynator_draws/jobmon/bin/monitored_job.py", line 28, in <module>
          execfile(args["runfile"])
            File "/ihme/code/test/joewag/under_development/format_dalynator_draws/formatter.py", line 169, in <module>
                raise RuntimeError('year is 1995!')
                RuntimeError: year is 1995!

    sqlite> .exit


Alternatively, (CentralJobMonitor.generate_report()) will create a csv of the sqlite database in the logging directory.

Sqlite doesn't support concurrent access, so don't do this while jobmon server is running! Instead, you can start up
another job.Job() in the same directory, and use the query method.


## Dependencies
- pyzmq
- pandas
- sqlalchemy
- numpy
- pymysql
- pyyaml
- drmaa
- jsonpickle
- subprocess32
