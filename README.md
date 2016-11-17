# Job Monitor
The package intends to provide simple, central monitoring of statuses and errors encountered by distributed tasks.
It seeks to easily drop-in to existing code bases without significant refactoring.

The monitor must run in py 3.5 because sqlite cannot write to an NFS file system in python 2

Therefore that process has a different python/conda env than the distributed jobs.

This discussion deliberately avoids the words _Client_ and _Server_ because those words were used in different ways
in the original implementation.

There is one central python process (CentralJobMonitor) that starts and monitors the individual application jobs.
Teh CentralJobMonitor is actually started by qmaster. A call to its constructor creates an (in process) instance of
CentralJobMonitorLauncher, which then uses popen to start CentralJobMonitor process (if it has not yet been started).
This central process contains a loop to start all the individual application jobs.

# Open Question
From where do the application tasks get their python environments?
QMaster accepts parameters for those two, but CentralComp and SciComp (i.e. GBD) need to define standard paths.


# Job Queing and automatic relaunching
Job Monitor comes with high level convenience packages for launching SGE jobs,
monitoring/logging their progress, and relaunching failed jobs.

## Central launch script
This process uses sqllite. sqllite can only write to an NFS file system in python 3, so this python process must be run
using a python 3 environment. The python process in the application swarm can use a different python environment

```python
from jobmon.qmaster import MonitoredQ

# start job queue before submitting jobs
log_dir = './logs'
jobQueue = MonitoredQ(log_dir, retries=1)
# This launches CentralJobMonitor

# launch SGE jobs that the Queue will track
# MonitoredQ.qsub will take a runfile (not necessarily python)
# and launch a sge job with the given parameters
for (arg1, arg2) in args_list:
    jobQueue.qsub(runfile=foo.py,
                  jobname='foo_{}_{}'.format(arg1, arg2),
                  parameters=[arg1, arg2]
                  project='my_proj',
                  slots=10,
                  memory=20)

# the queue will periodically poll that status of all launched jobs
# and attempt to relaunch failed jobs
jobQeue.qblock()
```

# Job logging
The job_monitor.sqlite file is created in your logging directory. You can open
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

Sqlite doesn't support concurrent access, so don't do this while 
jobmon server is running! Instead, you can start up another job.Job() in
the same directory, and use the query method.

## Dependencies
- pyzmq
