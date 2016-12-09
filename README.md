# Job Monitor
The package intends to provide simple, central monitoring of statuses and errors encountered by distributed tasks.
It seeks to easily drop-in to existing code bases without significant refactoring.

The monitor must run in py 3.5 because sqlite cannot write to an NFS file system in python 2.

Therefore that process has a different python/conda env than the distributed jobs.

This discussion deliberately avoids the words _Client_ and _Server_ because those words were used in different ways
in the original implementation.

This is the thrid iteration of the job mon design. The change betwee neach iteration is how the central job monitor is
launched. In the first iteration it was started manually in a separate shell window. In the secon diteration it was
started by the clint calling MonitoredQ. That appeared to be simpler but in practise was too complex, especially in
controlling which python environment was used for which process. Teh thir diteration therefore went back to manually
staing the monitor.

There is one central python process (CentralJobMonitor) that starts and monitors the individual application jobs.
This process should be started in sa spearte qlogin, using a python 3 environment (which will be part of this project
as soon as we have the central envrinemnt library epic completed.).  The command is:

```sh
python bin/launch_central_monitor.py <directory to hold monitor_info.json>
```

More usefully:

```sh
rm -f monitor_info.json && python bin/launch_central_monitor.py `pwd`
```

# Open Question
From where do the application tasks get their python environments?


# Job Queueing and Automatic Relaunching
Job Monitor comes with high level convenience packages for launching SGE jobs,
monitoring/logging their progress, and relaunching failed jobs.

## Central launch script
This process uses sqlite. sqlite can only write to an NFS file system in python 3, so this python process must be run
using a python 3 environment. The python process in the application swarm can use a different python environment.
Juts be careful which files you import. The response codes were moved to exceptions.py so that the client would be
decoupled from the responder and its pythno 3 requirement.

```python
from jobmon import sge

# launch SGE jobs that the Queue will track
# MonitoredQ.qsub will take a runfile (not necessarily python)
# and launch a sge job with the given parameters
for (arg1, arg2) in args_list:
    job_id = sge.qsub(runfile=foo.py,
                  jobname='foo_{}_{}'.format(arg1, arg2),
                  parameters=[arg1, arg2]
                  project='my_proj',
                  slots=10,
                  memory=20)

# the queue will periodically poll that status of all launched jobs
# and attempt to relaunch failed jobs
dalynator.wait_for_jobs_complete()   #TBD that code needs to be moved into CC?jobmon
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
