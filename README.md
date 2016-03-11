# Job Monitor
The package intends to provide simple, central monitoring of statuses and errors encountered by distributed tasks. It seeks to easily drop-in to existing code bases without significant refactoring.

Login to separate nodes and try out the client/server code below. A sqlite file named *job_monitor.sqlite* should show up in ~ (or whatever directory you specify) containing a log of status updates and errors.

## Server node
```python
import monitor
m = monitor.JobMonitor("~")
m.run()
```

## Client node(s)
```python
import job
import time
j1 = job.Job("~", jid=12345)
j1.start()
time.sleep(5)
j1.finish()
```
```python
import job
import time
j2 = job.Job("~", jid=98765)
j2.start()
time.sleep(2)
j2.log_error('Uh oh! Something went wrong!')
j2.failed()
```
# Job Queing and automatic relaunching
Job Monitor comes with high level convenience packages for launching SGE jobs,
monitoring/logging their progress, and relaunching failed jobs.

## Launch script
```python
from jobmon.qmaster import MonitoredQ

# start job queue before submitting jobs
log_dir = './logs'
jobQueue = MonitoredQ(log_dir, retries=1)
jobQueue.start_monitor(log_dir)

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
