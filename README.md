# Demo instructions

To run some jobs on the cluster, including retry/timeout functionality
try this:

```python

# Before running this script, install a ~/.jobmonrc file... Run or mimic
# install_rcfile.py to do so

from jobmon.job_list_manager import JobListManager
from jobmon.job_instance_factory import execute_sge

# Create a JobListManager and start it's status and instance services
jlm = JobListManager.in_memory(executor=execute_sge, start_daemons=True)

# Create and queue some jobs (in this dev case, queue = run immediately)
job_id = jlm.create_job("touch ~/foobarfile", "my_jobname")
jlm.queue_job(job_id)

for i in range(5):
    slots = i+1
    mem = slots*2
    job_id = jlm.create_job("sleep {}".format(i), "sleep{}".format(i),
                            slots=slots, mem_free=mem)
    jlm.queue_job(job_id)

# Block until everything is done
done, errors = jlm.block_until_no_instances()
print("Done: {}".format(done))  # Done: [1, 2, 3, 4, 6, 5]
print("Errors: {}".format(errors))  # Errors: []


# Submit some with timeouts and retries
for i in range(5, 31, 5):
    job_id = jlm.create_job("sleep {}".format(i),
                            "sleep{}".format(i),
                            max_attempts=3,
                            max_runtime=12)
    jlm.queue_job(job_id)

# Block until everything is done
done, errors = jlm.block_until_no_instances(raise_on_any_error=False)
print("Done: {}".format(done))  # Done: [7, 8, 9, 10]
print("Errors: {}".format(errors))  # Errors: [11, 12]

```


# Dev instructions (subject to rapid iteration)

To develop locally, you'll need docker and docker-compose. Clone this repo and
start the database and JobStateManager (formerly 'monitor') server:

```
docker-compose up --build
```

For the client side, in a separate shell, create a python 3 environment with
jobmon installed. Simplest case usage is as follows:


# Running tests

To run the tests, the database and JobStateManager must be running:
```
docker-compose up --build
```

Tests can then be run locally. It is recommended to run them without the cache.
The test threads seem to lock up sometimes, and clearing the cache helps. Need
to investigate further. My hunch is the issues with SUB processes may be
related to this: https://github.com/zeromq/pyzmq/issues/983.
```
pytest --cache-clear tests
```

# Job State Manager
The package intends to provide simple, central monitoring of statuses and errors encountered by distributed tasks.
It seeks to easily drop-in to existing code bases without significant refactoring.

There is one central python process (JobStateManager) that keeps track of and forces consistency between individual jobs and their instances.
This process must be launched on a host and listen on ports known by all jobs which will communicate with it.

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
**TODO**: The distributed tasks are executed in the same environment as the JobListManager process by default. This can be overriden by setting the
Job.environment attribute.


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
    cjm = central_job_monitor.CentralJobMonitor(mocks, persistent=False)
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
