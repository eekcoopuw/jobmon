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

# Deployment architecture
![deploy_arch_diagram](https://hub.ihme.washington.edu/download/attachments/44702059/Screen%20Shot%202017-10-18%20at%202.49.30%20PM.png?version=1&modificationDate=1508363448371&api=v2)

## TODOs
1. Create a launcher, which does absolutely nothing but launches the J-state-mg and j-query-server
1. jobmonrc reduces to just the database address
1. We create an epic for a watcher on the purple node, MVP is just a heartbeat and a slack channel


# Job State Manager
The package intends to provide simple, central monitoring of statuses and errors encountered by distributed tasks.
It seeks to easily drop-in to existing code bases without significant refactoring.

There is one central python process (JobStateManager) that keeps track of and forces consistency between individual jobs and their instances.
This process must be launched on a host and listen on ports known by all jobs which will communicate with it.


# Environments
**TODO**: The distributed tasks are executed in the same
environment as the JobListManager process by default. This can be overriden by
setting the Job.environment attribute.


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
