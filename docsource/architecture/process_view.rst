
Asynchronous Services
*********************

JObmon has two cron-like asynchronos services. They also are deployed in kubernetes but do
not expose any web routes.

Usage Integrator
================

UGE does not accurately record memory usage, specifically Resident Set Size (RSS).
Therefore an aditional memory profiling ssytem named qpid was developed.
The information it gathered was copied into the jobmon database, to the TaskInstance
table. This copying makes Resource Prediction easier becasue all the resource data is in one
database.

Therefore the copying of usage data was continued with the slurm port.

The usage integration code is not part of the main jobmon server.
It is in the jobmon_ihme_tad repository and is deployed as a separate Kubernetes service.
It runs asynchronously, working on a queue of jobs that have been launched but for which
there is no resource data. If the jobmon database shows that the job has completed successfully,
the usage integration coopies the resource data from Slurm, although it might take some time
for that data to appear. If the job failed, the usage integrator removes it from its watch-list

The Jobmon side of the code is jobmon.server.squid_integration.
This is deployed on Kubernetes as a Deployment. Essentially it is while-forever loop
that queries qpid on the ``jobmaxpss`` route to get the maxpss for each completed jobmon job.
It only queries for taks-instances that have recently completed and for which Jobmon does not
yet have QPID resource usage.

The Reaper
==========

Jobmon wraps the actual application in bash and Python. That Python calls routes
on the Jobmon server when the job starts, stops, or fails with an exception.
The wrapper is running asynchronosly from the application code, so the wrapper
can send back  "I am alive" heartbeats to the jobmon server.
However, if the node on which the Job is running goes down, then Jobmon never
receives notification that the job finished, because Jobmon's wrapper disappeared
with the node. Jobs sometime die without trace for other unknown reasons.

The Python client also sends heartbetas, annoucning that the WorkflowRun is still
alive.

The Reaper is an asynchronous process that looks for Tasks and WorkflowRuns
that have not sent heartbeats or finished within a specified timeout period.
These Tasks
and WorkflowRuns are "reaped," which means that they are moved to a Failed (for unknonw reasons)
state. ALl that Jobmon knows is that they disappeared.
