**********************
Core Jobmon Components
**********************

TODO: FILL IN AND UPDATE THIS PAGE
**********************************

Domain Objects
==============

Jobmon currently has four primary domain objects (Task, TaskInstance, Workflow, WorkflowRun). The domain objects are coupled into sets of two. Task and TaskInstance are one pair, and Workflow and WorkflowRun are another. Each pair is organized hierarchically into compute definitions and compute instances.

- TASK: a set of compute instructions; it is the intention to run a command on a cluster or in the cloud.
- WORKFLOW: a super instruction set of Tasks and how they relate to each other.
- TASK INSTANCE: an actualized compute instance of the compute instructions defined by a Task.
- WORKFLOW RUN: an actualized compute instance of the compute instructions defined by a Workflow.

The definition objects (Task and Workflow) can have many instances associated with them over time, but only one instantiated instance at any instant in time.

Stateful Deployment Units
=========================

Each domain object can be controlled by one of three stateful deployment units (client, distributor, worker node).

- CLIENT: user interface to Jobmon.
- DISTRIBUTOR: creates instances on a worker node.
- WORKER NODE: executes the compute instructions of an instance.

The web service is not considered a stateful actor because it cannot act, it can only transact. (perhaps this is incorrectly concieved??)
