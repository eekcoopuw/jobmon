Future Work
============

Create JobInstanceManager Abstraction
--------------------------------------

Goal
^^^^^

Improve jobmon code base by removing JobInstanceFactory, JobInstanceReconciler, and Executor, from the JobListManager. Create new abstraction called JobInstanceManager to encapsulate factored out classes.

Backgroud
^^^^^^^^^^

The JobListManager is responsible for exposing Job state to a TaskDag running on a remote host. The user does not interact with it directly. The JobInstanceFactory serves as a scheduling proxy for the JobStateManager, listening for Jobs to be queued for execution and creating JobInstances. The JobInstanceReconciler communicates state between an Executor and the JobStateManager.

Motivation
^^^^^^^^^^^

The job list manager combines two distinct functionalities that should be separated: 1) tracking job state, and 2) tracking job instance state.

 - The user’s primary interaction is at the Workflow level, so any changes to the JobListManager API would be transparent to users.
 - Jobmon’s data model draws a conceptual distinction between Job state and JobInstance state by separating them into two separate and distinct finite sets. The client-side application only partially separated the management of those states.
 - The JobListManager creates a JobInstanceFactory and a JobInstanceReconciler in 2 separates threads and never communicates with them again. All state changes are messaged via the finite state machine on the server via http.
 - The JobInstanceFactory and JobInstanceReconciler are the only objects that interact with the execution platform (UGE, sequential, etc) at any point in the entire code base.
 - A DAG of Jobs could have a reason to execute a subset of jobs on different types of executors. Splitting the functionalities allows Jobmon to implement this in a straightforward way. Eg (upload jobs vs. compute jobs)
 - Being able to easily switch between executors makes our code and therefore our science more portable.

Code Changes
^^^^^^^^^^^^^

In the current JobListManager, all arguments except for dag_id and requester are passed through to the JobInstanceReconciler and JobInstanceFactory. Currently this is the API.

