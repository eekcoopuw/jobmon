Future Work
============

Create JobInstanceManager Abstraction
--------------------------------------

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

.. literalinclude:: ../jobmon/client/swarm/job_management/job_list_manager.py
    :lines: 19-24

After a refactor, we would end up with 2 different classes.

.. code-block:: python

    class JobListManager(object):

        def __init__(self, dag_id, requester=shared_requester):
            pass


    class JobInstanceManager(object):

        def __init__(self, dag_id, executor=None, n_queued_jobs=1000,
                          requester=shared_requester):
            pass


By splitting the state management into 2 classes, we no longer need to pass arguments for the proposed JobInstanceManager down from Workflow -> TaskDag -> JobListManager -> JobInstanceFactory + JobInstanceReconciler. A more natural place for this class to be spawned would be at the workflow level where the executor is instantiated. I would suggest bundling the executor and JobInstanceManager into a separate swarm node service that runs in a separate process and gets launched during workflow instantiation.

Conclusion
^^^^^^^^^^

A few intuitive changes to which swarm objects have access to JobInstance management will result in a more intuitive and flexible code base and the option for adding more execution platforms in the future. The option for more pluggable and diverse distributed computing ecosystems within jobmon should lead to higher uptake in the open source community, while at the same time providing a pathway for internal applications to run on the public cloud.

It’s not science if someone else can’t repeat it…


More Granularity In JobInstance **Error** States
------------------------------------------------

Improve jobmon code base by distinguishing between application errors and infrastructure errors in the JobInstance finite state machine.

Backgroud
^^^^^^^^^^

The JobInstance finite state machine is a set of routes exposed by the job_state_manager service which track an job instances state as it progresses from created to done. Specifically: add_job_instance, log_executor_id, log_done, log_error, log_ji_report_by, reconcile,
log_running.

These routes are currently accessed by the JobInstanceFactory, JobInstanceReconciler, and the JobInstanceIntercom. The JobInstanceFactory waits for Jobs to be moved into queued state by the JobListManager. Then it creates new job_instances (add_job_instance) and submits them to the executor (log_executor_id). The JobInstanceIntercom is an api used to send messages from the worker nodes about the state of a given JobInstance (log_running, log_ji_report_by, log_done, log_error). The JobInstanceReconciler monitors the executor and makes sure that jobs the executor thinks are running or submitted also log a heartbeat (log_ji_report_by). It also ensures that any job instances that mysteriously die or time out have their state subsequently updated (log_error).

Motivation
^^^^^^^^^^^

 - The user is currently unable to quickly distinguish between an application error and an infrastructure error.
 - Jobmon's retry logic is one of it's most useful features, but it is wasteful to retry a job that is failing due to an application programming error because the programming error won't automatically repair itself whereas an infrastructure error may succeed on subsequent tries.
 - Jobmon's health monitor does not report useful information when it logs suspicious nodes right now because it captures all errors, not infrastructure errors.
 - Separating system errors from application errors could facilitate adding future system error subtypes such as resource allocation errors, enabling applications to dynamically configure how much resources they need to request to run successfully.


Code Changes
^^^^^^^^^^^^^

The cleanest way to implement this change is to keep all errors logged by the worker_node the same. Errors discovered by reconciliation or the executor should have separate states.

This change would require the addition of new states to models.job_instance_status and an expansion of the allowed state transitions in models.job_instance.valid_transitions.

The JobInstanceReconciler and the reconcile route in the job_state_manager would need to log a separate class of errors.

There is good reason to have a new state if UGE does not properly return an executor_id during qsub. This likely means the job entered error state immediately and therefore should be moved to ERROR_FATAL immediately.

models.job.transition() and models.job.transition_to_error() would need to be updated to make sure that jobs only transition to ERROR_RECOVERABLE when the error is a system error not an application programming error.

server.health_monitor.HealthMonitor._calculate_node_failure_rate() would need to be updated to incorporate the correct job_instance_status values into it's monitoring.

Conclusion
^^^^^^^^^^

More granularity in the JobInstance state machine would be a big quality of life improvement for both users and developers.


Executor Specific Timeouts
--------------------------

Improve timeout functionality by clarifying which classes are responsible for terminating timed out jobs.

Backgroud
^^^^^^^^^^

One of jobmon's key features has been its ability to terminate job instances that exceed a max runtime. Initially, this was implemented by adding a timer inside the worker node which would terminate the job's command locally. This approach was abandoned because the worker node process can be accidentally killed which means those jobs would never time out.

On cluster-prod
