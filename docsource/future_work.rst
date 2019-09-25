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


Dynamic Resource Allocation
------------------------------------------------

Improve jobmon code base by allowing the user to dynamically calculate resource requirements once a job is ready to be scheduled.

Backgroud
^^^^^^^^^^

Users have requested the ability to pass a generator function to each task which would compute executor specific resource requirements at runtime. Examples such as memory and runtime do not affect the structure of a jobmon workflow, which allows for the possibility of delaying evaluating these values and/or dynamically computing them right before a task is ready to be scheduled.

Motivation
^^^^^^^^^^^

 - A task may not be able to determine it's resource requirements a priori therefore delayed evalution of resources is a necessity for many users.
 - Reduce locations where executor parameters must be imported reducing code complexity.

Design rational
^^^^^^^^^^^^^^^^

A long term goal for jobmon is to ensure that the Executor thread remains easy to factor out into an individual service. This will help facilitate any future transition to Kubernetes.

With that in mind, a primary concern when implementing dynamic resources is ensuring the resource generating/modifying function is resident only in the swarm thread. Doing so avoids the need to communicate arbitrary functions via the database/JSM using pickle.

We can accomplish this feature by formalizing the "Adjusting" state in the job FSM. It will be a non-transient state that accurs after a job is "Registered" and before it moves to "Queued for Instantiation" or after a job_instance hits a resource error.

When a dag is being created for the first time all jobs would be in registered state. The TaskDag object then detects the fringe of the dag and moves these jobs into "Ajusted." From there the JobListManager collects jobs in "done", "error" and "adjusting". Jobs in adjusting call the generating function associated with a given task and move the job to "Queued for instantiation."

Alternatively if a job hits a resource error, the job would be moved back into "adjusting." The swarm thread would collect those jobs via the JobListManager and call the adjust function instead of the generating function.

Code Changes
^^^^^^^^^^^^^

This design would require changes to job_state_manager, JobInstanceFactory, Task, TaskDag, and JobListManager.


The first step would be to modify job_state_manager to be able to add a job without adding executor parameters.

jobmon/server/job_state_manager/job_state_manager.py 112-127, Remove:

.. literalinclude:: ../jobmon/server/job_state_manager/job_state_manager.py
    :lines: 112-127

At the same time add valid state transitions in the model for REGISTERED -> ADJUSTING_RESOURCES and ADJUSTING_RESOURCES -> QUEUED_FOR_INSTANTIATION. Remove REGISTERED -> QUEUED_FOR_INSTANTIATION.

jobmon/models/job.py 89-99, remove:

.. literalinclude:: ../jobmon/models/job.py
    :lines: 89-99

JobInstanceFactory is no longer responsible for adjusting resources.

jobmon/client/swarm/job_management/job_instance_factory.py 119-141, remove:

.. literalinclude:: ../jobmon/client/swarm/job_management/job_instance_factory.py
    :lines: 119-141

JobInstanceFactory is now responsible for validating any executor parameter set that is "Original" or "Adjusting". In order to accomplish this we need to serialize the current executor parameter set type. This includes modifications to the following:

jobmon/serializers.py 5-45:

.. literalinclude:: ../jobmon/serializers.py
    :lines: 5-45

jobmon/models/job.py 16-43:

.. literalinclude:: ../jobmon/models/job.py
    :lines: 16-43

Then add the validated executor parameters to the db if the parameters are currently "Original" or "Adjusting" similar to:

jobmon/client/swarm/job_management/job_instance_factory.py 137-140:

.. literalinclude:: ../jobmon/client/swarm/job_management/job_instance_factory.py
    :lines: 137-140

The change to Task is conceptually concise. The executor_parameters parameter is now a generating function instead of a concrete instance. The generating function is either a user specified function that is lazily evaluated only once the job is ready to be instantiated, or it is the current resource adjustment function that gets executed when a job hits a resource error. The changes would be confined to the __init__ method. The goal is to convert executor parameters into a callable for a unified API inside of TaskDag.

.. code-block:: python

        if executor_parameters is None:
            executor_parameters = ExecutorParameters(
                slots=slots,
                num_cores=num_cores,
                mem_free=mem_free,
                m_mem_free=m_mem_free,
                max_runtime_seconds=max_runtime_seconds,
                queue=queue,
                j_resource=j_resource,
                context_args=context_args,
                resource_scales=resource_scales,
                hard_limits=hard_limits,
                executor_class=executor_class)

        if isinstance(executor_parameters, ExecutorParameters):
            is_valid, msg = executor_parameters.is_valid()
            if not is_valid:
                logger.warning(msg)
            func = lambda executor_parameters: executor_parameters
            executor_parameters = partial(
                lambda executor_parameters: executor_parameters,
                executor_parameters)
        else:
            self.executor_parameters = partial(executor_parameters, self)

.. note::
    There may be a motivation to unify BoundTask and Task. If a user is dynamically generating resources, the object they use to infer usage is the task object itself. Currently, task does not have access to FSM info about a task such as job_id, status, etc.

Changes to JobListManager would be in the _create_job() and parse_done_and_errors(). In _create_job() we would no longer provide executor parameter arguments.

jobmon/client/swarm/job_management/job_list_manager.py 96-115

.. literalinclude:: ../jobmon/client/swarm/job_management/job_list_manager.py
    :lines: 96-115

In parse_done_and_errors() the method must now return jobs that are in adjusting states.

Changes to TaskDag would be in _execute(). We need a new loop to address jobs in adjusting state. The loop would add the executor parameters to the db but evaluating the executor parameters callable on the task object. It would also reset the callable to the standard scaling function that we currently use so that if a job hits a resource error it would get scaled normally.

jobmon/client/swarm/workflow/task_dag.py 228-234

.. literalinclude:: ../jobmon/client/swarm/workflow/task_dag.py
    :lines: 228-234

.. note::
    This design opens up the possibility of users providing custom scaling functions for resource errors similar to the custom function they provide for the initial resource request.

Testing Strategy
^^^^^^^^^^^^^^^^^
