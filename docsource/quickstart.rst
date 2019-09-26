Quickstart
##########


Install
*******
To get started::

    pip install jobmon

.. note::
    If you get **Could not find a version that satisfies the requirement jobmon (from version: )** then create (or append) the following to ``~/.pip/pip.conf``::

        [global]
        extra-index-url = http://dev-tomflem.ihme.washington.edu/simple
        trusted-host = dev-tomflem.ihme.washington.edu

.. note::

    Jobmon is intended to be used on the SGE cluster. At present, it has
    limited capabilities for executing jobs locally on a single machine using
    either sequential exeuction or Multiprocessing. These local job-management
    capabilities will be improved going forward, but SGE support will always be
    the primary goal for the project.


Getting Started
***************
Users will primarily interact with jobmon by creating a :term:`Workflow` and iteratively
adding :term:`Task` to it. Each Workflow is uniquely defined by its
:term:`WorkflowArgs` and the set of Tasks attached to it. A Workflow can only
be re-loaded if the WorkflowArgs and all Tasks added to it are shown to be
exact matches to a previous Workflow.


Create a Workflow
*****************

A Workflow is a framework by which a user may define the relationship between
Tasks and define the relationship between multiple runs of the same set of Tasks.

A Workflow represents a set of Tasks which may depend on one another such
that if each relationship were drawn (Task A) -> (Task B) meaning that Task B
depends on Task A, it would form a `directed-acyclic graph (DAG) <https://en.wikipedia.org/wiki/Directed_acyclic_graph>`_.
A Workflow is further uniquely identified by a set of WorkflowArgs which are
required if the Workflow is to be resumable.

For more about the objects go to the :doc:`Workflow and Task Reference <jobmon.client.swarm.workflow>`
or :doc:`Executor Parameter Reference <jobmon.client.swarm.executors>`

Constructing a Workflow and adding a few Tasks is simple::

    import getpass

    from jobmon.client.swarm.workflow.workflow import Workflow
    from jobmon.client.swarm.workflow.bash_task import BashTask
    from jobmon.client.swarm.workflow.python_task import PythonTask
    from jobmon.client.swarm.executors.base import ExecutorParameters

    # Create a Workflow
    user = getpass.getuser()
    my_wf = Workflow(workflow_args="quickstart", project='proj_tools',
                     stderr='/ihme/scratch/users/{}/sgeoutput'.format(user),
                     stdout='/ihme/scratch/users/{}/sgeoutput'.format(user)),
                     working_dir='/homes/{}'.format(user),
                     seconds_until_timeout=3600)

    # Add some Tasks with defined Parameters
    write_params = ExecutorParameters(m_mem_free='4G', num_cores=2,
                                      max_runtime_seconds=600,
                                      resource_scales={'m_mem_free': 0.3,
                                                       'max_runtime_seconds': 0.2})
    write_task = BashTask("touch ~/jobmon_qs.txt", executor_parameters=write_params)

    # this task will use all default parameters by not specifying its own requirements
    copy_task = BashTask("cp ~/jobmon_qs.txt ~/cpof_jobmon_qs.txt", upstream_tasks=[write_task])

    del_task = BashTask("rm ~/jobmon_qs.txt", upstream_tasks=[copy_task])
    # (create a runme.py in your home directory)
    run_params = ExecutorParameters(m_mem_free='4G', num_cores=2,
                                    max_runtime_seconds=100)
    run_task = PythonTask(path_to_python_binary='/ihme/code/central_comp/miniconda/bin/python',
                          script='~/runme.py', env_variables={'OP_NUM_THREADS': 1}, args=[1, 2], executor_parameters=run_params)

    my_wf.add_tasks([write_task, copy_task, del_task, run_task])
    my_wf.run()

.. note::
    Unique Workflows: If you know that your Workflow is to be used for a
    one-off project only, you may choose to use an anonymous Workflow, meaning
    you leave workflow_args blank. In this case, WorkflowArgs will default to
    a UUID which, as it is randomly generated, will be harder to remember and
    thus is not recommended for use cases outside of the one-off project.

Default Executor Parameters: Tasks, such as BashTask, PythonTask, etc. take
many qsub-type arguments, to help you allocate appropriate resources for your
job. These include num_cores, m_mem_free, and max_runtime_seconds. By default,
num_cores used will be 1, mem_free will be 1G, and max attempts will be 3.
Stderr, stdout, project, and working_dir (if desired) are set at the workflow
level (see below).

Additional Arguments: If you need to launch a Python, R, or Stata job, but
usually do so with a shellscript that sets environment variables before
running the full program, you can pass these environment variables to your
Jobmon Task, in the form of a dictionary. These will then be formatted and
prepended to the command, so that all environment variables will be set on
each node where the code executes.

.. note::
    By default Workflows are set to time out if your tasks haven't all
    completed after 10 hours (or 36000 seconds). If your Workflow times out
    before your tasks have finished running, those tasks will continue
    running, but you will need to restart your Workflow again. You can change
    this if your tasks combined run longer than 10 hours.

.. note::
    Errors with a return code of 199 indicate an issue occurring within Jobmon
    itself.

.. note::
    Resource Adjustments: If you want to define the rate at which resources
    are adjusted in case a job fails because it did not request enough
    resources (exit code 137), you can define which resources you want to
    adjust in the resource scales parameter and the factor by which they
    should be scaled if they do fail due to a resource error


Restart Tasks and Resume Workflows
=======================================

A Workflow allows for sophisticated tracking of how many times a DAG gets
executed, who ran them and when.
If on the old prod cluster it does uses ssh to kill off any job instances
that might be left over from previous failed attempts. With a Workflow you can:

#. Re-use a set of Tasks
#. Stop a set of Tasks mid-run and resume it (either intentionally or unfortunately, as
   a result of an adverse cluster event)
#. Re-attempt a set of Tasks that may have ERROR'd out in the middle (assuming you
   identified and fixed the source of the error)
#. Set stderr, stdout, working_dir, and project qsub arguments from the top level

To resume the Workflow created above::

    import getpass
    from jobmon.client.swarm.workflow.workflow import Workflow

    # Re-instantiate your Workflow with the same WorkflowArgs but add the resume flag
    user = getpass.getuser()
    my_wf = Workflow(workflow_args"quickstart", project='proj_jenkins',
                  stderr='/ihme/scratch/users/{}/sgeoutput'.format(user),
                  stdout='/ihme/scratch/users/{}/sgeoutput'.format(user),
                  working_dir='/homes/{}'.format(user), resume=True)

    # Re-add the same Tasks to it...
    write_task = BashTask("touch ~/jobmon_qs.txt", slots=2, mem_free=4)
    copy_task = BashTask("cp ~/jobmon_qs.txt ~/cpof_jobmon_qs.txt", upstream_tasks=[write_task])
    del_task = BashTask("rm ~/jobmon_qs.txt", upstream_tasks=[copy_task])
    # (create a runme.py in your home directory)
    run_task = PythonTask(path_to_python_binary='/ihme/code/central_comp/miniconda/bin/python',
                          script='~/runme.py', env_variables={'OP_NUM_THREADS': 1}, args=[1, 2], slots=2, mem_free=4)

    my_wf.add_tasks([write_task, copy_task, del_task, run_task])

    my_wf.run()

That's it. It is the same setup, just change the resume flag so that it is
true (otherwise you will get an error that you are creating a workflow that
already exists)

Behind the scenes, the Workflow will launch your Tasks as soon as each is
ready to run (i.e. as soon as the Task's upstream dependencies are DONE). It
will automatically restart Tasks that die due to cluster instability or other
intermittent issues. If for some reason, your Workflow itself dies (or you need
to kill it yourself), resuming the script at a later time will automatically pickup
where you left off (i.e. use the '--resume' flag). A resumed run will not
re-run any jobs that completed successfully in prior runs.

Note carefully the distinction between "restart" and "resume."
Jobmon itself will restart individual jobs, whereas a human operator can resume the
entire Workflow.

For more examples, take a look at the `tests <https://stash.ihme.washington.edu/projects/CC/repos/jobmon/browse/tests/test_workflow.py>`_.

.. note::

    Remember, a Workflow is defined by its WorkflowArgs and its Tasks. If you
    want to resume a previously stopped run, make sure you haven't changed the
    values of WorkflowArgs or added any different Tasks to it. If either of these change,
    you will end up creating a brand new Workflow.

.. note::

    Resuming a previously stopped Workflow will create a new
    :term:`WorkflowRun`. This is generally an internal detail that you won't
    need to worry about, but the concept may be helpful in debugging failures
    (SEE DEBUGGING TODO).

.. todo for the jobmon developers::

    (DEBUGGING) Figure out whether/how we want users to interact with
    WorkflowRuns. I tend to think they're only useful for debugging purposes...
    but that leads to the question of what utilities we want to expose to help
    users to debug in general.

As soon as you change any of the values of your WorkflowArgs or modify its Tasks,
you'll cause a new Workflow entry to be created in the jobmon
database. When calling run() on this new Workflow, any progress through the
Tasks that may have been made in previous Workflows will be ignored.

.. todo for the jobmon developers::

    Figure out how we want to give users visibility into the Workflows
    they've created over time.

Dynamically Configure Resources for a Given Task
================================================
It is now possible to dynamically configure the resources needed to run a
given task. For example, if an upstream task may better inform the resources
that a downstream task needs, the resources will not be checked and bound until
the downstream is about to run and all of its upstream dependencies
have completed. To do this, the user can provide a function that will be called
at runtime and return an ExecutorParameter object with the resources needed.


For example ::

    from jobmon.client.swarm.executors.base import ExecutorParameters
    from jobmon.client.swarm.workflow.workflow import Workflow
    from jobmon.client.swarm.workflow.bash_task import BashTask

    def assign_resources(*args, **kwargs):
        """ Callable to be evaluated when the task is ready to be scheduled
        to run"""
        fp = '/ihme/scratch/users/svcscicompci/tests/jobmon/resources.txt'
        with open(fp, "r") as file:
            resources = file.read()
            resource_dict = ast.literal_eval(resources)
        m_mem_free = resource_dict['m_mem_free']
        max_runtime_seconds = int(resource_dict['max_runtime_seconds'])
        num_cores = int(resource_dict['num_cores'])
        queue = resource_dict['queue']

        exec_params = ExecutorParameters(m_mem_free=m_mem_free,
                                         max_runtime_seconds=max_runtime_seconds,
                                         num_cores=num_cores, queue=queue)
        return exec_params

    # task with static resources that assigns the resources for the 2nd task
    # when it runs
    task1 = PythonTask(name='task_to_assign_resources',
                       script="/assign_resources.py", max_attempts = 1,
                       max_runtime_seconds=200, num_cores=1,
                       queue='all.q', m_mem_free='1G')

    task2 = BashTask(name='dynamic_resource_task', command='sleep 1',
                    max_attempts=2, executor_parameters=assign_resources)
    task2.add_upstream(task1) # make task2 dependent on task 1

    wf = Workflow(workflow_args='dynamic_resource_wf')
    wf.add_task(task1)
    wf.run()


Making Workflow Fail On First Failure
=======================================

On occassion, a user might want to see how far a workflow can get before it fails,
or want to immediately see where problem spots are. To do this, the user can just
instantiate the workflow with fail_fast set to True. Then add tasks to the workflow
as normal, and the workflow will fail on the first failure.

For example::

    wf = Workflow(workflow_args='testing', fail_fast=True)
    t1 = BashTask("not a command 1")
    t2 = BashTask("sleep 10", upstream_tasks=[t1])
    wf.add_tasks([t1, t2])
    wf.run()


A Workflow that adjusts the resources of a job
===============================================

Sometimes a user may not be able to accurately predict the runtime or memory usage
of a task. Jobmon will detect when the task fails due to resource constraints and
retry that task with with more resources. The default resource scaling factor is 50%
for m_mem_free and max_runtime_sec unless otherwise specified.

For example::

    from jobmon import Workflow, BashTask
    from jobmon.client.swarm.executors.base import ExecutorParameters

    my_wf = Workflow(
        workflow_args="resource starved workflow",
        project="proj_tools")


    # specify SGE specific parameters
    sleepy_params = ExecutorParameters(
        num_cores=1,
        m_mem_free="1G",
        max_runtime_seconds=100,  # set max runtime to be shorter than task runtime
        queue="all.q",
        executor_class="SGEExecutor",
        resource_scales={'m_mem_free': 0.5, 'max_runtime_seconds': 0.5})
    sleepy_task = BashTask(
        # set sleep to be longer than max runtime, forcing a retry
        "sleep 120",
        # job should succeed on second try. runtime will 150s on try 2
        max_attempts=2,
        executor_parameters=sleepy_params)
    my_wf.add_task(sleepy_task)

    # job will time out and get killed by the cluster. After a few minutes jobmon
    # will notice that it has disappeared and ask SGE for exit status. SGE will
    # show a resource kill. Jobmon will scale memory and runtime by 50% and retry the
    # job at which point it will succeed.
    my_wf.run()

.. note::

    The workflow level parameter, resource_adjustment, will be removed in
    future versions of jobmon however if you are currently specifying it as a
    value other than the default 0.5, it will override the individual resource
    scale values and the resource adjustment value will be applied to all
    resources specified



A Workflow that retries jobs if they fail
*****************************************

By default a job will be retried up to 3 times if it fails. This helps to
reduce the chance that random events on the cluster or landing on a bad node
will cause your entire job and workflow to fail.

In order to configure the number of times a job can be retried, configure the
max_attempts parameter in the task that you create. If you are still debugging
your code, please set the number of retries to zero so that it does not retry
code with a bug multiple times. When the code is debugged, and you are ready
to run in production, set the retries to a nonzero value.

The following example shows a configuration in which the user wants their job
to be retried 4 times and it will fail up until the fourth time.::

    import getpass
    from jobmon import Workflow, PythonTask
    from jobmon.client.swarm.executors.base import ExecutorParameters
    from jobmon.client.swarm.executors import sge_utils

    user = getpass.getuser()

    wf = Workflow(
        workflow_args="workflow_with_many_retries",
        project="proj_tools")

    params = ExecutorParameters(
        num_cores=1,
        m_mem_free="1G",
        max_runtime_seconds=100,  # set max runtime to be shorter than task runtime
        queue="all.q",
        executor_class="SGEExecutor",
        resource_scales={'m_mem_free': 0.5, 'max_runtime_seconds': 0.5})

    name = "retry_task"
    output_file_name = f"/ihme/scratch/users/{user}/retry_output"
    retry_task = PythonTask(
        script=sge_utils.true_path("tests/remote_sleep_and_write.py"),
        args=["--sleep_secs", "4",
              "--output_file_path", output_file_name,
              "--fail_count", 3,
              "--name", name],
        name=name, max_attempts=4, executor_parameters = params)

    wf.add_task(retry_task)

    # 3 job instances will fail before ultimately succeeding
    wf.run()

Jobmon Database
***************

By default, your Workflow talks to our centrally-hosted jobmon server
(jobmon-docker-cont-p01.hosts.ihme.washington.edu). You can access the
jobmon database from your favorite DB browser (e.g. Sequel Pro) using the credentials::

    host: jobmon-docker-cont-p01.hosts.ihme.washington.edu
    port: 10020
    user: read_only
    pass: docker
    database: docker

If you are accessing a version of jobmon prior to 0.8.4 or using jobmon==0.9.9 the database host is
jobmon-p01.ihme.washington.edu

.. todo for the jobmon developers::

    Create READ-ONLY credentials


Running Queries in Jobmon
*************************


You can query the jobmon database to see the status of a whole Workflow, or any set of jobs.
Open a SQL browser and connect to the database defined above.

Tables:

executor_parameter_set
    The executor-specific parameters for a given job
executor_parameter_set_type
    The type of parameters (original requested, validated, adjusted)
job
    The (potential) call of a job. Like a function definition in python
job_attribute
    Additional attributes being tracked for a job
job_attribute_type
    Type of attributes that can be tracked
job_instance
    An actual run of a job. Like calling a function in python. One job can
    have multiple job_instances if they are retried
job_instance_error_log
    Any errors produced by a job_instance.
job_instance_status
    Has the status of the running job_instance (as defined in the job_status table).
job_status
    Meta-data table that defines the four states of a job_instance.
task_dag
    Has every entry of task dags created, as identified by a dag_id and dag_hash
workflow
    Has every workflow created, along with it's associated dag_id, and workflow_args
workflow_attribute
    Additional attributes that are being tracked for a given workflow
workflow_attribute_type
    The types of attributes that can be tracked for workflows
workflow_run
    Has every run of a workflow, paired with it's workflow, as identified by workflow_id
workflow_run_attribute
    Additional attributes that are being tracked for a workflow run
workflow_run_attribute_type
    The types of attributes that can be tracked for workflow runs
workflow_run_status
    Meta-data table that defines the four states of a Workflow Run
workflow_status
    Meta-data table that defines the five states of a Workflow

You will need to know your workflow_id or dag_id. Hopefully your application
logged it, otherwise it will be obvious by name as one of the recent entries
in the task_dag table.

For example, the following command shows the current status of all jobs in dag 191:
    SELECT status, count(*) FROM job WHERE dag_id=191 GROUP BY status

