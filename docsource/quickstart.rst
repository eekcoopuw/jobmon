Quickstart
##########


Install
*******
To get started::

    pip install jobmon

.. todo for the jobmon developers::
    Add a 'test' subcommand to jobmon cli to ensure initial setup was run
    properly

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
adding :term:`Task` to it. Each Workflow is uniquely defined by its :term:`WorkflowArgs`. A Workflow can only be re-loaded if the WorkflowArgs and all Tasks added to it are shown to be exact matches to a previous Workflow.


Create a Workflow
****************

A Workflow is a framework by which a user may define the relationship between tasks and define the relationship between multiple runs of the same set of tasks.

A Workflow represents a set of Tasks which may depend on one another such that if each relationship were drawn (Task A) -> (Task B depends on Task A), it would form a `directed-acyclic graph (DAG) <https://en.wikipedia.org/wiki/Directed_acyclic_graph>`_.  A Workflow is further uniquely identified by a set of WorkflowArgs which are required if the Workflow is to be resumable.

Constructing a Workflow and adding a few tasks is simple::

    import getpass

    from jobmon.workflow.workflow import Workflow
    from jobmon.workflow.bash_task import BashTask
    from jobmon.workflow.python_task import PythonTask

    # Create a Workflow
    user = getpass.getuser()
    my_wf = Workflow(workflow_args="quickstart", project='proj_jenkins',
                     stderr='/ihme/scratch/users/{}/sgeoutput'.format(user),
                     stdout='/ihme/scratch/users/{}/sgeoutput'.format(user)))

    # Add some Tasks to it...
    write_task = BashTask("touch ~/jobmon_qs.txt", slots=2, mem_free=4)
    copy_task = BashTask("cp ~/jobmon_qs.txt ~/cpof_jobmon_qs.txt", upstream_tasks=[write_task])
    del_task = BashTask("rm ~/jobmon_qs.txt", upstream_tasks=[copy_task])
    # (create a runme.py in your home directory)
    run_task = PythonTask(path_to_python_binary='/ihme/code/central_comp/miniconda/bin/python',
                          script='~/runme.py', env_variables={'OP_NUM_THREADS': 1}, args=[1, 2], slots=2, mem_free=4)

    my_wf.add_tasks([write_task, copy_task, del_task, run_task])
    my_wf.run()

.. note::
    If you know that your Workflow is to be used for a one-off project only, you may choose to use an anonymous Workflow, meaning you leave workflow_args blank. In this case, WorkflowArgs will default to a UUID which, as it is randomly generated, will be harder to remember and thus is not recommended for use cases outside of the one-off project.

.. note::

    Tasks, such as BashTask, PythonTask, etc. take many qsub-type arguments, to help you launch your job the way you want. These include slots, mem_free, max_attempts, max_runtime. By default, slots used will be 1, mem_free 2, with a max_attempt of 3. Stderr, stdout, and project are set at the workflow level (see below).

.. note::
    If you need to launch a Python, R, or Stata job, but usually do so with a shellscript that sets environment variables before running the full program, you can pass these environment variables to your Jobmon task, in the form of a dictionary. These will then be formatted and prepended to the command, so that all environment variables will be set on each node where the code executes.


Restart Tasks and Resume Workflows
=======================================

A Workflow allows for sophisticated tracking of how many times a DAG gets executed, who ran them and when, and does some work to kill off any job instances that might be left over from previous failed attempts. With a Workflow you can:

#. Re-use a set of tasks
#. Stop a set of tasks mid-run and resume it (either intentionally or unfortunately, as
   a result of an adverse cluster event)
#. Re-attempt a set of Tasks that may have ERROR'd out in the middle (assuming you
   identified and fixed the source of the error)
#. Set stderr, stdout, and project qsub arguments from the top level

To resume the Workflow created above::

    import getpass
    from jobmon.workflow.workflow import Workflow

    # Re-instantiate your Workflow with the same WorkflowArgs
    user = getpass.getuser()
    my_wf = Workflow(workflow_args"quickstart", project='proj_jenkins',
                  stderr='/ihme/scratch/users/{}/sgeoutput'.format(user),
                  stdout='/ihme/scratch/users/{}/sgeoutput'.format(user))

    # Re-add the same Tasks to it...
    write_task = BashTask("touch ~/jobmon_qs.txt", slots=2, mem_free=4)
    copy_task = BashTask("cp ~/jobmon_qs.txt ~/cpof_jobmon_qs.txt", upstream_tasks=[write_task])
    del_task = BashTask("rm ~/jobmon_qs.txt", upstream_tasks=[copy_task])
    # (create a runme.py in your home directory)
    run_task = PythonTask(path_to_python_binary='/ihme/code/central_comp/miniconda/bin/python',
                          script='~/runme.py', env_variables={'OP_NUM_THREADS': 1}, args=[1, 2], slots=2, mem_free=4)

    my_wf.add_tasks([write_task, copy_task, del_task, run_task])

    my_wf.run()

That's it.

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

Jobmon Database
***************

By default, your Workflow talks to our centrally-hosted jobmon server
(jobmon-p01.ihme.washington.edu). You can access the jobmon database from your
favorite DB browser (e.g. Sequel Pro) using the credentials::

    host: jobmon-p01.ihme.washington.edu
    port: 3312
    user: docker
    pass: docker
    database: docker

.. todo for the jobmon developers::

    Create READ-ONLY credentials


Running Queries in Jobmon
*************************


You can query the jobmon database to see the status of a whole Workflow, or any set of jobs.
Open a SQL browser and connect to the database defined above.

Tables:

job
    The (potential) call of a job. Like a function definition in python
job_instance
    An actual run of a job. Like calling a function in python. One job can have multiple job_instances if they are retried
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
workflow_run
    Has every run of a workflow, paired with it's workflow, as identified by workflow_id
workflow_run_status
    Meta-data table that defines the four states of a Workflow Run
workflow_status
    Meta-data table that defines the five states of a Workflow

You will need to know your workflow_id or dag_id. Hopefully your application logged it, otherwise it will be obvious by name as one of the recent entries in the task_dag table.

For example, the following command shows the current status of all jobs in dag 191:
    SELECT status, count(*) FROM job WHERE dag_id=191 GROUP BY status

