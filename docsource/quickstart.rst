Quickstart
##########


Install
*******
To get started::

    pip install jobmon
    jobmon configure

.. todo::
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


Create a TaskDag
****************

Users will primarily interact with jobmon by creating a :term:`TaskDag` and
attaching it to a :term:`Workflow`. A TaskDag is so-named because it represents
a set of Tasks which may depend on one another such that if each relationship
were drawn (Task A) -> (Task B depends on Task A), it would form a
`directed-acyclic graph (DAG)
<https://en.wikipedia.org/wiki/Directed_acyclic_graph>`_.  Constructing a
TaskDag and adding a few tasks is simple::

    from jobmon.workflow.task_dag import TaskDag
    from jobmon.workflow.bash_task import BashTask

    # Create a TaskDag
    my_dag = TaskDag(name="MyTaskTag")

    # Add some Tasks to it...
    write_task = BashTask("touch ~/jobmon_qs.txt", project="proj_jenkins", slots=2, mem_free=4)
    copy_task = BashTask("cp ~/jobmon_qs.txt ~/cpof_jobmon_qs.txt", upstream_tasks=[write_task])
    del_task = BashTask("rm ~/jobmon_qs.txt", upstream_tasks=[copy_task])
    # (create a runme.py in your home directory)
    run_task = PythonTask(path_to_python_binary='/ihme/code/central_comp/miniconda/bin/python',
                          runfile='~/runme.py', args=[1, 2], slots=2, mem_free=4, project=proj_jenkins)

    my_dag.add_tasks([write_task, copy_task, del_task, run_task])

.. note::

    Tasks, such as BashTask, PythonTask, etc. take many qsub-type arguments, to help you launch your
    job the way you want. These include project, slots, mem_free, max_attempts, max_runtime, stderr,
    and stdout. By default, slots used will be 1, mem_free 2, with a max_attempt of 1, no stderr/out
    and no project, which will land you in the ihme_general queue.


Create a Workflow
*****************

You might want to re-use your TaskDag definition multiple times.  This is where
Workflows come into play. With a Workflow you can:

#. Re-use a TaskDag
#. Stop a Dag mid-run and resume it (either intentionally or unfortunately, as
   a result of an adverse cluster event)
#. Re-attempt a TaskDag that may have ERROR'd out in the middle (assuming you
   identified and fixed the source of the error)

Let's attach our TaskDag to a Workflow and run it::

    from jobmon.workflow.workflow import Workflow

    # Add your TaskDag to a Workflow and run it
    wf = Workflow(my_dag, "version1")
    wf.run()

That's it. When run() is finished, you should see the file
*cpof_jobmon_qs.txt* in your home directory.

For more examples, take a look at the `tests <https://stash.ihme.washington.edu/projects/CC/repos/jobmon/browse/tests/test_workflow.py>`_.


Restarting Tasks and Resuming Workflows
=======================================

Behind the scenes, the Workflow will launch your Tasks as soon as each is
ready to run (i.e. as soon as the Task's upstream dependencies are DONE). It
will automatically restart Tasks that die due to cluster instability or other
intermittent issues. If for some reason, your Workflow itself dies (or you need
to pause it), re-running the script at a later time will automatically pickup
where you left off (i.e. 'resume'). It will not re-run any jobs that completed
successfully in prior runs.

.. note::

    Remember, a Workflow is defined by its WorkflowArgs and TaskDag. If you
    want to resume a previously stopped run, make sure you haven't changed the
    values of WorkflowArgs or modified TaskDag. If either of these change,
    you will end up creating a brand new Workflow.

.. note::

    Resuming a previously stopped Workflow will create a new
    :term:`WorkflowRun`. This is generally an internal detail that you won't
    need to worry about, but the concept may be helpful in debugging failures
    (SEE DEBUGGING TODO).

.. todo::

    (DEBUGGING) Figure out whether/how we want users to interact with
    WorkflowRuns. I tend to think they're only useful for debugging purposes...
    but that leads to the question of what utilities we want to expose to help
    users to debug in general.

As soon as you change any of the values of your WorkflowArgs or modify the
TaskDag, you'll cause a new Workflow entry to be created in the jobmon
database. When calling run() on this new Workflow, any progress through the
TaskDag that may have been made in previous Workflows will be ignored.

.. todo::

    Figure out how we want to give users visibility into the Workflows
    they've created over time.

Jobmon Database
***************

By default, your Workflow talks to our centrally-hosted jobmon server
(jobmon-p01.ihme.washington.edu). You can access the jobmon database from your
favorite DB browser (e.g. Sequel Pro) using the credentials::

    host: jobmon-p01.ihme.washington.edu
    port: 3306
    user: docker
    pass: docker

.. todo::

    Create READ-ONLY credentials

.. todo::

    Explore other ways to expose relevant Database information to users without
    forcing them to explore the DB directly... too much schema knownledge is
    necessary to do anything useful.
