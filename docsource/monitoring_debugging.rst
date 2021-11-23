************************
Monitoring and Debugging
************************

.. _status-commands-label:

Jobmon Command Line Interface (CLI) Status Commands
###################################################
The Jobmon status commands allow you to check that status of your Workflows and Tasks from the
command line.

To use the status commands:
    1. Open a new terminal window
    2. SSH in to the cluster
    3. srun
    4. Activate the same conda environment that your tasks are running in

There are currently three supported commands:

workflow_status
***************
    Entering ``jobmon workflow_status`` in to the command line will show you
    a table of how many tasks are in each state within that workflow. You
    can specify the workflow by user using the -u flag. For example:
    ``jobmon workflow_status -u {user}``. You can also specify the workflow
    using the -w flag. For example: ``jobmon workflow_status -w 9876``.
    You can also use the -w flag to specify multiple workflows at the same
    time. For example, if you have one workflow named 9876 and one
    workflow named 1234 you would enter ``jobmon workflow_status -w 9876 1234``.

workflow_tasks
**************
    Entering ``jobmon workflow_tasks`` in to the command line will show you
    the status of specific tasks in a given workflow. You can specify which
    workflow with the -w flag. For example: ``jobmon workflow_tasks -w 9876``.
    You can also add a -s flag to only query tasks that are in a certain
    state. For example: ``jobmon workflow_tasks -w 9876 -s PENDING`` will query all
    tasks within workflow 9876 that have the pending status. You may also query by multiple
    statuses. For example: ``jobmon workflow_tasks -w 9876 -s PENDING RUNNING``

.. _task_status-commands-label:

task_status
***********
    Entering ``jobmon task_status`` in to the command line will show you the
    state of each task instance for a certain task. You may specify the task
    by adding a -t flag. For example: ``jobmon task_status -t 1234``. You may also filter by
    multiple task ids and statuses. The -s flag will allow you to filter upon a specific status.
    For example, if you wanted to query all task instances in the Done state for task 1234 and
    task 7652 you would do the following ``jobmon task_status -t 1234 7652 -s done``

JSON Flag
*********
    A new flag has been added to the Jobmon CLI to allow users to return their workflow and
    task statuses in JSON format. To use this feature add a ``-n`` flag to any of the Jobmon
    CLI commands. For example: ``jobmon task_status -t 1234 7652 -s done -n``

Possible states: PENDING, RUNNING, DONE, FATAL

task_dependencies
*****************
    Entering ``jobmon task_dependencies`` in to the command line will show users what tasks
    are upstream and downstream of the task provided. Users will specify the task by adding a
    -t flag. For example: ``jobmon task_dependencies -t 1672``. The ouput returned will look
    like::

        Upstream Tasks:

           Task ID         Status
           1               D

        Downstream Tasks:

           Task ID         Status
           3               D
           4               D

Jobmon Database
###############

Running Queries
***************
If the command line status commands do not provide the information you need,
you can query the Jobmon database.

By default, your Workflow talks to our centrally-hosted Jobmon server
(scicomp-maria-db-p01.db.ihme.washington.edu). You can access the
Jobmon database from your favorite database browser (e.g. SequelPro, MySQL Workbench) using
the credentials::

    host: scicomp-maria-db-p01.db.ihme.washington.edu
    port: 3306
    user: read_only
    pass: docker
    database: docker

To see previous database connections: https://hub.ihme.washington.edu/display/DataScience/Jobmon+Database+Connections

.. note::
    Jobmon has a persistent database. This means any time the client side of Jobmon is updated
    it will continue to use the same database. The database credentials will only change when
    database changes are implemented.

You can query the Jobmon database to see the status of a whole Workflow, or any set of tasks.
Open a SQL browser (e.g. Sequel Pro) and connect to the database defined above.

Useful Jobmon Queries
*********************
If you wanted the current status of all Tasks in workflow 191:
    | SELECT status, count(*)
    | FROM task
    | WHERE workflow_id = <workflow_id>
    | GROUP BY status

To find your Workflow if you know the Workflow name:
    | SELECT *
    | FROM workflow
    | WHERE name="<your workflow name>"

To find all of your Workflows by your username:
    | SELECT *
    | FROM workflow
    | JOIN workflow_run ON workflow.id = workflow_run.workflow_id
    | WHERE workflow_run.user = "<your username>"

To get all of the error logs associated with a given Workflow:
    | SELECT *
    | FROM task t1, task_instance t2, task_instance_error_log t3
    | WHERE t1.id = t2.task_id
    | AND t2.id = t3.task_instance_id
    | AND t1.workflow_id = <workflow id>

To get the error logs for a given WorkflowRun:
    | SELECT *
    | FROM task_instance t1, task_instance_error_log t2
    | WHERE t1.id = t2.task_instance_id
    | AND t1.workflow_run_id = <workflow_run_id>

Database Tables
###############

arg
***
    A list of args that the node_args and task_args use.

arg_type
********
    The different types of arguments (NODE_ARG, TASK_ARG, OP_ARG). For more information on
    argument types see, :ref:`jobmon-arguments-label`.

cluster
*******
    A list of cluster's that Jobmon is able to run jobs on, including Slurm and Buster.

cluster_type
************
    A list of cluster types that Jobmon can run jobs on. Currently includes dummy, sequential,
    multiprocess, UGE, and slurm.

dag
***
    This table has every entry for every DAG (Directed Acyclic Graph) created, as identified
    by it's ID and hash.

edge
****
    A table that shows the relationship between a specific node and it's upstream and
    downstream nodes.

node
****
    The object representing a Task within a DAG. Table includes the ID of the TaskTemplate
    version and the hash of the node args.

node_arg
********
    Arguments that identify a unique node within the DAG. For more information on node
    arguments see, :ref:`jobmon-node-args-label`.

queue
*****
    A table that lists all of the available queues for a given cluster. It also provides the
    resource bounds (minimum and maximum value for cores, runtime and memory) of a queue and
    the default resources of a queue.

task
****
    A single executable object in the workflow. The table includes the name of the task, the
    command it submitted, and it's task resource ID.

task_arg
********
    A list of args that make a command unique across different workflows, includes task_id,
    arg_id and the associated value. For more information on task arguments see,
    :ref:`jobmon-task-args-label`.

task_attribute
**************
    A table that tracks optional additional attributes of a task. For example, release ID or
    location set version ID.

task_attribute_type
*******************
    Types of task attributes that can be tracked.

task_instance
*************
    Table that tracks the actual runs of tasks. The table includes the workflow_run_id,
    cluster_type_id, and task_id associated with the task instance. It also includes what node
    the task instance ran on.

task_instance_error_log
***********************
    Any errors that are produced by a task instance are logged in this table.

task_instance_status
********************
    Meta-data table that defines the ten states of Task Instance. For more information see
    status section below.

task_resources
**************
    The resources that were requested for a Task. Resources include: memory, cores, runtime,
    queue, and project.

task_resources_type
*******************
    This table is used mostly for internal Jobmon functionality. There are three types of task
    resources: original (the resources requested by the user), validated (the requested
    resources that have been validated against the provided queue), adjusted (resources that
    have been scaled after a task instance failed due to a resource error).

task_status
***********
    Meta-data table that defines the eight states of Task. For more information, see the status
    section below.

task_template
*************
    This table has every TaskTemplate, paired with it's tool_version_id.

task_template_version
*********************
    A table listing the different versions a TaskTemplate can have.

template_arg_map
****************
    A table that maps TaskTemplate versions to argument IDs.

tool
****
    A table that shows the list of Tools that can be associated with your Workflow and
    TaskTemplates.

tool_version
************
    A table listing the different versions a Tool has.

workflow
********
    This table has every Workflow created. It includes the name of the workflow, the tool
    version it's associated with, and the DAG that it's associated with.

workflow_attribute
******************
    A table that lists optional additional attributes that are being tracked for a given
    Workflow.

workflow_attribute_type
***********************
    The types of attributes that can be tracked for Workflows.

workflow_run
************
    This table has every run of a workflow, paired with it's workflow, as identified by
    workflow_id. It also includes what user ran the workflow and the run status.

workflow_run_status
*******************
    Meta-data table that defines the thirteen states of Workflow Run.

workflow_status
***************
    Meta-data table that defines nine states of Workflow.

Jobmon Statuses
###############

Task Instance
*************
    +-----+---------------------------------+---------------------------------------------------------------------------------+
    |     | Status                          | Description                                                                     |
    +=====+=================================+=================================================================================+
    |  B  |  SUBMITTED_TO_BATCH_EXECUTIONER | Task instance submitted normally.                                               |
    +-----+---------------------------------+---------------------------------------------------------------------------------+
    |  D  |  DONE                           | Task instance finishes normally.                                                |
    +-----+---------------------------------+---------------------------------------------------------------------------------+
    |  E  |  ERROR                          | Task instance has hit an application error.                                     |
    +-----+---------------------------------+---------------------------------------------------------------------------------+
    |  F  |  ERROR_FATAL                    | Task instance encountered a fatal error; it will not be retried.                |
    +-----+---------------------------------+---------------------------------------------------------------------------------+
    |  I  |  INSTANTIATED                   | Task instance is created.                                                       |
    +-----+---------------------------------+---------------------------------------------------------------------------------+
    |  K  |  KILL_SELF                      | Task instance has been ordered to kill itself if it is still alive.             |
    +-----+---------------------------------+---------------------------------------------------------------------------------+
    |  R  |  RUNNING                        | Task instance starts running normally.                                          |
    +-----+---------------------------------+---------------------------------------------------------------------------------+
    |  U  |  UNKNOWN_ERROR                  | Task instance stops reporting that it's alive and Jobmon can't figure out why.  |
    +-----+---------------------------------+---------------------------------------------------------------------------------+
    |  W  |  NO_DISTRIBUTOR_ID              | Task instance submission has hit a bug and did not receive a distributor_id.    |
    +-----+---------------------------------+---------------------------------------------------------------------------------+
    |  Z  |  RESOURCE_ERROR                 | Task instance died because of an insufficient resource request.                 |
    +-----+---------------------------------+---------------------------------------------------------------------------------+

Task
****
    +-----+---------------------------+----------------------------------------------------------------------------------------+
    |     | Status                    | Description                                                                            |
    +=====+===========================+========================================================================================+
    |  A  |  ADJUSTING_RESOURCES      | Task has errored with a resource error, the resources will be adjusted before retrying.|
    +-----+---------------------------+----------------------------------------------------------------------------------------+
    |  D  |  DONE                     | Task ran to completion; Task has a TaskInstance that has succesfully completed.        |
    +-----+---------------------------+----------------------------------------------------------------------------------------+
    |  E  |  ERROR_RECOVERABLE        | Task has errored out but has more attempts so it will be retried.                      |
    +-----+---------------------------+----------------------------------------------------------------------------------------+
    |  F  |  ERROR_FATAL              | Task has errored out and has used all of the attempts. It cannot be retried.           |
    +-----+---------------------------+----------------------------------------------------------------------------------------+
    |  G  |  REGISTERED               | Task has been bound to the database.                                                   |
    +-----+---------------------------+----------------------------------------------------------------------------------------+
    |  I  |  INSTANTIATED             | Task has had a Task Instance created that will be submitted to the distributor.        |
    +-----+---------------------------+----------------------------------------------------------------------------------------+
    |  Q  |  QUEUED_FOR_INSTANTIATION | Task's dependencies have been met, task can be run when the scheduler is ready.        |
    +-----+---------------------------+----------------------------------------------------------------------------------------+
    |  R  |  RUNNING                  | Task is running on the specified distributor.                                          |
    +-----+---------------------------+----------------------------------------------------------------------------------------+

Workflow Run
************
    +-----+---------------+--------------------------------------------------------------------------------------------------------+
    |     | Status        | Description                                                                                            |
    +=====+===============+========================================================================================================+
    |  A  |  ABORTED      | WorkflowRun encountered problems while binding so it stopped.                                          |
    +-----+---------------+--------------------------------------------------------------------------------------------------------+
    |  B  |  BOUND        | WorkflowRun has been bound to the database.                                                            |
    +-----+---------------+--------------------------------------------------------------------------------------------------------+
    |  C  |  COLD_RESUME  | WorkflowRun was set to resume once all tasks were stopped.                                             |
    +-----+---------------+--------------------------------------------------------------------------------------------------------+
    |  D  |  DONE         | WorkflowRun has run to completion.                                                                     |
    +-----+---------------+--------------------------------------------------------------------------------------------------------+
    |  E  |  ERROR        | WorkflowRun has not completed successfully, may have lost contact with services.                       |
    +-----+---------------+--------------------------------------------------------------------------------------------------------+
    |  G  |  REGISTERED   | WorkflowRun has been validated.                                                                        |
    +-----+---------------+--------------------------------------------------------------------------------------------------------+
    |  H  |  HOT RESUME   | WorkflowRun was set to resume while tasks are still running, they will continue running.               |
    +-----+---------------+--------------------------------------------------------------------------------------------------------+
    |  I  |  INSTANTIATED | Scheduler is instantiating an instance on the distributor.                                             |
    +-----+---------------+--------------------------------------------------------------------------------------------------------+
    |  L  |  LINKING      | WorkflowRun was completed succesfully, connecting it to Workflow.                                      |
    +-----+---------------+--------------------------------------------------------------------------------------------------------+
    |  O  |  LAUNCHED     | Instantiation complete. Distributor control for tasks or waiting for scheduling loop for workflows.    |
    +-----+---------------+--------------------------------------------------------------------------------------------------------+
    |  R  |  RUNNING      | WorkflowRun is currently running.                                                                      |
    +-----+---------------+--------------------------------------------------------------------------------------------------------+
    |  S  |  STOPPED      | WorkflowRun has been stopped, probably due to keyboard interrupt from user.                            |
    +-----+---------------+--------------------------------------------------------------------------------------------------------+
    |  T  |  TERMINATED   | WorkflowRun was in resume, new WorkflowRun created to pick up remaining Tasks, so this one terminated. |
    +-----+---------------+--------------------------------------------------------------------------------------------------------+

Workflow
********
    +-----+----------------+-----------------------------------------------------------------------------------------------------+
    |     | Status         | Description                                                                                         |
    +=====+================+=====================================================================================================+
    |  A  |  ABORTED       | Workflow encountered an error before a WorkflowRun was created.                                     |
    +-----+----------------+-----------------------------------------------------------------------------------------------------+
    |  D  |  DONE          | Workflow finished successfully.                                                                     |
    +-----+----------------+-----------------------------------------------------------------------------------------------------+
    |  F  |  FAILED        | Workflow unsuccessful in one or more WorkflowRuns, no runs finished as DONE.                        |
    +-----+----------------+-----------------------------------------------------------------------------------------------------+
    |  G  |  REGISTERING   | Workflow is being validated.                                                                        |
    +-----+----------------+-----------------------------------------------------------------------------------------------------+
    |  H  |  HALTED        | Resume was set and wf shut down or the controller died and wf was reaped.                           |
    +-----+----------------+-----------------------------------------------------------------------------------------------------+
    |  I  |  INSTANTIATING | Scheduler is instantiating an instance on the distributor.                                          |
    +-----+----------------+-----------------------------------------------------------------------------------------------------+
    |  O  |  LAUNCHED      | Instantiation complete. Distributor control for tasks or waiting for scheduling loop for workflows. |
    +-----+----------------+-----------------------------------------------------------------------------------------------------+
    |  Q  |  QUEUED        | Client has added all necessary metadata, signal to scheduler to instantiate.                        |
    +-----+----------------+-----------------------------------------------------------------------------------------------------+
    |  R  |  RUNNING       | Workflow has a WorkflowRun that is running.                                                         |
    +-----+----------------+-----------------------------------------------------------------------------------------------------+

Graphical User Interface (GUI)
##############################
Coming soon - a read-only GUI that will allow users to see the status of their workflows.