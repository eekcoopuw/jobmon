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
    | WHERE workflow_id=191
    | GROUP BY status

To find your Workflow if you know the Workflow name:
    | SELECT *
    | FROM workflow
    | WHERE name="<your workflow name>"

To find all of your Workflows by your username:
    | SELECT *
    | FROM workflow
    | JOIN workflow_run ON workflow.id = workflow_run.workflow_id
    | WHERE workflow_run.user="<your username>"

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
    The different types of args (NODE_ARG, TASK_ARG, OP_ARG).

command_template_arg_type_mapping
*********************************
    A table that associates a TaskTemplate version with arg types.

dag
***
    Has every entry of dags created, as identified by it's id and hash.

edge
****
    A table that shows the relationship between two nodes.

executor_parameter_set
**********************
    The executor-specific parameters of a given Task, e.g max_runtime_seconds, m_mem_free,
    num_cores etc.

executor_parameteter_set_type
*****************************
    The type of parameters (original requested, validated, adjusted).

node
****
    The object representing a Task within a DAG. Table includes TaskTemplate version and the
    hash of the node args.

node_arg
********
    Args that identify a unique node in the DAG.

task
****
    A single executable object in the workflow. The table includes the name of the task, the
    command it submitted, and it's executor parameters.

task_arg
********
    A list of args that make a command unique across different workflows, includes task_id,
    arg_id and the associated value.

task_attribute
**************
    Additional attributes of the task that can be tracked. For example, release ID or location
    set version ID. Task attributes are not passed to the job but may be useful for profiling
    or resource prediction work in the Jobmon database. Pass in task attributes as a list or
    dictionary to create_task().

task_attribute_type
*******************
    Types of task attributes that can be tracked.

task_instance
*************
    This is an actual run of a task. Like calling a function in Python. One Task can have
    multiple task instances if they are retried.

task_instance_error_log
***********************
    Any errors that are produced by a task instance are logged in this table.

task_instance_status
********************
    Meta-data table that defines the ten states of Task Instance.

task_status
***********
    Meta-data table that defines the eight states of Task:

task_template
*************
    This table has every TaskTemplate, paired with it's tool_version_id.

task_template_version
*********************
    A table listing the different versions a TaskTemplate can have.

tool
****
    A table that shows the list of Tools that can be associated with your Workflow and
    TaskTemplates.

tool_version
************
    A table listing the different versions a Tool has.

workflow
********
    This table has every Workflow created, along with it’s associated dag_id,
    and workflow_args.

workflow_attribute
******************
    Additional attributes that are being tracked for a given Workflow. They are not required
    to use Jobmon, and workflow_attributes are not passed to your jobs. They are intended to
    track information for a given run and can be utilized for profiling and resource
    prediction.

workflow_attribute_type
***********************
    The types of attributes that can be tracked for Workflows.

workflow_run
************
    This table has every run of a workflow, paired with it's workflow, as identified by
    workflow_id.

workflow_run_status
*******************
    Meta-data table that defines the ten states of Workflow Run.

workflow_status
***************
    Meta-data table that defines eight states of Workflow.

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
    |  F  |  ERROR_FATAL                    | Task instance encountered a fatal error.                                        |
    +-----+---------------------------------+---------------------------------------------------------------------------------+
    |  I  |  INSTANTIATED                   | Task instance is created.                                                       |
    +-----+---------------------------------+---------------------------------------------------------------------------------+
    |  K  |  KILL_SELF                      | Task instance has been ordered to kill itself if it is still alive.             |
    +-----+---------------------------------+---------------------------------------------------------------------------------+
    |  R  |  RUNNING                        | Task instance starts running normally.                                          |
    +-----+---------------------------------+---------------------------------------------------------------------------------+
    |  U  |  UNKNOWN_ERROR                  | Task instance stops reporting that it's alive and Jobmon can't figure out why.  |
    +-----+---------------------------------+---------------------------------------------------------------------------------+
    |  W  |  NO_EXECUTOR_ID                 | Task instance submission has hit a bug and did not receive an executor_id.      |
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
    |  D  |  DONE                     | Task ran to completion.                                                                |
    +-----+---------------------------+----------------------------------------------------------------------------------------+
    |  E  |  ERROR_RECOVERABLE        | Task has errored out but has more attempts so it will be retried.                      |
    +-----+---------------------------+----------------------------------------------------------------------------------------+
    |  F  |  ERROR_FATAL              | Task has errored out and has used all of the attempts. It cannot be retried.           |
    +-----+---------------------------+----------------------------------------------------------------------------------------+
    |  G  |  REGISTERED               | Task has been bound to the database.                                                   |
    +-----+---------------------------+----------------------------------------------------------------------------------------+
    |  I  |  INSTANTIATED             | Task has had a Task Instance created that will be submitted to the Executor.           |
    +-----+---------------------------+----------------------------------------------------------------------------------------+
    |  Q  |  QUEUED_FOR_INSTANTIATION | Task's dependencies have been met, task can be run when the scheduler is ready.        |
    +-----+---------------------------+----------------------------------------------------------------------------------------+
    |  R  |  RUNNING                  | Task is running on the specified Executor.                                             |
    +-----+---------------------------+----------------------------------------------------------------------------------------+

Workflow Run
************
    +-----+--------------+--------------------------------------------------------------------------------------------------------+
    |     | Status       | Description                                                                                            |
    +=====+==============+========================================================================================================+
    |  A  |  ABORTED     | WorkflowRun encountered problems while binding so it stopped.                                          |
    +-----+--------------+--------------------------------------------------------------------------------------------------------+
    |  B  |  BOUND       | WorkflowRun has been bound to the database.                                                            |
    +-----+--------------+--------------------------------------------------------------------------------------------------------+
    |  C  |  COLD_RESUME | WorkflowRun was set to resume once all tasks were stopped.                                             |
    +-----+--------------+--------------------------------------------------------------------------------------------------------+
    |  D  |  DONE        | WorkflowRun has run to completion.                                                                     |
    +-----+--------------+--------------------------------------------------------------------------------------------------------+
    |  E  |  ERROR       | WorkflowRun has not completed successfully, may have lost contact with services.                       |
    +-----+--------------+--------------------------------------------------------------------------------------------------------+
    |  G  |  REGISTERED  | WorkflowRun has been validated.                                                                        |
    +-----+--------------+--------------------------------------------------------------------------------------------------------+
    |  H  |  HOT RESUME  | WorkflowRun was set to resume while tasks are still running, they will continue running.               |
    +-----+--------------+--------------------------------------------------------------------------------------------------------+
    |  L  |  LINKING     | Instantiation complete. Executor control for tasks or waiting for first scheduling loop for workflows. |
    +-----+--------------+--------------------------------------------------------------------------------------------------------+
    |  R  |  RUNNING     | WorkflowRun is currently running.                                                                      |
    +-----+--------------+--------------------------------------------------------------------------------------------------------+
    |  S  |  STOPPED     | WorkflowRun has been stopped, probably due to keyboard interrupt from user.                            |
    +-----+--------------+--------------------------------------------------------------------------------------------------------+
    |  T  |  TERMINATED  | WorkflowRun was in resume, new WorkflowRun created to pick up remainingtTasks, so this one terminated. |
    +-----+--------------+--------------------------------------------------------------------------------------------------------+

Workflow
********
    +-----+--------------+-----------------------------------------------------------------------------+
    |     | Status       | Description                                                                 |
    +=====+==============+=============================================================================+
    |  A  |  ABORTED     | Workflow encountered an error before a WorkflowRun was created.             |
    +-----+--------------+-----------------------------------------------------------------------------+
    |  D  |  DONE        | Workflow finished successfully.                                             |
    +-----+--------------+-----------------------------------------------------------------------------+
    |  F  |  FAILED      | Workflow unsuccessful in one or more WorkflowRuns, none finished as Done.   |
    +-----+--------------+-----------------------------------------------------------------------------+
    |  G  |  REGISTERING | Workflow is being validated.                                                |
    +-----+--------------+-----------------------------------------------------------------------------+
    |  H  |  HALTED      | 1. Resume was set and wf shut down or 2. Controller died and wf was reaped. |
    +-----+--------------+-----------------------------------------------------------------------------+
    |  Q  |  QUEUED      | Client has added all necessary metadata, signal to scheduler to instantiate.|
    +-----+--------------+-----------------------------------------------------------------------------+
    |  R  |  RUNNING     | Workflow has a WorkflowRun that is running.                                 |
    +-----+--------------+-----------------------------------------------------------------------------+

Graphical User Interface (GUI)
##############################
TODO: FILL OUT THIS SECTION