*************
Core Concepts
*************

Cluster Name
############
The name of the cluster users want to run their Tasks on. Usually either Buster or Slurm. You
can specify the cluster you want to use on the Task, TaskTemplate, Workflow
and Tool level. To set cluster name on Tasks, use "cluster_name". To set cluster_name on
TaskTemplate, Workflow, and Tool, use "default_cluster_name". If cluster name
is set on multiple Jobmon objects, Jobmon has a hierarchy of which cluster will take precedence,
the hierarchy is Task -> TaskTemplate -> Workflow -> Tool.

Tool
####
A tool is the project (e.g. STG-PR, CODCorrect) that you want to associate your Workflow and
Task Templates with. You may overhaul your Workflows and Tasks/TaskTemplates over time, but the
concept of the project will remain to categorize them within the broader IHME pipeline.

.. _jobmon-workflow-label:

Workflow
########
*(aka Batch, aka Swarm)*
The object that encompasses all of your Tasks that will be executed. It builds on the
DAG to include the setup logic to bind all of the objects to the database and capture
metadata about the Workflow. Much like Task is the intent to run a command, and Task
Instance is the actual execution, Workflow is the intent to run a DAG of Tasks, and a
Workflow Run is the actual execution of the DAG traversal. Therefore a Workflow can be
resumed if it failed on a previous Workflow Run, but the Tasks that it will execute
must remain the same. This is codified in the WorkflowArgs parameter that the user
can define to indicate what makes this set of Tasks unique such that no other
Workflow will be the same.

.. _jobmon-wf-arg-label:

WorkflowArgs
************
A set of arguments that are used to determine the "uniqueness" of the
Workflow and whether it can be resumed. Must be hashable. For example,
CodCorrect or Como version might be passed as Args to the Workflow.
Coupled with a populated TaskDag, WorkflowArgs define a Workflow.

Workflow Attributes
*******************
Additional attributes that are being tracked for a given Workflow. They are not required
to use Jobmon, and workflow_attributes are not passed to your jobs. They are intended to
track information for a given run and can be utilized for profiling and resource
prediction.

Workflow Run
************
WorkflowRun enables tracking for multiple runs of a single Workflow. A
Workflow may be started/paused/ and resumed multiple times. Each start
or resume represents a new WorkflowRun.

In order for a Workflow to be deemed DONE (successful), it must have 1 or more
WorkflowRuns. A Task belongs to a Workflow, so it may be run multiple times (i.e.
multiple Task Instances will be created), but once the Task reaches a DONE
state, it will no longer be added to a subsequent WorkflowRun, and therefore the
Workflow Run will not create any Task Instances for that Task. (If a user wants it to
be rerun, then it must be reset to a REGISTERED ("G"), or other non-DONE state)


Task Template
##############
TaskTemplates are the underlying structure of a given Task. A user defines a command template that
individual Tasks will fill in with varying arguments. A Task's uniqueness is defined by it's
NodeArgs and TaskArgs. A Task Template can be used in different Workflows and is
associated with a given Tool. TaskTemplates can also be versioned, meaning you can iterate
upon them. A user would create a new version of their TaskTemplate if the command changes or
if the underlying methods change in a way that the user wants to recognize as different from
before.

.. _jobmon-task-label:

Task
####
A single executable object in the workflow; a command that will be run. Relate it to a
Task Template in order to classify it as a type of job within the context of your
Workflow. Do this by using the TaskTemplate create_task() function.

TaskAttribute
*************
Additional attributes of the task that can be tracked. For example, release ID or
location set version ID. Task attributes are not passed to the job but may be useful
for profiling or resource prediction work in the Jobmon database. Pass in task
attributes as a list or dictionary to create_task().

Task Instance
*************
The actual instance of execution of a Task command. The equivalent of a single srun on
the Slurm Cluster. Jobmon will create TaskInstances from the Tasks that you define. This
is an actual run of a task. Like calling a function in Python. One Task can have
multiple task instances if they are retried.

Nodes
#####
Nodes are the object representing a Task within a DAG. It simply keeps track of where a
Task is and what attributes make the task unique within the DAG. Tasks
will often be created from a TaskTemplate and they will vary somewhat e.g. by location, this
variation is what makes a Node unique.

Compute Resources
#################
Compute resources are used for users to request resources for their tasks. Compute resources
are passed in as dictionaries. Users are able to specify requested memory, cores, runtime,
queue, stdout, stderr, and project. To set compute resources on Tasks, use
"compute_resources". To set resources on TaskTemplate, Workflow, and Tool, use
"default_compute_resources". If compute resources are set on multiple objects, Jobmon has a
hierarchy of which resources will take precedence, the hierarchy is Task -> TaskTemplate ->
Workflow -> Tool.

YAML Configuration Files
************************
Users are also able to pass in compute resources via a YAML file. You can specify compute
resources via YAML on the Tool and TaskTemplate objects. Simply create a YAML file with the
requested resources, for example:

.. code-block:: yaml

    tool_resources:
      buster:
        num_cores: 1
        m_mem_free: "1G"
        max_runtime_seconds: (60 * 60 * 24 * 7)
        queue: "null.q"
      slurm:
        cores: 2
        memory: "2G"
        runtime: (60 * 60 * 24)
        queue: "null.q"
    task_template_resources:
      example_task_template_name:
        buster:
          num_cores: 1
          m_mem_free: "3G"
          max_runtime_seconds: (60 * 60 * 4)
          queue: "null.q"
        slurm:
          cores: 2
          memory: "4G"
          runtime: (60 * 60 * 24)
          queue: "null.q"

Users can specify the YAML file on a TaskTemplate by passing the file path to the YAML to the
keyword argument "yaml_file" in the "get_task_template()" method.

Users can specify the YAML file on a Tool by passing the file path to the YAML to the
keyword argument "yaml_file" in the "set_default_compute_resources_from_yaml" method.

Default Resources
*****************
Each queue on both clusters have default resources specified. These are the resources that will
be used if the user does not provide them. For the Slurm cluster, default compute resources
are: cores will be 1, memory will be 1G, and runtime will be 10 minutes.

Dependencies
############
Jobmon allows for fine-grained job dependencies. Users can specify upstream dependencies (Tasks)
on their Tasks. This means that the Task won't run until all of it's upstream dependencies
have successfully run and are in DONE state. Users can set upstream dependencies by passing a
list of Tasks to the keyword parameter "upstream_tasks" in the "create_task()" method.


.. _jobmon-arguments-label:

Arguments
#########

.. _jobmon-node-args-label:

node_args
*********
    Any named arguments in command_template that make the command unique within this template
    for a given workflow run. Generally these are arguments that can be parallelized over, e.g.
    location_id.

op_args
*******
    Any named arguments in command_template that can change without changing the identity of
    the task. Generally these are things like the task executable location or the verbosity of
    the script.

.. _jobmon-task-args-label:

task_args
*********
    Any named arguments in command_template that make the command unique across workflows if
    the node args are the same as a previous workflow. Generally these are arguments about
    data moving though the task, e.g. release_id.
