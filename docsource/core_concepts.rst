
.. _jobmon-core-label:

*************
Core Concepts
*************


.. _jobmon-tool-label:

Tool
####
A tool is a major research application, e.g. STG-PR, CODCorrect.
Task Templates and Workflows  are owned by Tools. You may overhaul your Workflows and Tasks/TaskTemplates over time, but the
concept of the Tool will remain to categorize them within the broader IHME pipeline.


.. _jobmon-workflow-label:

Workflow
########
A workflow encompasses all of your :ref:`jobmon-task-label` and the dependencies
between them. A Workflow is the intent to run a :ref:`jobmon-dag-label`
(Directed Acyclic Graph) of Tasks, and a :ref:`jobmon-wfrun-label` is the
actual execution of that Workflow on a cluster. A Workflow can have multiple
WorkflowRuns associated with it if previous runs fail or are stopped manually. A Workflow
can be resumed if it failed on a previous Workflow Run, but the Tasks that it will execute
must remain the same.
The ability to Resume is controlled by the :ref:`jobmon-wf-arg-label` parameter.
You can resume a Workflow if you specify a workflow with exactly the same
workflow arguments.

Jobmon expects a Workflow to run to completion. If you have a long
research process that requires human vetting of intermediate results then
each of the computation steps between human vetting steps should be a
separate Workflow.


.. _jobmon-dag-label:

DAG
###
Directed Acyclic Graph. The graph of Tasks that will be traversed during the execution of a
WorkflowRun. THe DAG is composed  of Tasks with specific node arguments and
:ref:`jobmon-edge-label` (the relationship between two Nodes)

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

.. _jobmon-wfrun-label:

WorkflowRun
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
#####
A single executable object in the workflow; a command that will be run. Relate it to a
Task Template in order to classify it as a type of job within the context of your
Workflow. Do this by using the TaskTemplate create_task() function.

.. _cluster-name-label:

Cluster Name
############
The name of the cluster users on which to to run your Tasks.
You can specify the cluster you want to use on the Task, TaskTemplate, Workflow
and Tool level. To set cluster name on Tasks, use "cluster_name".
To set cluster_name on
TaskTemplate, Workflow, and Tool, use "default_cluster_name".
If cluster name is set on multiple Jobmon objects,
Jobmon will use the standard hierarchy precedence:
*Task -> TaskTemplate -> Workflow -> Tool.*

.. _jobmon-task-attribute-label:

TaskAttribute
*************
Custom attributes of the task that can be tracked. For example, release ID or
location set version ID. Task attributes are not passed to the job but may be useful
for profiling or resource prediction work in the Jobmon database. Pass in task
attributes as a list or dictionary to create_task().

.. _jobmon-ti-label:

Task Instance
*************
The actual instance of execution of a Task command. The equivalent of a single srun on
the Slurm Cluster. Jobmon will create TaskInstances from the Tasks that you define. This
is an actual run of a task. Like calling a function in Python. One Task can have
multiple task instances if they are retried.


.. _jobmon-edge-label:

Edge
#####
The relationships between an upstream and a downstream Node.


Compute Resources
#################
Compute resources a requests for hardware and software resources
such as memory, cores, runtime,
queue, stdout, stderr, and project. Compute resources
are passed in as dictionaries.  To set compute resources on Tasks, use
"compute_resources". To set resources on TaskTemplate, Workflow, and Tool, use
"default_compute_resources". If compute resources are set on multiple objects, Jobmon has a
hierarchy of which resources will take precedence, the hierarchy is Task -> TaskTemplate ->
Workflow -> Tool.

If compute resources
are set on multiple objects,
Jobmon uses the following hierarchy to determine which resources will take precedence:
*Task -> TaskTemplate -> Workflow -> Tool.*

The default compute resources are stored in the _queue_ table
in the database.

YAML Configuration Files
************************
You can also specifiy compute resources via a YAML file, which keeps them all
in one location rather than being scattered throughout the code. Users can specify compute
resources via YAML on the Tool and TaskTemplate objects. Simply create a YAML file with the
requested resources, for example:

.. code-block:: yaml

    # tool_resources is a hardcoded Jobmon key
    tool_resources:
      # example_tool_name matches the name of a Tool defined in the python script
      example_tool_name:
          # buster corresponds to a cluster in the Jobmon database
          buster:
            cores: 1
            memory: "1G"
            runtime: (60 * 60 * 24 * 7)
            queue: "null.q"
          # slurm corresponds to a cluster in the Jobmon database
          slurm:
            cores: 2
            memory: "2G"
            runtime: (60 * 60 * 24)
            queue: "null.q"
    # task_template_resources is a hardcoded Jobmon key
    task_template_resources:
      # example_task_template_name matches the name of a TaskTemplate defined in the python script
      example_task_template_name:
        # buster corresponds to a cluster in the Jobmon database
        buster:
          num_cores: 1
          m_mem_free: "3G"
          max_runtime_seconds: (60 * 60 * 4)
          queue: "null.q"
        # slurm corresponds to a cluster in the Jobmon database
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
Each queue on both clusters have default resources specified.
These are the resources that will
be used if the user does not provide them.
For the Slurm cluster, default compute resources
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



Analogy to Programming Languages
################################

The Jobmon DAG and execution algorithm is similar to a programming language.

A TaskTemplate is analogous to a function call.
The formal arguments are the named Node Args in the call that creates the
TaskTemplate. The actual arguments are the values of the NOdeArgs when the
Workflow is created. For example, imagine a TaskTemplate created to parallelize
a disease model over locations. The TaskTemplate has a NodeArg named location_id.
When the workflow is created an a list of location_ids is passed to the Workflow,
which Jobmon uses to create a set of Tasks from that Template, one per location_id.
This is analogous to calling a function in a for-loop over those location-ids.

A Task is analogous to a function, but in this case it is the _intention to call that function._
When the workflow is executed, a TaskInstance created represents an actual call
to that function, analogous to a runtime call and its stack frame.

Abstract, Concrete, and Runtime Objects
#######################################

In the above set of objects the same concept appears in three
different points in the lifecycle of computation
1. Abstract Plan. The highest level intention of what you want to run.
For example, declare that this workflow will parallelize over a set of locations
1. Concrete Plan. ThHe actual computational plan – all their jobs and their arguments.
For example, provide the exact set of locations so the exact set of nodes and Tasks can be generated by Jobmon
1. Runtime. Actually execute the concrete Plan
For example, there could be two TaskInstances for a particular location due to a resource retry


+---------------+----------------+----------------+
| Abstract Plan | Concrete Plan  | Execution Time |
+===============+================+================+
| TaskTemplate  | Task           | Task Instance  |
+---------------+----------------+----------------+
| Tool          |                |                |
+---------------+----------------+----------------+
|               | Workflow & DAG | WorkflowRun    |
+---------------+----------------+----------------+
|               | Edge           |                |
+---------------+----------------+----------------+

