*************
Core Concepts
*************

Tool
####
A tool is the project (e.g. STG-PR, CODCorrect) that you want to associate your Workflow and
Task Templates with. A Tool keeps track of where Workflows and Tasks are within the IHME
pipeline.

Workflow
########

Workflow Run
************

Task Template
##############
TaskTemplates are the underlying structure of a given Task. A user defines a command template that
individual Tasks will fill in with varying arguments. A Task's uniqueness is defined by it's
NodeArgs and TaskArgs. A Task Template can be used in different Workflows and is
associated with a given Tool. TaskTemplates can also be versioned, meaning you can iterate
upon them. A user would create a new version of their TaskTemplate if the command changes or
if the underlying methods change in a way that the user wants to recognize as different from
before.

Task
####

Task Instance
*************

Nodes
#####
Nodes are the object representing a Task within a DAG. It simply keeps track of where a
Task is and what attributes make the task unique within the DAG. Tasks
will often be created from a TaskTemplate and they will vary somewhat e.g. by location, this
variation is what makes a Node unique.

Compute Resources
#################

YAML Configuration Files
************************

Default Resources
*****************

Required Resources
******************

Dependencies
############

Arguments
#########

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

task_args
*********
    Any named arguments in command_template that make the command unique across workflows if
    the node args are the same as a previous workflow. Generally these are arguments about
    data moving though the task, e.g. release_id.
