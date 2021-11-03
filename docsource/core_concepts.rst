*************
Core Concepts
*************

Tool
####

Workflow
########

Workflow Run
************

Task Template
##############

Task
####

Task Instance
*************

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
