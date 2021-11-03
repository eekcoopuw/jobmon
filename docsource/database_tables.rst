***************
Database Tables
***************

arg
###
    A list of args that the node_args and task_args use.

arg_type
########
    The different types of args (NODE_ARG, TASK_ARG, OP_ARG).

command_template_arg_type_mapping
#################################
    A table that associates a TaskTemplate version with arg types.

dag
###
    Has every entry of dags created, as identified by it's id and hash.

edge
####
    A table that shows the relationship between two nodes.

executor_parameter_set
######################
    The executor-specific parameters of a given Task, e.g max_runtime_seconds, m_mem_free,
    num_cores etc.

executor_parameteter_set_type
#############################
    The type of parameters (original requested, validated, adjusted).

node
####
    The object representing a Task within a DAG. Table includes TaskTemplate version and the
    hash of the node args.

node_arg
########
    Args that identify a unique node in the DAG.

task
####
    A single executable object in the workflow. The table includes the name of the task, the
    command it submitted, and it's executor parameters.

task_arg
########
    A list of args that make a command unique across different workflows, includes task_id,
    arg_id and the associated value.

task_attribute
##############
    Additional attributes of the task that can be tracked. For example, release ID or location
    set version ID. Task attributes are not passed to the job but may be useful for profiling
    or resource prediction work in the Jobmon database. Pass in task attributes as a list or
    dictionary to create_task().

task_attribute_type
###################
    Types of task attributes that can be tracked.

task_instance
#############
    This is an actual run of a task. Like calling a function in Python. One Task can have
    multiple task instances if they are retried.

task_instance_error_log
#######################
    Any errors that are produced by a task instance are logged in this table.

task_instance_status
####################
    Meta-data table that defines the ten states of Task Instance.

task_status
###########
    Meta-data table that defines the eight states of Task:

task_template
#############
    This table has every TaskTemplate, paired with it's tool_version_id.

task_template_version
#####################
    A table listing the different versions a TaskTemplate can have.

tool
####
    A table that shows the list of Tools that can be associated with your Workflow and
    TaskTemplates.

tool_version
############
    A table listing the different versions a Tool has.

workflow
########
    This table has every Workflow created, along with it’s associated dag_id,
    and workflow_args.

workflow_attribute
##################
    Additional attributes that are being tracked for a given Workflow. They are not required
    to use Jobmon, and workflow_attributes are not passed to your jobs. They are intended to
    track information for a given run and can be utilized for profiling and resource
    prediction.

workflow_attribute_type
#######################
    The types of attributes that can be tracked for Workflows.

workflow_run
############
    This table has every run of a workflow, paired with it's workflow, as identified by
    workflow_id.

workflow_run_status
###################
    Meta-data table that defines the ten states of Workflow Run.

workflow_status
###############
    Meta-data table that defines eight states of Workflow.
