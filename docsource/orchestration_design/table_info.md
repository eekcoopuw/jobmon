command_type:
  command types that jobmon can run. 
    1) run workflow
    2) run array
    3) run task
tool:
  a namespace used to group commands into logical groups. Often a git repository of different scripts that may relate to each other
    1) dismod
    2) injuries pipeline
    3) covid SEIR model
    4) mortality paper production
command:
  a named command type associated with a tool. Jobmon's most abstract intention to run something
    1) run dismod most detailed array
    2) run mortality paper tables generating script
    3) run covid SEIR model workflow
arg:
  a logical subcomponent of a command. Commands are constructed by arguments that tell the program what to do. 
    1) --location_id 101
    2) --data_version 2021.12.03.v01
    3) --workflow_id 3
    4) --command_string "{python} --location_id {location_id} --data_version {data_version}"
command_arg:
  an arg associated with a command_instance that is invariant across new instances of that command string (runs). Often command_args represent the parallelism of an array or are the "structural" arguments in a workflow.
    1) --location_id 101 is the same for every most_detailed dismod task.
command_template:
  a hash representing the set of all invarient args (command_args) in a command_instance. command_instances may share a node.
    1) a dismod most_detailed task for (--sex_id 2, --location_id 101) is distinguished from (--sex_id 1, --location_id 101)
    2) an array task with the same name and tool but a new argument has been added to the command
    3) a workflow with new parallelism but the same set of commands
command_instance:
  the unique set of args that represent an intention to run a command. a command_instance may be dependent on other command_instances or not
    1) a single callable task: /usr/bin/python run_my_code.py --data_version 123
    2) an array of tasks parallelized by location_id
    3) a workflow or arrays or tasks dependent on upstreams finishing successfully
command_instance_arg:
  an arg associated with a command_instance that varies across new instances of that command string (runs). Often node_args represent the data that moves through a node_instance.
    1) --data_version 2021.12.03.v01 vs 2022.01.01.v05
task:
  a batch executable program.
array:
  a collection of similar tasks who's execution is order immaterial to the result. only node_args may vary between tasks in an array.
workflow:
  a collection of tasks, arrays, and workflows who's interdependencies are directed and acyclic.

