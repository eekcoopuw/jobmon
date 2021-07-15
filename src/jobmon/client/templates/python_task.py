"""Python Task for backward compatibility with Jobmon 1.* series. Used for Tasks that execute
a python script.
"""
import getpass
import sys
from typing import Any, Dict, List, Optional, Union

from jobmon.client.task import Task
from jobmon.client.tool import Tool


class PythonTask(Task):
    """Python Task for backward compatibility with Jobmon 1.* series. Used for Tasks that
    execute a python script.
    """

    _tool_registry: Dict[str, Tool] = {}

    current_python = sys.executable

    def __init__(self, path_to_python_binary: str = current_python,
                 script: Optional[str] = None,
                 args: Optional[List[Any]] = None,
                 upstream_tasks: List[Task] = [],
                 task_attributes: Union[dict, List] = {},
                 env_variables: Optional[Dict[str, str]] = None,
                 name: Optional[str] = None,
                 num_cores: Optional[int] = None,
                 max_runtime_seconds: Optional[int] = None,
                 queue: Optional[str] = None,
                 max_attempts: int = 3,
                 j_resource: bool = False,
                 context_args: Optional[dict] = None,
                 resource_scales: Optional[Dict] = None,
                 m_mem_free: Optional[str] = None,
                 hard_limits: bool = False,
                 executor_class: str = 'DummyExecutor',
                 # TODO: Replacee executor_parameters, with compute_resources
                 executor_parameters: Any = None,
                 # executor_parameters: Optional[Union[ExecutorParameters, Callable]] = None,
                 tool: Optional[Tool] = None):
        """
        Python Task object can be used by users upgrading from older versions of
        Jobmon (version < 2.0). It sets a default of unknown tool and a task
        template for each unique script for the user, however if the user wants
        to build out their objects to better classify their tasks, they should
        use the Task and Task Template objects.

        Args:
            path_to_python_binary (str): the python install that should be used
                Default is the Python install where Jobmon is installed
            script (str): the full path to the python code to run
            args (list): list of arguments to pass in to the script
            upstream_tasks: Task objects that must be run prior to this
            task_attributes (dict or list): attributes and their values or
                just the attributes that will be given values later
            env_variables: any environment variable that should be set
                for this job, in the form of a key: value pair.
                This will be prepended to the command.
            name: name that will be visible in qstat for this job
            num_cores: number of cores to request on the cluster
            m_mem_free: amount of memory in gbs, tbs, or mbs, G, T, or M,
                to request on the fair cluster.
            max_attempts: number of attempts to allow the cluster to try
                before giving up. Default is 3
            max_runtime_seconds: how long the job should be allowed to run
                before the executor kills it. Default is None, for indefinite.
            queue: queue of cluster nodes to submit this task to. Must be
                a valid queue, as defined by "qconf -sql"
            j_resource: whether this task is using the j-drive or not
            context_args: additional args to be passed to the executor
            resource_scales: for each resource, a scaling value (between 0 and
                1) can be provided so that different resources get scaled
                differently. Default is:
                {'m_mem_free': 0.5, 'max_runtime_seconds': 0.5},
                only resources that are provided
                will ever get adjusted
            hard_limits: if the user wants jobs to stay on the chosen queue
                and not expand if resources are exceeded, set this to true
            executor_class: the type of executor so we can instantiate the
                executor parameters properly. Default is Dummy Executor.
            executor_parameters: an instance of executor
                parameters class
            tool: tool to associate python task with
        """
        # script cannot be None at this point
        if script is None:
            raise ValueError("script cannot be None")

        command_template = "{path_to_python_binary} " + script

        # define node args
        node_args: Dict[str, str] = {}
        if args:
            for i in range(len(args)):
                arg_name = f'arg_{i}'
                command_template += " {{{arg}}}".format(arg=arg_name)
                node_args[arg_name] = args[i]

        # define op args
        op_args = {"path_to_python_binary": path_to_python_binary}
        if env_variables is not None:
            env_str = ' '.join(
                '{}={}'.format(key, val) for key, val in env_variables.items()
            )
            op_args["env_str"] = env_str
            command_template = "{env_str} " + command_template

        # tool is a task template registry
        if tool is None:
            tool_name = f"unknown-{getpass.getuser()}"
        else:
            tool_name = tool.name
        try:
            tool = self._tool_registry[tool_name]
        except KeyError:
            if tool is None:
                tool = Tool()
            self._add_tool_to_registry(tool)

        task_template = tool.get_task_template(
            template_name=script,
            command_template=command_template,
            node_args=list(node_args.keys()),
            task_args=list(),
            op_args=list(op_args.keys())
        )

        # construct deprecated API for executor_parameters
        if executor_parameters is None:
            # TODO: replace executor parameters with compute resources
            executor_parameters = {}
            # executor_parameters = ExecutorParameters(
            #     num_cores=num_cores,
            #     m_mem_free=m_mem_free,
            #     max_runtime_seconds=max_runtime_seconds,
            #     queue=queue,
            #     j_resource=j_resource,
            #     context_args=context_args,
            #     resource_scales=resource_scales,
            #     hard_limits=hard_limits,
            #     executor_class=executor_class
            # )

        command = command_template.format(**op_args, **node_args)
        id_node_args = {task_template.active_task_template_version.id_name_map[k]: v
                        for k, v in node_args.items()
                        if k in task_template.active_task_template_version.node_args}
        super().__init__(
            command=command.strip(),
            task_template_version_id=task_template.active_task_template_version.id,
            node_args=id_node_args,
            task_args={},
            executor_parameters=executor_parameters,
            name=name,
            max_attempts=max_attempts,
            upstream_tasks=upstream_tasks,
            task_attributes=task_attributes)

    @classmethod
    def _add_tool_to_registry(cls, tool: Tool):
        cls._tool_registry[tool.name] = tool
