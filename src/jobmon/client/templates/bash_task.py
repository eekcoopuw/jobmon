from typing import Optional, List, Dict, Callable, Union

from jobmon.client.task import Task
from jobmon.client.tool import Tool
from jobmon.client.execution.strategies.base import ExecutorParameters


class BashTask(Task):

    _bash_task_template_registry: Dict = {}

    def __init__(self,
                 command: str,
                 upstream_tasks: List[Task] = [],
                 task_attributes: Union[List, Dict] = {},
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
                 executor_parameters: Optional[Union[ExecutorParameters, Callable]] = None,
                 tool: Optional[Tool] = None,
                 task_args: Optional[Dict] = None,
                 node_args: Optional[Dict] = None,
                 op_args: Optional[Dict] = None):
        """
        Bash Task object can be used by users upgrading from older versions of Jobmon
        (version < 2.0). It sets a default tool and task template for the user, however if the
        user wants to build out their objects to better classify their tasks, they should use
        the Task and Task Template objects. Some arg tracking functionality is allowed with
        task_args, node_args, and op_args, however for more complex tracking, migrating to a
        task template may be easier.
        The arg parsing behavior is: if there are flags with the
        same name as the arg key, then replace the value in the template with the arg name, if
        there are no flags, then search for the matching value, and replace it with the node
        arg there is an inherent parsing problem if there are multiple args with the same
        value, but if they are ordered as node args, then task args, then op args in the
        command, then it will be ok.

        Args:
            command (str): the command to execute
            upstream_tasks: Task objects that must be run prior to this
            task_attributes (dict or list): attributes and their values or
                just the attributes that will be given values later
            env_variables: any environment variable that should be set
                for this job, in the form of a key: value pair.
                This will be prepended to the command.
            name: name that will be visible in qstat for this job
            num_cores: number of cores to request on the cluster
            max_runtime_seconds: how long the job should be allowed to run
                before the executor kills it. Default is None, for indefinite.
            queue: queue of cluster nodes to submit this task to. Must be
                a valid queue, as defined by "qconf -sql"
            m_mem_free: amount of memory in gbs, tbs, or mbs, G, T, or M,
                to request on the fair cluster.
            max_attempts: number of attempts to allow the cluster to try
                before giving up. Default is 3
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
                executor parameters properly
            executor_parameters: an instance of executor
                parameters class
            tool: tool to associate bash task with
            task_args: if the user wants to supply arguments to describe the data arguments for
                this task
            node_args: if the user wants to supply arguments to describe the arguments that
                make this task unique within a set of task with identical command patterns
            op_args: if the user wants to supply arguments to describe the operational
                arguments for this task
            """
        if task_args is None:
            task_args = {}
        if node_args is None:
            node_args = {}
        if op_args is None:
            op_args = {}
        # build op arg dict for environmental variables
        if env_variables is not None:
            env_str = ' '.join(
                '{}={}'.format(key, val) for key, val in env_variables.items())
            op_args["env_variables"] = env_str
        else:
            env_str = ""
        full_command = f'{env_str} {command}'.strip()
        if not node_args and not task_args and len(op_args) < 2:
            command_template = '{command}'
            node_args['command'] = full_command
        else:
            command_template, node_args, task_args, op_args = self._parse_command_to_args(
                full_command, node_args, task_args, op_args)

        try:
            task_template = self._bash_task_template_registry[command]
        except KeyError:
            if tool is None:
                tool = Tool("unknown")
            task_template = tool.get_task_template(
                template_name='bash_task',
                command_template=command_template,
                node_args=list(node_args.keys()),
                task_args=list(task_args.keys()),
                op_args=list(op_args.keys())
            )
            self._add_task_template_to_registry(command, task_template)

        # construct deprecated API for executor_parameters
        if executor_parameters is None:
            executor_parameters = ExecutorParameters(
                num_cores=num_cores,
                m_mem_free=m_mem_free,
                max_runtime_seconds=max_runtime_seconds,
                queue=queue,
                j_resource=j_resource,
                context_args=context_args,
                resource_scales=resource_scales,
                hard_limits=hard_limits,
                executor_class=executor_class)

        node_args = {task_template.task_template_version.id_name_map[k]: v for k, v in
                     node_args.items() if k in task_template.task_template_version.node_args}
        task_args = {task_template.task_template_version.id_name_map[k]: v for k, v in
                     task_args.items() if k in task_template.task_template_version.task_args}

        super().__init__(
            command=full_command,
            task_template_version_id=(
                task_template.task_template_version.id),
            node_args=node_args,
            task_args=task_args,
            executor_parameters=executor_parameters,
            name=name,
            max_attempts=max_attempts,
            upstream_tasks=upstream_tasks,
            task_attributes=task_attributes)

    @classmethod
    def _add_task_template_to_registry(cls, command, task_template):
        cls._bash_task_template_registry[command] = task_template
