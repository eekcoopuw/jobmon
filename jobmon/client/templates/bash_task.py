from typing import Optional, List, Dict, Callable, Union

from jobmon.client.task import Task
from jobmon.client.tool import Tool
from jobmon.client.execution.strategies.base import ExecutorParameters


class BashTask(Task):

    _tool = Tool("unknown")
    _task_template = _tool.get_task_template(
        template_name="bash_template",
        command_template="{env_variables} {command}",
        node_args=["env_variables", "command"],
        task_args=[],
        op_args=[])

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
                 tag: Optional[str] = None,
                 context_args: Optional[dict] = None,
                 resource_scales: Optional[Dict] = None,
                 m_mem_free: Optional[str] = None,
                 hard_limits: bool = False,
                 executor_class: str = 'DummyExecutor',
                 executor_parameters:
                 Optional[Union[ExecutorParameters, Callable]] = None):
        """
        Args:
            command (str): the command to execute using a python binary
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
                paremeters class
            """

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

        # build node arg dict
        node_arg_vals = {}
        if env_variables is not None:
            env_str = ' '.join(
                '{}={}'.format(key, val) for key, val in env_variables.items())
        else:
            env_str = ""
        node_arg_vals["env_variables"] = env_str

        # build command
        node_arg_vals["command"] = command
        command = self._task_template.task_template_version.command_template.format(
            env_variables=env_str, command=command)

        # arg id name mappings
        node_args = {self._task_template.task_template_version.id_name_map[k]: v
                     for k, v in node_arg_vals.items()}

        super().__init__(
            command=command,
            task_template_version_id=(
                self._task_template.task_template_version.id),
            node_args=node_args,
            task_args={},
            executor_parameters=executor_parameters,
            name=name,
            max_attempts=max_attempts,
            upstream_tasks=upstream_tasks,
            task_attributes=task_attributes)
