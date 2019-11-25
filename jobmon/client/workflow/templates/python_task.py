import sys
from typing import Optional, List, Dict, Callable, Union

from jobmon.client.workflow.task import Task
from jobmon.client.workflow.tool import Tool
from jobmon.client.swarm.executors.base import ExecutorParameters


class PythonTask(Task):

    _python_task_template_registry: dict = {}
    current_python = sys.executable

    def __init__(self, path_to_python_binary=current_python, script=None,
                 args=None,
                 upstream_tasks: List[Task] = [],
                 env_variables: Optional[Dict[str, str]] = None,
                 name: Optional[str] = None,
                 num_cores: Optional[int] = None,
                 max_runtime_seconds: Optional[int] = None,
                 queue: Optional[str] = None,
                 max_attempts: Optional[int] = 3,
                 j_resource: bool = False,
                 context_args: Optional[dict] = None,
                 resource_scales: Dict = None,
                 task_attributes: Optional[dict] = None,
                 m_mem_free: Optional[str] = None,
                 hard_limits: Optional[bool] = False,
                 executor_class: str = 'DummyExecutor',
                 executor_parameters:
                 Optional[Union[ExecutorParameters, Callable]] = None):
        """
        Args:
            path_to_python_binary (str): the python install that should be used
                Default is the Python install where Jobmon is installed
            script (str): the full path to the python code to run
            args (list): list of arguments to pass in to the script
            upstream_tasks: Task objects that must be run prior to this
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
            tag: a group identifier. Currently just used for visualization.
                All tasks with the same tag will be colored the same in a
                TaskDagViz instance. Default is None.
            queue: queue of cluster nodes to submit this task to. Must be
                a valid queue, as defined by "qconf -sql"
            task_attributes: any attributes that will be
                tracked. Once the task becomes a job and receives a job_id,
                these attributes will be used for the job_factory
                add_job_attribute function
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

        # script cannot be None at this point
        if script is None:
            raise ValueError("script cannot be None")

        try:
            task_template = self._python_task_template_registry[script]
        except KeyError:
            tool = Tool("unknown")
            task_template = tool.get_task_template(
                template_name=script,
                command_template=(
                    "{env_variables} "
                    "{path_to_python_binary} "
                    f"{script} "
                    "{command_line_args}"),
                node_args=["env_variables", "command_line_args"],
                task_args=[],
                op_args=["path_to_python_binary"])
            self._add_task_template_to_registry(script, task_template)

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

        if args is not None:
            command_line_args = ' '.join([str(x) for x in args])
        else:
            command_line_args = ""
        node_arg_vals["command_line_args"] = command_line_args

        command = task_template.command_template.format(
            env_variables=env_str, path_to_python_binary=path_to_python_binary,
            command_line_args=command_line_args)

        # arg id name mappings
        node_args = {task_template.arg_id_name_map[k]: v
                     for k, v in node_arg_vals.items()}

        super().__init__(
            command=command,
            task_template_version_id=task_template.task_template_version_id,
            node_args=node_args,
            task_args={},
            executor_parameters=executor_parameters,
            name=name,
            max_attempts=max_attempts,
            upstream_tasks=upstream_tasks,
            task_attributes=task_attributes)

    @classmethod
    def _add_task_template_to_registry(cls, script, task_template):
        cls._python_task_template_registry[script] = task_template
