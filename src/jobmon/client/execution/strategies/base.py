import os
import shutil
from typing import Dict, List, Optional, Tuple, Type, Union

from jobmon.exceptions import RemoteExitInfoNotAvailable

import pkg_resources

import structlog as logging


logger = logging.getLogger(__name__)


class ExecutorParameters:
    """Base parameter class for executors, each executor has specific '
    parameters and must validate them accordingly"""

    _strategies: dict = {}

    def __init__(self,
                 num_cores: Optional[int] = None,
                 queue: Optional[str] = None,
                 max_runtime_seconds: Optional[int] = None,
                 j_resource: Optional[bool] = False,
                 m_mem_free: Optional[Union[str, float]] = None,
                 context_args: Optional[Union[Dict, str]] = None,
                 hard_limits: Optional[bool] = False,
                 executor_class: str = 'SGEExecutor',
                 resource_scales: Dict = None):
        """
        Args:
            num_cores: number of cores fair cluster terminology
            queue: queue to be requested for the given task depending on
                the resources the job will need
            max_runtime_seconds: the maximum runtime for the job in seconds
            j_resource: j drive access
            m_mem_free: the amount of memory to be requested, can either be in
                string format ex. '300M', '1G', '0.2T' or as a float
            context_args: additional arguments to be provided to the
                executor
            hard_limits: if the user wants jobs to stay on the chosen queue
                and not expand if resources are exceeded, set this to true
            resource_scales: for each resource, a scaling value can be provided
                so that different resources get scaled differently
            executor_class: name of the executor class so that params can
                be parsed accordingly
        """

        # initialize
        self._num_cores = num_cores
        logger.debug("num_cores: " + str(num_cores))
        self._queue = queue
        logger.debug("queue: " + str(queue))
        self._max_runtime_seconds = max_runtime_seconds
        logger.debug("max_runtime_seconds: " + str(max_runtime_seconds))
        self._j_resource = j_resource
        logger.debug("j_resource: " + str(j_resource))
        self._m_mem_free = m_mem_free
        logger.debug("m_mem_free: " + str(m_mem_free))
        self._context_args = context_args
        logger.debug("context_args: " + str(context_args))
        self._hard_limits = hard_limits
        logger.debug("hard_limits: " + str(hard_limits))
        self._resource_scales = resource_scales
        logger.debug("resource_scales: " + str(resource_scales))

        StrategyCls = self._strategies.get(executor_class)
        self._strategy: Optional[Type[ExecutorParameters]] = None

        if StrategyCls is None and executor_class == "SGEExecutor":
            raise RuntimeError("SGEExecutor was specified but SGEExecutor was "
                               "not imported before setting ExecutorParameters."
                               "The Executor you use is a workflow-level "
                               "relationship, therefore if you are initializing"
                               " a Task with Executor Parameters before you "
                               "have initialized the Workflow with the "
                               "associated Executor Class, you may not yet be "
                               "allowed to use that strategy. Initialize the "
                               "Workflow and then create your tasks in order to"
                               " ensure the strategy is available.")

        if StrategyCls is not None:
            StrategyCls.set_executor_parameters_strategy(self)

        self._is_valid = False

    @classmethod
    def add_strategy(cls, StrategyCls, executor_class: str):
        cls._strategies[executor_class] = StrategyCls

    def _attribute_proxy(self, attr_name: str):
        """checks whether executor specific class has implemented given
        paremeter and returns it, or else returns base implemenetation"""
        if self._strategy is not None and hasattr(self._strategy, attr_name):
            return getattr(self._strategy, attr_name)
        else:
            return getattr(self, "_" + attr_name)

    @property
    def num_cores(self):
        return self._attribute_proxy("num_cores")

    @property
    def queue(self):
        return self._attribute_proxy("queue")

    @property
    def max_runtime_seconds(self):
        return self._attribute_proxy("max_runtime_seconds")

    @property
    def j_resource(self):
        return self._attribute_proxy("j_resource")

    @property
    def m_mem_free(self):
        return self._attribute_proxy("m_mem_free")

    @property
    def context_args(self):
        return self._attribute_proxy("context_args")

    @property
    def resource_scales(self):
        return self._attribute_proxy("resource_scales")

    @property
    def hard_limits(self):
        return self._attribute_proxy("hard_limits")

    def is_valid(self) -> Tuple[bool, Optional[str]]:
        if self._strategy is not None:
            msg = self._strategy.validation_msg()
            if msg:
                return False, msg
        return True, None

    def adjust(self, **kwargs) -> None:
        """adjust executor specific values when resource error is encountered
        """
        if self._strategy is not None:
            self._strategy.adjust(**kwargs)

    def validate(self):
        """convert invalid parameters to valid ones for a given executor"""
        if self._strategy is not None:
            self._strategy.validate()

    def is_valid_throw(self):
        """
        Calls validate and converts a False result into an exception
        """
        if self._strategy is not None:
            msg = self._strategy.validation_msg()
            if msg:
                raise ValueError(msg)

    def to_wire(self):
        return {
            'max_runtime_seconds': self.max_runtime_seconds,
            'context_args': self.context_args,
            'queue': self.queue,
            'num_cores': self.num_cores,
            'm_mem_free': self.m_mem_free,
            'j_resource': self.j_resource,
            'resource_scales': self.resource_scales,
            'hard_limits': self.hard_limits}


class Executor:
    """Base class for executors. Subclasses are required to implement an
    execute() method that takes a TaskInstance, constructs a
    jobmon-interpretable executable command (typically using this base class's
    build_wrapped_command()), and optionally returns an executor_id.

    Also optional, get_actual_submitted_or_running() and
    terminate_task_instances() are recommended in case jobs fail in ways
    that they are unable to contact Jobmon re: the reasons for their failure.
    These methods will allow jobmon to identify jobs that have been lost
    and retry them.
    """

    def __init__(self, *args, **kwargs) -> None:
        self.temp_dir: Optional[str] = None
        self.started = False
        self._jobmon_command = shutil.which(("jobmon_command"))
        logger.info("Initializing {}".format(self.__class__.__name__))

    def start(self, jobmon_command=None) -> None:
        self.jobmon_command = jobmon_command
        self.started = True

    def stop(self, executor_ids, report_by_buffer) -> None:
        self.started = False

    @property
    def jobmon_command(self) -> str:
        return self._jobmon_command

    @jobmon_command.setter
    def jobmon_command(self, val: str) -> None:
        if val is None:
            val = shutil.which("jobmon_command")
        if val is None:
            raise ValueError("jobmon_command cannot be None. Must be path to "
                             "jobmon worker node jobmon_cli")
        self._jobmon_command = val

    def execute(self, command: str, name: str,
                executor_parameters: ExecutorParameters, executor_ids) -> \
            Tuple[int, Dict[int, int]]:
        """SUBCLASSES ARE REQUIRED TO IMPLEMENT THIS METHOD.

        It is recommended that subclasses use build_wrapped_command() to
        generate the executable command string itself. It is then up to the
        Executor subclass to provide a means of actually executing that
        command.

        Optionally, return an (int) executor_id which the subclass could
        use at a later time to identify the associated TaskInstance, terminate
        it, monitor for missingness, or collect usage statistics. If the
        subclass does not intend to offer those functionalities, this method
        can return None.

        Args:
            command: command to be run
            name: name of task
            executor_parameters: executor specific requested resources
            executor_ids: running executor task ids already being tracked
        """
        raise NotImplementedError

    def get_remote_exit_info(self, executor_id: int) -> Tuple[str, str]:
        raise RemoteExitInfoNotAvailable

    def get_errored_jobs(self, executor_ids: Dict):
        raise NotImplementedError

    def get_actual_submitted_or_running(self, executor_ids: Dict, report_by_buffer) -> \
            Tuple[List[int], Dict[int, int]]:
        raise NotImplementedError

    def terminate_task_instances(self, executor_ids: List[int]) -> None:
        """If implemented, return a list of (task_instance_id, hostname) tuples
        for any task_instances that are terminated
        """
        logger.warning("terminate_task_instances not implemented by executor: "
                       f"{self.__class__.__name__}")

    def build_wrapped_command(self, command: str, task_instance_id: int,
                              heartbeat_interval: int, report_by_buffer: float
                              ) -> str:
        """Build a command that can be executed by the shell and can be
        unwrapped by jobmon itself to setup proper communication channels to
        the monitor server.
        Args:
            command: command to run the desired job
            task_instance_id: id for the given instance of this task

        Returns:
            (str) unwrappable command

        """
        wrapped_cmd = [
            "--command", f"'{command}'",
            "--task_instance_id", task_instance_id,
            "--expected_jobmon_version",
            pkg_resources.get_distribution("jobmon").version,
            "--executor_class", self.__class__.__name__,
            "--heartbeat_interval", heartbeat_interval,
            "--report_by_buffer", report_by_buffer
        ]
        if self.temp_dir and 'stata' in command:
            wrapped_cmd.extend(["--temp_dir", self.temp_dir])
        str_cmd = " ".join([str(i) for i in wrapped_cmd])
        logger.debug(str_cmd)
        return str_cmd

    def set_temp_dir(self, temp_dir: str) -> None:
        self.temp_dir = temp_dir
        os.environ["JOBMON_TEMP_DIR"] = self.temp_dir


class TaskInstanceExecutorInfo:
    """Base class defining interface for gathering executor specific info
    in the execution_wrapper.

    While not required, implementing get_usage_stats() will allow collection
    of CPU/memory utilization stats for each job.

    Get exit info is used to determine the error type if the task hits a
    system error of some variety.
    """

    @property
    def executor_id(self) -> Optional[int]:
        raise NotImplementedError

    def get_usage_stats(self) -> Dict:
        raise NotImplementedError

    def get_exit_info(self, exit_code, error_msg) -> Tuple[str, str]:
        raise NotImplementedError


class TaskInstanceStatus:
    """TaskInstanceStatus DB Model.

    TODO: Break out statuses that are specific to their executor (eqw will only happen on SGE
    as will Resource Error)
    """

    INSTANTIATED = 'I'
    NO_EXECUTOR_ID = 'W'
    SUBMITTED_TO_BATCH_EXECUTOR = 'B'
    RUNNING = 'R'
    RESOURCE_ERROR = 'Z'
    UNKNOWN_ERROR = 'U'
    ERROR = 'E'
    DONE = 'D'
    KILL_SELF = 'K'
