import inspect
import logging
import os
import shutil
from typing import List, Tuple, Dict, Optional, Type, Union
import warnings

from jobmon.client import client_config
from jobmon.client.swarm.executors.sge_parameters import SGEParameters
from jobmon.exceptions import RemoteExitInfoNotAvailable


logger = logging.getLogger(__name__)


class ExecutorParameters:
    """Base parameter class for executors, each executor has specific '
    parameters and must validate them accordingly"""

    _strategies = {'SGEExecutor': SGEParameters}
    _executor_parameter_args = [
        "slots", "mem_free", "num_cores", "queue", "max_runtime_seconds",
        "j_resource", "m_mem_free", "context_args"]

    def __init__(self,
                 slots: Optional[int] = None,
                 mem_free: Optional[int] = None,
                 num_cores: Optional[int] = None,
                 queue: Optional[str] = None,
                 max_runtime_seconds: Optional[int] = None,
                 j_resource: bool = False,
                 m_mem_free: Optional[Union[str, float]] = None,
                 context_args: Optional[Union[Dict, str]] = None,
                 executor_class: str = 'SGEExecutor'):

        if slots is not None:
            warnings.warn(
                "slots is deprecated and will be removed in a future release",
                FutureWarning)
        if mem_free is not None:
            warnings.warn(
                "mem_free is deprecated and will be removed in a future"
                " release", FutureWarning)

        # initialize
        self._slots = slots
        self._mem_free = mem_free
        self._num_cores = num_cores
        self._queue = queue
        self._max_runtime_seconds = max_runtime_seconds
        self._j_resource = j_resource
        self._m_mem_free = m_mem_free
        self._context_args = context_args

        StrategyCls = self._strategies.get(executor_class)
        self._strategy: Optional[SGEParameters] = None
        if StrategyCls is not None:
            StrategyCls.set_executor_parameters_strategy(self)
        else:
            if slots is not None:
                raise ValueError(
                    "slots is only supported when using the SGEExecutor class")
            if mem_free is not None:
                raise ValueError(
                    "mem_free is only supported when using the SGEExecutor "
                    "class")

        self._is_valid = False

    def _attribute_proxy(self, attr_name):
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

    def is_valid(self) -> Tuple[bool, Optional[str]]:
        if self._strategy is not None:
            msg = self._strategy.validation_msg()
            if msg:
                return False, msg
        # TODO: implement any base typing logic
        return True, None

    def adjust(self, **kwargs) -> None:
        """
        Create a new parameter object with adjusted params, kwargs map any
        """
        if self._strategy is not None:
            self._strategy.adjust(**kwargs)

    def validate(self):
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

    @classmethod
    def parse_constructor_kwargs(cls, kwarg_dict: Dict) -> Tuple[Dict, Dict]:
        argspec = inspect.getfullargspec(cls.__init__)
        constructor_kwargs = {}
        for arg in argspec.args:
            if arg in kwarg_dict:
                constructor_kwargs[arg] = kwarg_dict.pop(arg)
        return kwarg_dict, constructor_kwargs

    def to_wire(self):
        return {
            'max_runtime_seconds': self.max_runtime_seconds,
            'context_args': self.context_args,
            'queue': self.queue,
            'num_cores': self.num_cores,
            'm_mem_free': self.m_mem_free,
            'j_resource': self.j_resource}


class Executor:
    """Base class for executors. Subclasses are required to implement an
    execute() method that takes a JobInstance, constructs a
    jobmon-interpretable executable command (typically using this base class's
    build_wrapped_command()), and optionally returns an executor_id.

    Also optional, get_actual_submitted_or_running() and
    terminate_job_instances() are recommended in case jobs fail in ways
    that they are unable to contact Jobmon re: the reasons for their failure.
    These methods will allow jobmon to identify jobs that have been lost
    and retry them.
    """
    ExecutorParameters_cls: Type[ExecutorParameters] = ExecutorParameters

    def __init__(self, *args, **kwargs) -> None:
        self.temp_dir: Optional[str] = None
        logger.info("Initializing {}".format(self.__class__.__name__))

    def execute(self, command: str, name: str,
                executor_parameters: ExecutorParameters) -> int:
        """SUBCLASSES ARE REQUIRED TO IMPLEMENT THIS METHOD.

        It is recommended that subclasses use build_wrapped_command() to
        generate the executable command string itself. It is then up to the
        Executor subclass to provide a means of actually executing that
        command.

        Optionally, return an (int) executor_id which the subclass could
        use at a later time to identify the associated JobInstance, terminate
        it, monitor for missingness, or collect usage statistics. If the
        subclass does not intend to offer those functionalities, this method
        can return None.
        """
        raise NotImplementedError

    def get_remote_exit_info(self, executor_id: int) -> Tuple[str, str]:
        raise RemoteExitInfoNotAvailable

    def get_actual_submitted_or_running(self) -> List[int]:
        raise NotImplementedError

    def terminate_job_instances(self, jiid_exid_tuples: List[Tuple[int, int]]
                                ) -> List[Tuple[int, str]]:
        """If implemented, return a list of (job_instance_id, hostname) tuples
        for any job_instances that are terminated
        """
        raise NotImplementedError

    def build_wrapped_command(self, command: str, job_instance_id: int,
                              last_nodename: Optional[str] = None,
                              last_process_group_id: Optional[int] = None
                              ) -> str:
        """Build a command that can be executed by the shell and can be
        unwrapped by jobmon itself to setup proper communication channels to
        the monitor server.

        Args:
            job (job.Job): the job to be run
            job_instance_id (int): the id of the job_instance to be run

        Returns:
            (str) unwrappable command
        """
        jobmon_command = client_config.jobmon_command
        if not jobmon_command:
            jobmon_command = shutil.which("jobmon_command")
        wrapped_cmd = [
            jobmon_command,
            "--command", f"'{command}'",
            "--job_instance_id", job_instance_id,
            "--jm_host", client_config.jm_conn.host,
            "--jm_port", client_config.jm_conn.port,
            "--executor_class", self.__class__.__name__,
            "--heartbeat_interval", client_config.heartbeat_interval,
            "--report_by_buffer", client_config.report_by_buffer
        ]
        if self.temp_dir and 'stata' in command:
            wrapped_cmd.extend(["--temp_dir", self.temp_dir])
        if last_nodename:
            wrapped_cmd.extend(["--last_nodename", last_nodename])
        if last_process_group_id:
            wrapped_cmd.extend(["--last_pgid", last_process_group_id])
        str_cmd = " ".join([str(i) for i in wrapped_cmd])
        logger.debug(str_cmd)
        return str_cmd

    def set_temp_dir(self, temp_dir: str) -> None:
        self.temp_dir = temp_dir
        os.environ["JOBMON_TEMP_DIR"] = self.temp_dir


class JobInstanceExecutorInfo:
    """Base class defining interface for gathering executor specific info
    in the execution_wrapper.

    While not required, implementing get_usage_stats() will allow collection
    of CPU/memory utilization stats for each job.

    Get exit info is used to determine the error type if the job hits a
    system error of some variety.
    """

    @property
    def executor_id(self) -> Optional[int]:
        raise NotImplementedError

    def get_usage_stats(self) -> Dict:
        raise NotImplementedError

    def get_exit_info(self, exit_code, error_msg) -> Tuple[str, str]:
        raise NotImplementedError
