import inspect
import logging
import os
import shutil
from typing import List, Tuple, Dict, Optional, Type, Union

from jobmon.client import client_config
from jobmon.exceptions import RemoteExitInfoNotAvailable


logger = logging.getLogger(__name__)


class ExecutorParameters:
    """Base parameter class for executors, each executor has specific '
    parameters and must validate them accordingly"""

    # TODO: consider converting this into a stragegy based class instead of an
    # abstract base class

    def __init__(self, *args, **kwargs):
        logger.info("Initializing Base Class ExecutorParameters")

    def return_adjusted(self) -> 'ExecutorParameters':
        """
        If the parameters need to be adjusted then create and return a new
        object, otherwise None
        """
        raise NotImplementedError

    def is_valid(self) -> Tuple[bool, Dict, Union[str, None]]:
        """
        If the object is valid, return (True, None), otherwise
        (False, error_message), deliberately does not throw so it can be used
        where the client does not want an exception
        """
        return True, {}, None

    def return_validated(self, params) -> 'ExecutorParameters':
        return self

    def is_valid_throw(self) -> Tuple[bool, 'ExecutorParameters']:
        """
        Calls is_valid and converts a False result into an exception
        """
        valid, obj, message = self.is_valid()
        if valid:
            return True, obj
        else:
            raise ValueError(message)

    @classmethod
    def parse_constructor_kwargs(cls, kwarg_dict: Dict) -> Tuple[Dict, Dict]:
        argspec = inspect.getfullargspec(cls.__init__)
        constructor_kwargs = {}
        for arg in argspec.args:
            if arg in kwarg_dict:
                constructor_kwargs[arg] = kwarg_dict.pop(arg)
        return kwarg_dict, constructor_kwargs

    def to_wire(self):
        return {}


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
