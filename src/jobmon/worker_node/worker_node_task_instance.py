"""The Task Instance Object once it has been submitted to run on a worker node."""
from functools import partial
from io import TextIOBase
import logging
import logging.config
import os
from pathlib import Path
from queue import Queue
import signal
import socket
import subprocess
import sys
from threading import Thread
from time import sleep, time
from typing import Dict, Optional, Union

from jobmon.cluster_type import ClusterWorkerNode
from jobmon.constants import TaskInstanceStatus
from jobmon.exceptions import InvalidResponse, ReturnCodes, TransitionError
from jobmon.requester import http_request_ok, Requester
from jobmon.serializers import SerializeTaskInstance
from jobmon.worker_node.worker_node_config import WorkerNodeConfig

logger = logging.getLogger(__name__)


class WorkerNodeTaskInstance:
    """The Task Instance object once it has been submitted to run on a worker node."""

    def __init__(
        self,
        cluster_interface: ClusterWorkerNode,
        task_instance_id: int,
        stdout: Optional[Path] = None,
        stderr: Optional[Path] = None,
        heartbeat_interval: int = 90,
        report_by_buffer: float = 3.1,
        command_interrupt_timeout: int = 10,
        requester: Optional[Requester] = None,
    ) -> None:
        """A mechanism whereby a running task_instance can communicate back to the JSM.

         Logs its status, errors, usage details, etc.

        Args:
            cluster_interface: interface that gathers executor info in the execution_wrapper.
            task_instance_id: the id of the task_instance that is reporting back.
            array_id: If this is an array job, the corresponding array ID
            batch_number: If this is an array job, what is the batch?
            heartbeat_interval: how ofter to log a report by with the db
            report_by_buffer: multiplier for report by date in case we miss a few.
            command_interrupt_timeout: the amount of time to wait for the child process to
                terminate.
            requester: communicate with the flask services.
        """
        # identity attributes
        self._task_instance_id = task_instance_id
        self.stdout = stdout
        self.stderr = stderr

        # service API
        if requester is None:
            requester = Requester(WorkerNodeConfig.from_defaults().url)
        self.requester = requester

        # cluster API
        self.cluster_interface = cluster_interface

        # get distributor id from executor
        self._distributor_id = self.cluster_interface.distributor_id

        # config
        self.heartbeat_interval = heartbeat_interval
        self.report_by_buffer = report_by_buffer
        self.command_interrupt_timeout = command_interrupt_timeout

        # set last heartbeat
        self.last_heartbeat_time = time()

    @property
    def task_instance_id(self) -> int:
        """Returns a task instance ID if it's been bound."""
        if self._task_instance_id is None:
            raise AttributeError("Cannot access task_instance_id because it is None.")
        return self._task_instance_id

    @property
    def distributor_id(self) -> Optional[str]:
        """Executor id given from the executor it is being run on."""
        logger.debug("distributor_id: " + str(self._distributor_id))
        return self._distributor_id

    @property
    def nodename(self) -> Optional[str]:
        """Node it is being run on."""
        if not hasattr(self, "_nodename"):
            self._nodename = socket.getfqdn()
        return self._nodename

    @property
    def process_group_id(self) -> Optional[int]:
        """Process group to track parent and child processes."""
        if not hasattr(self, "_process_group_id"):
            self._process_group_id = os.getpid()
        return self._process_group_id

    @property
    def status(self) -> str:
        """Returns the last known status of the task instance."""
        if not hasattr(self, "_status"):
            raise AttributeError(
                "Cannot access status until log_running has been called."
            )
        return self._status

    @property
    def command(self) -> str:
        """Returns the command this task instance will run."""
        if not hasattr(self, "_command"):
            raise AttributeError(
                "Cannot access command until log_running has been called."
            )
        return self._command

    @property
    def command_return_code(self) -> int:
        """Returns the exit code of the command that was run."""
        if not hasattr(self, "_proc"):
            raise AttributeError(
                "Cannot access command_return_code until command has run."
            )
        return self._proc.returncode

    @property
    def proc_stderr(self) -> str:
        """Returns the final 10k characters of the stderr from the command."""
        if not hasattr(self, "_proc"):
            raise AttributeError("Cannot access stderr until command has run.")
        return self._proc_stderr

    def configure_logging(self) -> None:
        """Setup logging for the worker node. INFO level goes to standard out."""
        _DEFAULT_LOG_FORMAT = (
            "%(asctime)s [%(name)-12s] %(module)s %(levelname)-8s: %(message)s"
        )
        logging_config: Dict = {
            "version": 1,
            "disable_existing_loggers": True,
            "formatters": {
                "default": {"format": _DEFAULT_LOG_FORMAT, "datefmt": "%Y-%m-%d %H:%M:%S"}
            },
            "handlers": {
                "default": {
                    "level": "INFO",
                    "class": "logging.StreamHandler",
                    "formatter": "default",
                    "stream": sys.stdout,
                },

            },
            "loggers": {
                "jobmon.worker_node": {
                    "handlers": ["default"],
                    "propagate": False,
                    "level": "INFO",
                },
            }
        }
        logging.config.dictConfig(logging_config)

    def log_done(self) -> None:
        """Tell the JobStateManager that this task_instance is done."""
        logger.info(f"Logging done for task_instance {self.task_instance_id}")
        message = {"nodename": self.nodename}
        if self.distributor_id is not None:
            message["distributor_id"] = str(self.distributor_id)
        else:
            logger.debug("No distributor id was found in the job submission env (e.g. sbatch, "
                         "qsub) at this time")

        app_route = f"/task_instance/{self.task_instance_id}/log_done"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message=message,
            request_type="post",
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {response}"
            )
        self._status = response["status"]
        if self.status != TaskInstanceStatus.DONE:
            raise TransitionError(
                f"TaskInstance {self.task_instance_id} failed because it could not transition "
                f"to {TaskInstanceStatus.DONE} status. Current status is {self.status}."
            )

    def log_error(self, error_state: str, msg: str) -> None:
        """Tell the JobStateManager that this task_instance has errored."""
        logger.info(f"Logging error for task_instance {self.task_instance_id}")

        message = {
            "error_message": msg,
            "error_state": error_state,
            "nodename": self.nodename,
        }
        if self.distributor_id is not None:
            message["distributor_id"] = str(self.distributor_id)
        else:
            logger.debug("No distributor_id was found in the sbatch env at this time")

        app_route = f"/task_instance/{self.task_instance_id}/log_error_worker_node"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message=message,
            request_type="post",
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {response}"
            )
        self._status = response["status"]
        if self.status != error_state:
            raise TransitionError(
                f"TaskInstance {self.task_instance_id} failed because it could not transition "
                f"to {error_state} status. Current status is {self.status}."
            )

    def log_running(self) -> None:
        """Tell the JobStateManager that this task_instance is running.

        Update the report_by_date to be further in the future in case it gets reconciled
        immediately.
        """
        logger.info(f"Log running for task_instance {self.task_instance_id}")
        message = {
            "nodename": self.nodename,
            "process_group_id": str(self.process_group_id),
            "next_report_increment": (self.heartbeat_interval * self.report_by_buffer),
            "stdout": str(self.stdout) if self.stdout is not None else None,
            "stderr": str(self.stderr) if self.stderr is not None else None
        }
        if self.distributor_id is not None:
            message["distributor_id"] = str(self.distributor_id)
        else:
            logger.info(
                "No distributor_id was found in the worker_node env at this time."
            )

        app_route = f"/task_instance/{self.task_instance_id}/log_running"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message=message,
            request_type="post",
        )

        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {response}"
            )

        kwargs = SerializeTaskInstance.kwargs_from_wire_worker_node(
            response["task_instance"]
        )
        self._task_instance_id = kwargs["task_instance_id"]
        self._status = kwargs["status"]
        self._command = kwargs["command"]
        self.last_heartbeat_time = time()

        if self.status != TaskInstanceStatus.RUNNING:
            raise TransitionError(
                f"TaskInstance {self.task_instance_id} failed because it could not transition "
                f"to {TaskInstanceStatus.RUNNING} status. Current status is {self.status}."
            )

    def log_report_by(self) -> None:
        """Log the heartbeat to show that the task instance is still alive."""
        logger.debug(f"Logging heartbeat for task_instance {self.task_instance_id}")
        message: Dict = {
            "next_report_increment": self.heartbeat_interval * self.report_by_buffer
        }
        if self.distributor_id is not None:
            message["distributor_id"] = str(self.distributor_id)
        else:
            logger.debug("No distributor_id was found in the sbatch env at this time")

        app_route = f"/task_instance/{self.task_instance_id}/log_report_by"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message=message,
            request_type="post",
        )

        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {response}"
            )
        self._status = response["status"]
        self.last_heartbeat_time = time()

        if self.status != TaskInstanceStatus.RUNNING:
            raise TransitionError(
                f"TaskInstance {self.task_instance_id} failed because it could not transition "
                f"to {TaskInstanceStatus.RUNNING} status. Current status is {self.status}."
            )

    def run(self) -> None:
        """This script executes on the target node and wraps the target application.

        Could be in any language, anything that can execute on linux. Similar to a stub or a
        container set ENV variables in case tasks need to access them.
        """
        # If it logs running and is not able to transition it raises TransitionError
        self.log_running()

        try:
            self._proc = subprocess.Popen(
                self.command,
                env=os.environ.copy(),
                stderr=subprocess.PIPE,
                stdout=sys.stdout,
                shell=True,
                universal_newlines=True,
            )

            # open thread for reading stderr eagerly otherwise process will deadlock if the
            # pipe fills up
            self._proc_stderr = ""
            self._err_q: Queue = Queue()  # queues for returning stderr to main thread
            err_thread = Thread(
                target=enqueue_stderr, args=(self._proc.stderr, self._err_q)
            )
            err_thread.daemon = True  # thread dies with the program
            err_thread.start()

            is_done = False
            while not is_done:
                # process any commands that we can in the time alotted
                time_till_next_heartbeat = self.heartbeat_interval - (
                    time() - self.last_heartbeat_time
                )
                is_done = self._poll_subprocess(timeout=time_till_next_heartbeat)

                # log report
                self.log_report_by()

        # some other deployment unit transitioned task instance out of R state
        except TransitionError as e:
            msg = (
                f"TaskInstance is in status '{self.status}'. Expected status 'R'."
                f" Terminating command {self.command}."
            )
            logger.error(msg)

            # cleanup process
            is_done = self._poll_subprocess(timeout=0)
            if not is_done:
                # interrupt command
                self._proc.send_signal(signal.SIGINT)

                # if it doesn't die of natural causes raise TimeoutExpired
                try:
                    self._proc.wait(timeout=self.command_interrupt_timeout)
                except subprocess.TimeoutExpired:
                    self._proc.kill()
                    self._proc.wait(timeout=self.command_interrupt_timeout)

                self._collect_stderr()
                logger.info(f"Collected stderr after termination: {self.proc_stderr}")

            # log an error with db if we are in K state
            if self.status == TaskInstanceStatus.KILL_SELF:
                msg = (
                    f"Command: {self.command} got KILL_SELF signal. Collected stderr after "
                    f"interrupt.\n{self.proc_stderr}"
                )
                error_state = TaskInstanceStatus.ERROR_FATAL

                self.log_error(error_state, msg)

            # otherwise raise the error cause we are in trouble
            else:
                raise e

        # normal happy path
        else:

            if self.command_return_code == ReturnCodes.OK:
                logger.info(f"Command: {self.command}. Finished Successfully.")
                self.log_done()
            else:
                logger.info(
                    f"Command: {self.command}\n Failed with stderr:\n {self.proc_stderr}"
                )
                error_state, msg = self.cluster_interface.get_exit_info(
                    self.command_return_code, self.proc_stderr
                )
                self.log_error(error_state, msg)

    def _poll_subprocess(self, timeout: Union[int, float] = -1) -> bool:
        """Poll subprocess until it is finished or timeout is reached.

        Args:
            timeout: time until we stop processing. -1 means process till no more work

        Returns: true if the  subprocess has exited
        """
        # this way we always process at least 1 command
        loop_start = time()

        ret_code = self._proc.poll()
        keep_polling = ret_code is None
        while keep_polling:
            sleep(0.5)

            # keep polling if we have time and no exit code
            ret_code = self._proc.poll()
            timeout_exceeded = (time() - loop_start) > timeout and not timeout == -1
            keep_polling = not timeout_exceeded and ret_code is None

            self._collect_stderr()

        return ret_code is not None

    def _collect_stderr(self) -> None:

        # pull stderr off queue and clip at 10k to avoid mysql has gone away errors
        # when posting long messages and keep memory low
        while not self._err_q.empty():
            self._proc_stderr += self._err_q.get()
        if len(self._proc_stderr) >= 10000:
            self._proc_stderr = self._proc_stderr[-10000:]


def enqueue_stderr(stderr: TextIOBase, queue: Queue) -> None:
    """Eagerly print 100 byte blocks to stderr so pipe doesn't fill up and deadlock.

    Also collect blocks for reporting to db by putting them in a queue to main thread.

    Args:
        stderr: stderr pipe
        queue: queue to communicate between listener thread and main thread
    """
    # read 100 bytes at a time so the pipe never deadlocks even if someone
    # tries to print a dataframe into stderr
    logger.debug("enqueue_stderr")
    block_reader = partial(stderr.read, 100)
    for new_block in iter(block_reader, ""):

        # push the block we just read to stderr and onto the queue that's
        # communicating w/ the main thread
        sys.stderr.write(new_block)
        queue.put(new_block)

    # cleanup
    stderr.close()
