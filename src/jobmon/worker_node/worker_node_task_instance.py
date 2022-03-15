"""The Task Instance Object once it has been submitted to run on a worker node."""
from functools import partial
from io import TextIOBase
import logging
import os
from queue import Queue
import signal
import socket
import subprocess
import sys
from threading import Thread
from time import sleep, time
from typing import Dict, Optional, Union

from jobmon.cluster import Cluster
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
        cluster_name: str,
        task_instance_id: Optional[int] = None,
        array_id: Optional[int] = None,
        batch_number: Optional[int] = None,
        heartbeat_interval: int = 90,
        report_by_buffer: float = 3.1,
        command_interrupt_timeout: int = 10,
        requester: Optional[Requester] = None,
    ) -> None:
        """A mechanism whereby a running task_instance can communicate back to the JSM.

         Logs its status, errors, usage details, etc.

        Args:
            task_instance_id: the id of the task_instance that is reporting back.
            cluster_name: the name of the cluster.
            array_id: If this is an array job, the corresponding array ID
            batch_number: If this is an array job, what is the batch?
            heartbeat_interval: how ofter to log a report by with the db
            report_by_buffer: multiplier for report by date in case we miss a few.
            requester: communicate with the flask services.
        """
        # identity attributes
        self._task_instance_id = task_instance_id
        self._array_id = array_id
        self._batch_number = batch_number

        # service API
        if requester is None:
            requester = Requester(WorkerNodeConfig.from_defaults().url)
        self.requester = requester

        # cluster API
        cluster = Cluster.get_cluster(cluster_name)
        self.cluster_worker_node = cluster.cluster_worker_node_class()

        # get distributor id from executor
        self._distributor_id = self.cluster_worker_node.distributor_id

        # get task_instance_id for array task
        if self._task_instance_id is None:
            if self._array_id is None:
                raise ValueError("Neither task_instance_id nor array_id were provided.")

                # Always assumed to be a value in the range [1, len(array)]
            array_step_id = self.cluster_worker_node.array_step_id

            # Fetch from the database
            app_route = (
                f"/get_array_task_instance_id/"
                f"{self._array_id}/{self._batch_number}/{array_step_id}"
            )
            rc, resp = self.requester.send_request(
                app_route=app_route, message={}, request_type="get"
            )
            if http_request_ok(rc) is False:
                raise InvalidResponse(
                    f"Unexpected status code {rc} from POST "
                    f"request through route {app_route}. Expected code "
                    f"200. Response content: {rc}"
                )
            self._task_instance_id = resp["task_instance_id"]

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
            raise AttributeError("Cannot access status until log_running has been called.")
        return self._status

    @property
    def command(self) -> str:
        """Returns the command this task instance will run."""
        if not hasattr(self, "_command"):
            raise AttributeError("Cannot access command until log_running has been called.")
        return self._command

    @property
    def command_return_code(self) -> int:
        """Returns a task instance ID if it's been bound."""
        if not hasattr(self, "_command_return_code"):
            raise AttributeError("Cannot access command_return_code until command has run.")
        return self._command_return_code

    def log_done(self) -> None:
        """Tell the JobStateManager that this task_instance is done."""
        logger.info(f"Logging done for task_instance {self.task_instance_id}")
        message = {"nodename": self.nodename}
        if self.distributor_id is not None:
            message["distributor_id"] = str(self.distributor_id)
        else:
            logger.debug("No executor id was found in the qsub env at this time")

        app_route = f"/task_instance/{self.task_instance_id}/log_done"
        return_code, response = self.requester.send_request(
            app_route=f"/task_instance/{self.task_instance_id}/log_done",
            message=message,
            request_type="post",
            logger=logger,
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

    def log_error(self, error_message: str, exit_status: int) -> None:
        """Tell the JobStateManager that this task_instance has errored."""
        logger.info(f"Logging error for task_instance {self.task_instance_id}")

        error_state, msg = self.cluster_worker_node.get_exit_info(exit_status, error_message)

        message = {
            "error_message": msg,
            "error_state": error_state,
            "nodename": self.nodename,
        }
        if self.distributor_id is not None:
            message["distributor_id"] = str(self.distributor_id)
        else:
            logger.debug("No distributor_id was found in the qsub env at this time")

        app_route = f"/task_instance/{self.task_instance_id}/log_error_worker_node"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message=message,
            request_type="post",
            logger=logger,
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
        }
        if self.distributor_id is not None:
            message["distributor_id"] = str(self.distributor_id)
        else:
            logger.info("No distributor_id was found in the worker_node env at this time.")

        app_route = f"/task_instance/{self.task_instance_id}/log_running"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message=message,
            request_type="post",
            logger=logger,
        )

        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {response}"
            )

        kwargs = SerializeTaskInstance.kwargs_from_wire_worker_node(response["task_instance"])
        self._task_instance_id = kwargs["task_instance_id"]
        self._status = kwargs["status"]
        self._command = kwargs["command"]
        self.last_heartbeat_time = time()

        if self.status != TaskInstanceStatus.RUNNING:
            raise TransitionError(
                f"TaskInstance {self.task_instance_id} failed because it could not transition "
                f"to {TaskInstanceStatus.RUNNING} status. Current status is {self.status}."
            )

    def log_report_by(self, next_report_increment: Union[int, float]) -> None:
        """Log the heartbeat to show that the task instance is still alive."""
        logger.debug(f"Logging heartbeat for task_instance {self.task_instance_id}")
        message: Dict = {"next_report_increment": next_report_increment}
        if self.distributor_id is not None:
            message["distributor_id"] = str(self.distributor_id)
        else:
            logger.debug("No distributor_id was found in the qsub env at this time")

        app_route = f"/task_instance/{self.task_instance_id}/log_report_by"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message=message,
            request_type="post",
            logger=logger,
        )

        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {response}"
            )
        self._status = response["status"]
        self.last_heartbeat_time = time()

    def run(self, temp_dir: Optional[str] = None) -> None:
        """This script executes on the target node and wraps the target application.

        Could be in any language, anything that can execute on linux. Similar to a stub or a
        container set ENV variables in case tasks need to access them.
        """

        # If it logs running and is not able to transition it the process will raise an error
        self.log_running()

        try:
            ret_code = None
            proc = subprocess.Popen(
                self.command,
                env=os.environ.copy(),
                stderr=subprocess.PIPE,
                shell=True,
                universal_newlines=True,
            )

            # open thread for reading stderr eagerly
            stderr = ""
            err_q: Queue = Queue()  # queues for returning stderr to main thread
            err_thread = Thread(target=enqueue_stderr, args=(proc.stderr, err_q))
            err_thread.daemon = True  # thread dies with the program
            err_thread.start()

            is_running = self.status == TaskInstanceStatus.RUNNING
            keep_monitoring = True
            while keep_monitoring:

                # check for a return code from the subprocess
                ret_code = proc.poll()

                if (time() - self.last_heartbeat_time) >= self.heartbeat_interval:
                    # this call returns the current db status and logs a heartbeat
                    self.log_report_by(
                        next_report_increment=(self.heartbeat_interval * self.report_by_buffer)
                    )
                    is_running = self.status == TaskInstanceStatus.RUNNING

                # command is still running and task instance hasn't been killed
                if ret_code is None and is_running:
                    # pull stderr off queue and clip at 10k to avoid mysql has gone away errors
                    # when posting long messages and keep memory low
                    while not err_q.empty():
                        stderr += err_q.get()
                    if len(stderr) >= 10000:
                        stderr = stderr[-10000:]

                    sleep(0.5)
                    keep_monitoring = True

                # command is still running but TaskInstance status changed underneath us
                elif ret_code is None and not is_running:
                    msg = (
                        f"TaskInstance is in status '{self.status}'. Expected status 'R'."
                        f" Terminating command {self.command}."
                    )
                    logger.info(msg)

                    # interrupt command
                    proc.send_signal(signal.SIGINT)

                    # if it doesn't die of natural causes raise TimeoutExpired
                    proc.wait(timeout=self.command_interrupt_timeout)

                    # collect stderr and log it
                    msg += "Collected stderr after termination: "
                    while not err_q.empty():
                        stderr += err_q.get()
                    allowed_err_len = 10000 - len(msg)
                    if len(stderr) >= allowed_err_len:
                        stderr = stderr[-allowed_err_len:]
                    msg += stderr
                    logger.info(msg)

                    # log error with db
                    self.log_error(error_message=msg, exit_status=proc.returncode)

                    # break out of loop
                    keep_monitoring = False

                # command returned an OK return code while in running state
                elif ret_code == ReturnCodes.OK and is_running:

                    logger.info(f"Command: {self.command}. Finished Successfully.")
                    self.log_done()

                    # break out of loop
                    keep_monitoring = False

                # got a non OK return code
                else:
                    while not err_q.empty():
                        stderr += err_q.get()
                    if len(stderr) >= 10000:
                        stderr = stderr[-10000:]
                    logger.info(f"Command: {self.command}\n Failed with stderr:\n {stderr}")

                    if is_running:
                        self.log_error(error_message=str(stderr), exit_status=proc.returncode)\

        except Exception as e:
            # if the process is still alive, kill it
            if proc.poll() is None:
                proc.kill()
                try:
                    _, stderr = proc.communicate(timeout=self.command_interrupt_timeout)
                except subprocess.TimeoutExpired:
                    pass

                logger.info(
                    f"Command: {self.command}\n was aborted due to an unexpected error {e}."
                    f"Got stderr from subprocess:\n {stderr}"
                )

            # re-raise the original exception
            raise e

        finally:
            self._command_return_code = proc.returncode


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
