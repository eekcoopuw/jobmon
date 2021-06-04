"""The Task Instance Object once it has been submitted to run on a worker node."""
import argparse
import os
import socket
import shlex
import signal
import subprocess
import sys
import traceback
from functools import partial
from io import TextIOBase
from queue import Queue
from threading import Thread
from time import sleep, time
from typing import Dict, Optional, Tuple, Union

from jobmon.client.client_config import ClientConfig
from jobmon.cluster_type.api import register_cluster_plugin, import_cluster
from jobmon.cluster_type.base import ClusterWorkerNode
from jobmon.exceptions import InvalidResponse, ReturnCodes
from jobmon.requester import Requester, http_request_ok

import structlog as logging

from jobmon.serializers import SerializeClusterType

import pkg_resources

logger = logging.getLogger(__name__)


class WorkerNodeTaskInstance:
    """The Task Instance object once it has been submitted to run on a worker node."""

    def __init__(self, task_instance_id: int,
                 cluster_type_name: str,
                 requester_url: Optional[str] = None):
        """The WorkerNodeTaskInstance is a mechanism whereby a running task_instance can
        communicate back to the JobStateManager to log its status, errors, usage details, etc.

        Args:
            task_instance_id (int): the id of the job_instance_id that is
                reporting back
            task_instance_executor_info (TaskInstanceExecutorInfo): instance of
                executor that was used for this job instance
            nodename (str): hostname where this job_instance is running
            process_group_id (int): linux process_group_id that this
                job_instance is a part of
            requester_url (str): url to communicate with the flask services.
        """
        self.task_instance_id = task_instance_id
        self._executor_id: Optional[int] = None
        self._nodename: Optional[str] = None
        self._process_group_id: Optional[int] = None

        if requester_url is None:
            requester_url = ClientConfig.from_defaults().url
        self.requester = Requester(requester_url)

        self.executor = self._get_worker_node(cluster_type_name)

    def _get_worker_node(self, cluster_type_name: str, **worker_node_kwargs) -> ClusterWorkerNode:

        """Lookup ClusterType, getting package_location back."""
        app_route = f'/client/cluster_type/{cluster_type_name}'
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={},
            request_type="get",
            logger=logger
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from POST '
                f'request through route {app_route}. Expected code '
                f'200. Response content: {response}'
            )
        cluster_type_kwargs = SerializeClusterType.kwargs_from_wire(response["cluster_type"])

        register_cluster_plugin(cluster_type_name, cluster_type_kwargs["package_location"])

        module = import_cluster(cluster_type_name)

        WorkerNode = module.get_cluster_worker_node_class()
        return WorkerNode(**worker_node_kwargs)

    @property
    def executor_id(self) -> Optional[int]:
        """Executor id given from the executor it is being run on."""
        if self._executor_id is None and self.executor.executor_id is not None:
            self._executor_id = self.executor.executor_id
        logger.debug("executor_id: " + str(self._executor_id))
        return self._executor_id

    @property
    def nodename(self) -> Optional[str]:
        """Node it is being run on."""
        if self._nodename is None:
            self._nodename = socket.getfqdn()
        return self._nodename

    @property
    def process_group_id(self) -> Optional[int]:
        """Process group to track parent and child processes."""
        if self._process_group_id is None:
            self._process_group_id = os.getpid()
        return self._process_group_id

    def log_done(self) -> int:
        """Tell the JobStateManager that this task_instance is done."""
        logger.info(f"Logging done for task_instance {self.task_instance_id}")
        message = {'nodename': self.nodename}
        if self.executor_id is not None:
            message['executor_id'] = str(self.executor_id)
        else:
            logger.debug("No executor id was found in the qsub env at this time")
        rc, _ = self.requester.send_request(
            app_route=f'/worker/task_instance/{self.task_instance_id}/log_done',
            message=message,
            request_type='post',
            logger=logger
        )
        return rc

    def log_error(self, error_message: str, exit_status: int) -> int:
        """Tell the JobStateManager that this task_instance has errored."""
        logger.info(f"Logging error for task_instance {self.task_instance_id}")

        # clip at 10k to avoid mysql has gone away errors when posting long
        # messages
        e_len = len(error_message)
        if e_len >= 10000:
            error_message = error_message[-10000:]
            logger.info(f"Error_message is {e_len} which is more than the 10k "
                        "character limit for error messages. Only the final "
                        "10k will be captured by the database.")

        error_state, msg = self.executor.get_exit_info(exit_status, error_message)

        message = {'error_message': msg,
                   'error_state': error_state,
                   'nodename': self.nodename}

        if self.executor_id is not None:
            message['executor_id'] = str(self.executor_id)
        else:
            logger.debug("No executor_id was found in the qsub env at this time")
        rc, _ = self.requester.send_request(
            app_route=f'/worker/task_instance/{self.task_instance_id}/log_error_worker_node',
            message=message,
            request_type='post',
            logger=logger
        )
        return rc

    def log_task_stats(self) -> None:
        """Tell the JobStateManager all the applicable task_stats for this task_instance."""
        logger.info(f"Logging usage for task_instance {self.task_instance_id}")
        try:
            usage = self.executor.get_usage_stats()
            dbukeys = ['usage_str', 'wallclock', 'maxrss', 'maxpss', 'cpu',
                       'io']
            msg = {k: usage[k] for k in dbukeys if k in usage.keys()}
            rc, _ = self.requester.send_request(
                app_route=f'/worker/task_instance/{self.task_instance_id}/log_usage',
                message=msg,
                request_type='post',
                logger=logger
            )
        except NotImplementedError:
            logger.warning(f"Usage stats not available for "
                           f"{self.executor.__class__.__name__} executors")
        except Exception as e:
            # subprocess.CalledProcessError is raised if qstat fails.
            # Not a critical error, keep running and log an error.
            logger.error(f"Usage stats not available due to exception {e}")
            logger.error(f"Traceback {traceback.format_exc()}")

    def log_running(self, next_report_increment: Union[int, float]) -> Tuple[int, str]:
        """Tell the JobStateManager that this task_instance is running, and update the
        report_by_date to be further in the future in case it gets reconciled immediately.
        """
        logger.info(f'Log running for task_instance {self.task_instance_id}')
        message = {'nodename': self.nodename,
                   'process_group_id': str(self.process_group_id),
                   'next_report_increment': next_report_increment}
        if self.executor_id is not None:
            message['executor_id'] = str(self.executor_id)
        else:
            logger.info("No Task ID was found in the qsub env at this time")
        rc, resp = self.requester.send_request(
            app_route=(f'/worker/task_instance/{self.task_instance_id}/log_running'),
            message=message,
            request_type='post',
            logger=logger
        )
        logger.debug(f"Response from log_running was: {resp}")
        return rc, resp["message"], resp["command"]

    def log_report_by(self, next_report_increment: Union[int, float]) -> int:
        """Log the heartbeat to show that the task instance is still alive."""
        logger.debug(f"Logging heartbeat for task_instance {self.task_instance_id}")
        message: Dict = {"next_report_increment": next_report_increment}
        if self.executor_id is not None:
            message['executor_id'] = str(self.executor_id)
        else:
            logger.debug("No executor_id was found in the qsub env at this time")
        rc, _ = self.requester.send_request(
            app_route=f'/worker/task_instance/{self.task_instance_id}/log_report_by',
            message=message,
            request_type='post',
            logger=logger
        )
        return rc

    def in_kill_self_state(self) -> bool:
        """Check if the task instance has been set to kill itself (upon resume or other error
        from miscommunication).
        """
        logger.debug(f"checking kill_self for task_instance {self.task_instance_id}")
        rc, resp = self.requester.send_request(
            app_route=f'/worker/task_instance/{self.task_instance_id}/kill_self',
            message={},
            request_type='get',
            logger=logger
        )
        if resp.get('should_kill'):
            logger.debug("task_instance is in a state that indicates it needs to kill itself")
            return True
        else:
            logger.debug("task instance does not need to kill itself")
            return False

    def run(self, expected_jobmon_version: str,
               temp_dir: Optional[str] = None,
               heartbeat_interval: float = 90, report_by_buffer: float = 3.1) -> ReturnCodes:
        """This script executes on the target node and wraps the target application. Could be in
        any language, anything that can execute on linux.Similar to a stub or a container set ENV
        variables in case tasks need to access them.
        """
        os.environ["JOBMON_JOB_INSTANCE_ID"] = str(self.task_instance_id)

        version = pkg_resources.get_distribution("jobmon").version
        if version != expected_jobmon_version:
            msg = (f"Your expected Jobmon version is {expected_jobmon_version} and your "
                   f"worker node is using {version}. Please check your bash profile ")
            logger.error(msg)
            sys.exit(ReturnCodes.WORKER_NODE_ENV_FAILURE)

        # if it logs running and is in the 'W' or 'U' state then it will go
        # through the full process of trying to change states and receive a
        # special exception to signal that it can't run and should kill itself
        rc, kill, command = self.log_running(next_report_increment=(
            heartbeat_interval * report_by_buffer))
        if kill == 'True': #TODO: possibly incorrect; check to see if it should be 'kill self'.
            kill_self()

        try:
            err_q, returncode = _run_in_sub_process(command, temp_dir, heartbeat_interval,
                                                    self, report_by_buffer)

            # compile stderr to send to db
            stderr = ""
            while not err_q.empty():
                stderr += err_q.get()

        except Exception as exc:
            stderr = "{}: {}\n{}".format(type(exc).__name__, exc, traceback.format_exc())
            logger.warning(stderr)
            returncode = ReturnCodes.WORKER_NODE_CLI_FAILURE

        # post stats usage. this is a non critical error so it catches all
        # exceptions in the method
        self.log_task_stats()

        # check return code
        if returncode != ReturnCodes.OK:
            self.log_error(error_message=str(stderr), exit_status=returncode)
        else:
            self.log_done()

        return returncode


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
    for new_block in iter(block_reader, ''):

        # push the block we just read to stderr and onto the queue that's
        # communicating w/ the main thread
        sys.stderr.write(new_block)
        queue.put(new_block)

    # cleanup
    stderr.close()


def kill_self(child_process: subprocess.Popen = None) -> None:
    """If the worker has received a signal to kill itself, kill the child processes and then
    self, will show up as an exit code 299 in qacct.
    """
    logger.info("kill self message received")
    if child_process:
        child_process.kill()
    sys.exit(signal.SIGKILL)


def parse_arguments(argstr: Optional[str] = None) -> dict:
    """Parse out the components of the command sent to the node."""
    # parse arguments
    logger.debug("parsing arguments")
    parser = argparse.ArgumentParser()
    parser.add_argument("--task_instance_id", required=True, type=int)
    parser.add_argument("--expected_jobmon_version", required=True)
    parser.add_argument("--cluster_type_name", required=True)
    parser.add_argument("--temp_dir", required=False)
    parser.add_argument("--heartbeat_interval", default=90, type=float)
    parser.add_argument("--report_by_buffer", default=3.1, type=float)

    if argstr is not None:
        arglist = shlex.split(argstr)
        args = parser.parse_args(arglist)
    else:
        args = parser.parse_args()

    return vars(args)


def _run_in_sub_process(command: str, temp_dir: Optional[str], heartbeat_interval: float,
                        worker_node_task_instance: WorkerNodeTaskInstance,
                        report_by_buffer: float):
    """Move out of unwrap for easy mock."""
    proc = subprocess.Popen(
        command,
        cwd=temp_dir,
        env=os.environ.copy(),
        stderr=subprocess.PIPE,
        shell=True,
        universal_newlines=True
    )
    # open thread for reading stderr eagerly
    err_q: Queue = Queue()  # queues for returning stderr to main thread
    err_thread = Thread(target=enqueue_stderr, args=(proc.stderr, err_q))
    err_thread.daemon = True  # thread dies with the program
    err_thread.start()

    last_heartbeat_time = time() - heartbeat_interval
    while proc.poll() is None:
        if (time() - last_heartbeat_time) >= heartbeat_interval:

            # since the report by is not a state transition, it will not
            #  get the error that the log running route gets, so just
            # check the database and kill if its status means it should
            #  be killed
            if worker_node_task_instance.in_kill_self_state():
                kill_self(child_process=proc)
            else:
                worker_node_task_instance.log_report_by(
                    next_report_increment=(heartbeat_interval * report_by_buffer)
                )

            last_heartbeat_time = time()
        sleep(0.5)  # don't thrash CPU by polling as fast as possible
    return err_q, proc.returncode

