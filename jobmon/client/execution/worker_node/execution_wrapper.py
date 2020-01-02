import argparse
from functools import partial
from io import TextIOBase
import pkg_resources
import os
from queue import Queue
import shlex
import signal
import subprocess
import sys
import traceback
from threading import Thread
from time import sleep, time
from typing import Optional

from jobmon.exceptions import ReturnCodes
from jobmon.client.execution.worker_node.worker_node_task_instance import (
    WorkerNodeTaskInstance)
from jobmon.client.execution.worker_node._logging import NodeLogging as logging

logger = logging.getLogger(__name__)


def enqueue_stderr(stderr: TextIOBase, queue: Queue) -> None:
    """eagerly print 100 byte blocks to stderr so pipe doesn't fill up and
    deadlock. Also collect blocks for reporting to db by putting them in a
    queue to main thread

    Args:
        stderr: stderr pipe
        queue: queue to communicate between listener thread and main thread
    """

    # read 100 bytes at a time so the pipe never deadlocks even if someone
    # tries to print a dataframe into stderr
    logger.info("enqueue_stderr")
    block_reader = partial(stderr.read, 100)
    for new_block in iter(block_reader, ''):

        # push the block we just read to stderr and onto the queue that's
        # communicating w/ the main thread
        sys.stderr.write(new_block)
        queue.put(new_block)

    # cleanup
    stderr.close()


def kill_self(child_process: subprocess.Popen = None):
    """If the worker has received a signal to kill itself, kill the child
    processes and then self, will show up as an exit code 299 in qacct"""
    logger.info("kill_self")
    if child_process:
        child_process.kill()
    sys.exit(signal.SIGKILL)


def parse_arguments(argstr=None):

    # parse arguments
    logger.info("parsing arguments")
    parser = argparse.ArgumentParser()
    parser.add_argument("--job_instance_id", required=True, type=int)
    parser.add_argument("--command", required=True)
    parser.add_argument("--expected_jobmon_version", required=True)
    parser.add_argument("--executor_class", required=True)
    parser.add_argument("--temp_dir", required=False)
    parser.add_argument("--last_nodename", required=False)
    parser.add_argument("--last_pgid", required=False)
    parser.add_argument("--heartbeat_interval", default=90, type=float)
    parser.add_argument("--report_by_buffer", default=3.1, type=float)

    if argstr is not None:
        arglist = shlex.split(argstr)
        args = parser.parse_args(arglist)
    else:
        args = parser.parse_args()

    return vars(args)


def unwrap(task_instance_id: int, command: str, expected_jobmon_version: str,
           executor_class: str, temp_dir: Optional[str] = None,
           last_nodename: str = None, last_pgid: str = None,
           heartbeat_interval: float = 90,
           report_by_buffer: float = 3.1):

    # This script executes on the target node and wraps the target application.
    # Could be in any language, anything that can execute on linux.
    # Similar to a stub or a container

    # set ENV variables in case tasks need to access them
    os.environ["JOBMON_JOB_INSTANCE_ID"] = str(task_instance_id)

    # identify executor class
    if executor_class == "SequentialExecutor":
        from jobmon.client.execution.strategies.sequential import \
            TaskInstanceSequentialInfo as TaskInstanceExecutorInfo
    elif executor_class == "SGEExecutor":
        from jobmon.client.execution.strategies.sge import \
            TaskInstanceSGEInfo as TaskInstanceExecutorInfo
    elif executor_class == "DummyExecutor":
        from jobmon.client.execution.strategies.base import \
            TaskInstanceExecutorInfo
    else:
        raise ValueError("{} is not a valid ExecutorClass".format(
            executor_class))
    ti_executor_info = TaskInstanceExecutorInfo()

    # Any subprocesses spawned will have this parent process's PID as
    # their PGID (useful for cleaning up processes in certain failure
    # scenarios)
    version = pkg_resources.get_distribution("jobmon").version
    if version != expected_jobmon_version:
        msg = f"Your workflow master node is using, " \
            f"{expected_jobmon_version} and your worker node is using" \
            f" {version}. Please check your bash profile "
        logger.error(msg)
        sys.exit(ReturnCodes.WORKER_NODE_ENV_FAILURE)

    worker_node_task_instance = WorkerNodeTaskInstance(
        task_instance_id=task_instance_id,
        task_instance_executor_info=ti_executor_info)

    # if it logs running and is in the 'W' or 'U' state then it will go
    # through the full process of trying to change states and receive a
    # special exception to signal that it can't run and should kill itself
    rc, kill = worker_node_task_instance.log_running(next_report_increment=(
        heartbeat_interval * report_by_buffer))
    if kill == 'True':
        kill_self()

    try:
        # open subprocess using a process group so any children are also killed
        proc = subprocess.Popen(
            command,
            cwd=temp_dir,
            env=os.environ.copy(),
            stderr=subprocess.PIPE,
            shell=True,
            universal_newlines=True)

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
                        next_report_increment=(heartbeat_interval *
                                               report_by_buffer))

                last_heartbeat_time = time()
            sleep(0.5)  # don't thrash CPU by polling as fast as possible

        # compile stderr to send to db
        stderr = ""
        while not err_q.empty():
            stderr += err_q.get()

        returncode = proc.returncode

    except Exception as exc:
        stderr = "{}: {}\n{}".format(type(exc).__name__, exc,
                                     traceback.format_exc())
        logger.warning(stderr)
        returncode = ReturnCodes.WORKER_NODE_CLI_FAILURE

    # post stats usage. this is a non critical error so it catches all
    # exceptions in the method
    worker_node_task_instance.log_task_stats()

    # check return code
    if returncode != ReturnCodes.OK:
        worker_node_task_instance.log_error(error_message=str(stderr),
                                            exit_status=returncode)
    else:
        worker_node_task_instance.log_done()

    return returncode


def main():
    kwargs = parse_arguments()
    returncode = unwrap(**kwargs)

    # If there's nothing wrong with the unwrapping itself we want to propagate
    # the return code from the subprocess onward for proper reporting
    sys.exit(returncode)


if __name__ == '__main__':
    main()
