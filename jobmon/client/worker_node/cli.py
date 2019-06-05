import argparse
import logging
import os
import subprocess
import sys
import traceback
from functools import partial
from queue import Queue
from threading import Thread
from time import sleep, time

from jobmon.client.swarm.job_management.job_instance_intercom import \
    JobInstanceIntercom
from jobmon.client.utils import kill_remote_process_group
from jobmon.exceptions import ReturnCodes

logger = logging.getLogger()

def enqueue_stderr(stderr, queue):
    """eagerly print 100 byte blocks to stderr so pipe doesn't fill up and
    deadlock. Also collect blocks for reporting to db by putting them in a
    queue to main thread

    Args:
        stderr: stderr pipe
        queue: queue to communicate between listener thread and main thread
    """

    # read 100 bytes at a time so the pipe never deadlocks even if someone
    # tries to print a dataframe into stderr
    block_reader = partial(stderr.read, 100)
    for new_block in iter(block_reader, ''):

        # push the block we just read to stderr and onto the queue that's
        # communicating w/ the main thread
        sys.stderr.write(new_block)
        queue.put(new_block)

    # cleanup
    stderr.close()


def is_on_prod() -> bool:
    """
    A wrapper to hide the dirty way we determine which cluster we are on
    Useful to have this be a function because it can be monkey patched in test.
    :return:  True if we are on prod
    """
    return "el6" in os.environ['SGE_ENV']


def unwrap():

    # This script executes on the target node and wraps the target application.
    # Could be in any language, anything that can execute on linux.
    # Similar to a stub or a container

    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--job_instance_id", required=True, type=int)
    parser.add_argument("--command", required=True)
    parser.add_argument("--jm_host", required=True)
    parser.add_argument("--jm_port", required=True)
    parser.add_argument("--executor_class", required=True)
    parser.add_argument("--temp_dir", required=False)
    parser.add_argument("--last_nodename", required=False)
    parser.add_argument("--last_pgid", required=False)
    parser.add_argument("--heartbeat_interval", default=90, type=int)
    parser.add_argument("--report_by_buffer", default=3.1, type=float)

    # makes a dict
    args = vars(parser.parse_args())

    # set ENV variables in case tasks need to access them
    os.environ["JOBMON_JOB_INSTANCE_ID"] = str(args["job_instance_id"])

    # identify executor class
    if args["executor_class"] == "SequentialExecutor":
        from jobmon.client.swarm.executors.sequential import \
            SequentialExecutor as ExecutorClass
    elif args["executor_class"] == "SGEExecutor":
        from jobmon.client.swarm.executors.sge import SGEExecutor \
            as ExecutorClass
    elif args["executor_class"] == "DummyExecutor":
        from jobmon.client.swarm.executors.dummy import DummyExecutor \
            as ExecutorClass
    else:
        raise ValueError("{} is not a valid ExecutorClass".format(
            args["executor_class"]))

    # Any subprocesses spawned will have this parent process's PID as
    # their PGID (useful for cleaning up processes in certain failure
    # scenarios)
    ji_intercom = JobInstanceIntercom(job_instance_id=args["job_instance_id"],
                                      executor_class=ExecutorClass,
                                      process_group_id=os.getpid(),
                                      hostname=args['jm_host'])
    ji_intercom.log_running(next_report_increment=(
        args['heartbeat_interval'] * args['report_by_buffer']),
        executor_id=os.environ.get('JOB_ID'))

    try:
        # SGE on prod & dev did not kill orphan processes following
        # an OOM kill, so if this is a retry job instance it must go
        # kill any remaining processes on the last node.
        # Not necessary on fair/buster.
        if args['last_nodename'] is not None and \
                args['last_pgid'] is not None and \
                is_on_prod():
            kill_remote_process_group(args['last_nodename'], args['last_pgid'])

        # open subprocess using a process group so any children are also killed
        proc = subprocess.Popen(
            args["command"],
            cwd=args["temp_dir"],
            env=os.environ.copy(),
            stderr=subprocess.PIPE,
            shell=True,
            universal_newlines=True)

        # open thread for reading stderr eagerly
        err_q = Queue()  # open queues for returning stderr to main thread
        err_thread = Thread(target=enqueue_stderr, args=(proc.stderr, err_q))
        err_thread.daemon = True  # thread dies with the program
        err_thread.start()

        last_heartbeat_time = time() - args['heartbeat_interval']
        while proc.poll() is None:
            if (time() - last_heartbeat_time) >= args['heartbeat_interval']:
                ji_intercom.log_report_by(next_report_increment=(
                    args['heartbeat_interval'] * args['report_by_buffer']),
                    executor_id=os.environ.get('JOB_ID'))
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

    # check return code
    if returncode != ReturnCodes.OK:
        if args["executor_class"] == "SGEExecutor":
            ji_intercom.log_job_stats()
        jid = os.environ.get('JOB_ID')
        logger.debug("jid:" + str(jid))
        ji_intercom.log_error(error_message=str(stderr),
                              executor_id=jid,
                              exit_status=returncode
                              )
    else:
        if args["executor_class"] == "SGEExecutor":
            ji_intercom.log_job_stats()
        ji_intercom.log_done(executor_id=os.environ.get('JOB_ID'))

    # If there's nothing wrong with the unwrapping itself we want to propagate
    # the return code from the subprocess onward for proper reporting
    sys.exit(returncode)
