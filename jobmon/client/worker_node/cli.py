import argparse
from functools import partial
import os
import subprocess
import sys
from threading import Thread
import traceback
from time import sleep, time
from queue import Queue

from jobmon.exceptions import ReturnCodes
from jobmon.client.swarm.job_management.job_instance_intercom import \
    JobInstanceIntercom
from jobmon.client.utils import kill_remote_process_group


def enqueue_stderr(stderr, queue):
    """reader that listens for new lines in a pipe and prints them. Will
    terminate when pipe closes. puts each resulting line into a queue

    Args:
        stderr: stderr pipe
    """
    # eagerly print each line to stderr so the pipe doesn't deadlock but also
    # collect for later reportin to DB
    line_accum = ""

    # read 100 bytes at a time so the pipe never deadlocks even if someone
    # tries to print a dataframe into stderr
    block_reader = partial(stderr.read, 100)
    for new_block in iter(block_reader, ''):

        # concat the block we just read onto our line accumlator
        line_accum = line_accum + new_block
        lines = line_accum.splitlines()
        if len(lines) > 1:

            # if we have multiple lines, eagerly print all to stderr except the
            # last one because it should be incomplete
            for line in lines[:-1]:
                print(line, file=sys.stderr)
                queue.put(line)
            line_accum = lines[-1]

    # print the final line block to stderr
    print(line_accum, file=sys.stderr)
    stderr.close()

    # return full to main thread
    queue.put(line_accum)


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
        args['heartbeat_interval'] * args['report_by_buffer']))

    try:
        if args['last_nodename'] is not None and args['last_pgid'] is not None:
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
                    args['heartbeat_interval'] * args['report_by_buffer']))
                last_heartbeat_time = time()
            sleep(0.5)  # don't thrash CPU by polling as fast as possible

        # compile stderr to send to db
        err_lines = []
        while not err_q.empty():
            err_lines.append(err_q.get())
        stderr = "\n".join(err_lines)

        returncode = proc.returncode

    except Exception as exc:
        stderr = "{}: {}\n{}".format(type(exc).__name__, exc,
                                     traceback.format_exc())
        returncode = ReturnCodes.WORKER_NODE_CLI_FAILURE

    # check return code
    if returncode != ReturnCodes.OK:
        if args["executor_class"] == "SGEExecutor":
            ji_intercom.log_job_stats()
        ji_intercom.log_error(str(stderr))
    else:
        if args["executor_class"] == "SGEExecutor":
            ji_intercom.log_job_stats()
        ji_intercom.log_done()

    # If there's nothing wrong with the unwrapping itself we want to propagate
    # the return code from the subprocess onward for proper reporting
    sys.exit(returncode)
