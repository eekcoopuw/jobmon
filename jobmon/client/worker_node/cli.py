import argparse

import os
import subprocess
import sys
import traceback
from time import sleep, time


from jobmon.exceptions import ReturnCodes
from jobmon.client.swarm.job_management.job_instance_intercom import \
    JobInstanceIntercom
from jobmon.client.utils import kill_remote_process_group

# TODO: Redesign the scope of the try-catch blocks (return code 199)


def unwrap():

    # This script executes on the target node and wraps the target application.
    # Could be in any language, anything that can execute on linux.
    # Similar to a stub or a container

    def eprint(*args, **kwargs):
        """Utility function for displaying stderr captured from subprocess
        in stderr stream of parent process
        """
        print(*args, file=sys.stderr, **kwargs)

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
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
            universal_newlines=True)

        last_heartbeat_time = time() - args['heartbeat_interval']
        while proc.poll() is None:
            if (time() - last_heartbeat_time) >= args['heartbeat_interval']:
                ji_intercom.log_report_by(next_report_increment=(
                    args['heartbeat_interval'] * args['report_by_buffer']))
                last_heartbeat_time = time()
            sleep(0.5)

        # communicate the stdout and stderr
        stdout, stderr = proc.communicate()
        returncode = proc.returncode

    except Exception as exc:
        stdout = ""
        stderr = "{}: {}\n{}".format(type(exc).__name__, exc,
                                     traceback.format_exc())
        returncode = ReturnCodes.WORKER_NODE_CLI_FAILURE

    print(stdout)
    eprint(stderr)

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
