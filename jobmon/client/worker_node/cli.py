from __future__ import print_function

import argparse
import os
import sys
import traceback

import jsonpickle

from jobmon.exceptions import ReturnCodes
from jobmon.client.swarm.job_management.job_instance_intercom import \
    JobInstanceIntercom
from jobmon.client.utils import kill_remote_process_group

if sys.version_info > (3, 0):
    import subprocess
else:
    import subprocess32 as subprocess


def unwrap():

    # This script executes on the target node and wraps the target application.
    # Could be in any language, anything that can execute on linux.
    # Similar to a stub or a container

    def eprint(*args, **kwargs):
        """Utility function for displaying stderr captured from subprocess
        in stderr stream of parent process"""
        print(*args, file=sys.stderr, **kwargs)

    def jpickle_parser(s):
        return jsonpickle.decode(s)

    def intnone_parser(s):
        try:
            return int(s)
        except ValueError:
            return None

    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--job_instance_id", required=True, type=int)
    parser.add_argument("--command", required=True)
    parser.add_argument("--jsm_host", required=True)
    parser.add_argument("--jsm_port", required=True)
    parser.add_argument("--executor_class", required=True)
    parser.add_argument("--temp_dir", required=False)
    parser.add_argument("--last_nodename", required=False)
    parser.add_argument("--last_pgid", required=False)

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
                                      hostname=args['jsm_host'])
    ji_intercom.log_running()

    try:
        if 'last_nodename' in args and 'last_pgid' in args:
            kill_remote_process_group(args['last_nodename'], args['last_pgid'])

        if 'temp_dir' not in args:
            args['temp_idr'] = None

        # open subprocess using a process group so any children are also killed
        proc = subprocess.Popen(
            args["command"],
            cwd=args["temp_dir"],
            env=os.environ.copy(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True)

        # communicate till done
        stdout, stderr = proc.communicate()
        returncode = proc.returncode

    except Exception as exc:
        stdout = ""
        stderr = "{}: {}\n{}".format(type(exc).__name__, exc,
                                     traceback.format_exc())
        returncode = None

    print(stdout)
    eprint(stderr)

    # check return code
    if returncode != ReturnCodes.OK:
        ji_intercom.log_job_stats()
        ji_intercom.log_error(str(stderr))
    else:
        ji_intercom.log_job_stats()
        ji_intercom.log_done()
