from __future__ import print_function

import logging
import os
import signal
import sys
import traceback

import jsonpickle

from jobmon.config import config
from jobmon.connection_config import ConnectionConfig
from jobmon.job_instance_intercom import JobInstanceIntercom
from jobmon.exceptions import ReturnCodes

if sys.version_info > (3, 0):
    import subprocess
    from subprocess import TimeoutExpired
else:
    import subprocess32 as subprocess
    from subprocess32 import TimeoutExpired


logger = logging.getLogger(__name__)


def build_wrapped_command(job, job_instance_id, process_timeout=None):
    """submit jobs to sge scheduler using sge.qsub. They will automatically
    register with server and sqlite database.

    Args:
        job (job.Job): the job to be run
        job_instance_id (int): the id of the job_instance to be run
        process_timeout (int, optional): how many seconds to wait for a job
            to finish before killing it and registering a failure. Default
            is forever.

    Returns:
        sge job id
    """
    wrapped_cmd = [
        "jobmon_command",
        "--command", "'{}'".format(job.command),
        "--job_instance_id", job_instance_id,
        "--jsm_host", config.jm_rep_conn.host,
        "--jsm_port", config.jm_rep_conn.port,
        "--process_timeout", process_timeout
    ]
    wrapped_cmd = " ".join([str(i) for i in wrapped_cmd])
    logger.debug(wrapped_cmd)
    return wrapped_cmd


def unwrap():

    import argparse

    # This script executes on the target node and wraps the target application.
    # Could be in any language, anything that can execute on linux.
    # Similar to a stub or a container

    # for sge logging of standard error
    def eprint(*args, **kwargs):
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
    parser.add_argument("--process_timeout", required=False,
                        type=intnone_parser)

    # makes a dict
    args = vars(parser.parse_args())

    cc = ConnectionConfig(args["jsm_host"], args["jsm_port"])
    ji_intercom = JobInstanceIntercom(job_instance_id=args["job_instance_id"],
                                      jm_rep_cc=cc)

    ji_intercom.log_running()

    try:
        # open subprocess using a process group so any children are also killed
        proc = subprocess.Popen(
            args["command"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            preexec_fn=os.setsid,
            shell=True)

        # communicate till done
        stdout, stderr = proc.communicate(timeout=args["process_timeout"])
        returncode = proc.returncode

    except TimeoutExpired:
        # kill process group
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        stdout, stderr = proc.communicate()
        stderr = stderr + " Process timed out after: {}".format(
            args["process_timeout"])
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
