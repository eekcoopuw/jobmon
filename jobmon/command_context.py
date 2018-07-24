from __future__ import print_function

import logging
import json
import os
import sys
import traceback

import jsonpickle

from cluster_utils.io import makedirs_safely

from jobmon.config import config
from jobmon.exceptions import ReturnCodes
from jobmon.job_instance_intercom import JobInstanceIntercom
from jobmon.utils import kill_remote_process_group

if sys.version_info > (3, 0):
    import subprocess
    from subprocess import TimeoutExpired
else:
    import subprocess32 as subprocess
    from subprocess32 import TimeoutExpired


logger = logging.getLogger(__name__)


def build_qsub(job, job_instance_id, stderr=None, stdout=None, project=None,
               working_dir=None):
    """Process the Job's context_args, which are assumed to be
    a json-serialized dictionary"""
    # TODO: Settle on a sensible way to pass and validate settings for the
    # command's context (i.e. context = Executor, SGE/Sequential/Multiproc)

    ctx_args = json.loads(job.context_args)
    if 'sge_add_args' in ctx_args:
        sge_add_args = ctx_args['sge_add_args']
    else:
        sge_add_args = ""
    if project:
        project_cmd = "-P {}".format(project)
    else:
        project_cmd = ""
    if stderr:
        stderr_cmd = "-e {}".format(stderr)
        makedirs_safely(stderr)
    else:
        stderr_cmd = ""
    if stdout:
        stdout_cmd = "-o {}".format(stdout)
        makedirs_safely(stdout)
    else:
        stdout_cmd = ""
    if working_dir:
        wd_cmd = "-wd {}".format(working_dir)
    else:
        wd_cmd = ""
    cmd = build_wrapped_command(job, job_instance_id)
    thispath = os.path.dirname(os.path.abspath(__file__))

    # NOTE: The -V or equivalent is critical here to propagate the value of the
    # JOBMON_CONFIG environment variable to downstream Jobs... otherwise those
    # Jobs could end up using a different config and not be able to talk back
    # to the appropriate server(s)
    qsub_cmd = ('qsub {wd} -N {jn} '
                '-pe multi_slot {slots} -l mem_free={mem}g '
                '{project} {stderr} {stdout} '
                '{sge_add_args} '
                '-V {path}/submit_master.sh '
                '"{cmd}"'.format(
                    wd=wd_cmd,
                    jn=job.name,
                    slots=job.slots,
                    mem=job.mem_free,
                    sge_add_args=sge_add_args,
                    path=thispath,
                    cmd=cmd,
                    project=project_cmd,
                    stderr=stderr_cmd,
                    stdout=stdout_cmd))
    return qsub_cmd


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
    jobmon_command = config.default_opts.get('jobmon_command', None)
    if not jobmon_command:
        jobmon_command = subprocess.check_output(["which", "jobmon_command"])
    jobmon_command = jobmon_command.strip().decode("utf-8")
    wrapped_cmd = [
        jobmon_command,
        "--command", "'{}'".format(job.command),
        "--job_instance_id", job_instance_id,
        "--jsm_host", config.jsm_conn.host,
        "--jsm_port", config.jsm_conn.port
    ]
    if job.last_nodename:
        wrapped_cmd.extend(["--last_nodename", job.last_nodename])
    if job.last_process_group_id:
        wrapped_cmd.extend(["--last_pgid", job.last_process_group_id])
    if process_timeout is not None:
        wrapped_cmd += ["--process_timeout", process_timeout]
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
    parser.add_argument("--last_nodename", required=False)
    parser.add_argument("--last_pgid", required=False)
    parser.add_argument("--process_timeout", required=False,
                        type=intnone_parser)

    # makes a dict
    args = vars(parser.parse_args())

    # Any subprocesses spawned will have this parent process's PID as
    # their PGID (useful for cleaning up processes in certain failure
    # scenarios)
    ji_intercom = JobInstanceIntercom(job_instance_id=args["job_instance_id"],
                                      process_group_id=os.getpid(),
                                      hostname=args['jsm_host'])
    ji_intercom.log_running()

    try:
        if 'last_nodename' in args and 'last_pgid' in args:
            kill_remote_process_group(args['last_nodename'], args['last_pgid'])

        # open subprocess using a process group so any children are also killed
        proc = subprocess.Popen(
            args["command"],
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

    sge_id = os.environ.get('JOB_ID')
    if not sge_id:  # This allows sequentially executed (not SGE) jobs to work
        logger.debug("No job_id env variable set. Can't log job stats")

    # check return code
    if returncode != ReturnCodes.OK:
        ji_intercom.log_job_stats(sge_id)
        ji_intercom.log_error(str(stderr))
    else:
        ji_intercom.log_job_stats(sge_id)
        ji_intercom.log_done()
