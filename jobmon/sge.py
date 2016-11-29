"""
Interface to the dynamic resource manager (DRM), aka the scheduler.
"""
import atexit
import collections.abc
from datetime import datetime, time
import itertools
import logging
import os
import re
import subprocess
import types
import pandas as pd
import numpy as np
import drmaa

this_path = os.path.dirname(os.path.abspath(__file__))


logger = logging.getLogger(__name__)
DRMAA_PATH = "/usr/local/UGE-{}/lib/lx-amd64/libdrmaa.so.1.0"
# Comes from object_name in `man sge_types`. Also, * excluded.
UGE_NAME_POLICY = re.compile("^[.#\n\t\r /\\\[\]:´{}|\(\)@%,*]|"
                             "[\n\t\r /\\\[\]:´{}|\(\)@%,*]")


def _drmaa_session():
    """
    Get the global DRMAA session or initialize it if this is the first call.
    Can set this by setting DRMAA_LIBRARY_PATH. This should be the complete
    path the the library with .so at the end.
    """
    if "session" not in vars(_drmaa_session):
        if "DRMAA_LIBRARY_PATH" not in os.environ:
            os.environ["DRMAA_LIBRARY_PATH"] = DRMAA_PATH.format(
                os.environ["SGE_CLUSTER_NAME"])
        session = drmaa.Session()
        session.initialize()
        atexit.register(_drmaa_exit)
        _drmaa_session.session = session
    return _drmaa_session.session


def _drmaa_exit():
    _drmaa_session.session.exit()


#TODO Should this be two separate functions?
def true_path(file_or_dir=None, executable=None):
    """Get true path to file or executable.
    Args:
        :param file_or_dir partial file path, to be expanded
               as per the current user
        :param executable  the name of an executable, which
               will be resolved using "which"

        Specify one of the two arguments, not both.
    """
    if file_or_dir is not None:
        f = file_or_dir
    elif executable is not None:
        f = subprocess.check_output(["which", executable])
    else:
        raise ValueError("true_path: file_or_dir and executable "
                         "cannot both be null")

    # Be careful, in python 3 check_output returns bytes
    if not isinstance(f, str):
        f = f.decode('utf-8')
    f = os.path.abspath(os.path.expanduser(f))
    return f.strip(' \t\r\n')


def qstat(status=None, pattern=None, user=None, jids=None):
    """parse sge qstat information into DataFrame

    Args:
        status (string, optional): status filter to use when running qstat
            command
        pattern (string, optional): pattern filter to use when running qstat
            command
        user (string, optional): user filter to use when running qstat command
        jids (list, optional): list of job ids to use when running qstat
            command

    Returns:
        DataFrame of qstat return values
    """
    cmd = ["qstat", "-r"]
    if status is not None:
        cmd.extend(["-s", status])
    if user is not None:
        cmd.extend(["-u", user])

    p1 = subprocess.Popen(
        " ".join(cmd),
        stdout=subprocess.PIPE,
        shell=True)
    p2 = subprocess.Popen(
        ["grep", "Full jobname:", "-B1"],
        stdin=p1.stdout,
        stdout=subprocess.PIPE)

    p1.stdout.close()
    output, err = p2.communicate()
    p2.stdout.close()

    logger.debug("qstat OUTPUT ={}=".format(output))

    # Careful, python 2 vs 3 - bytes versus strings
    if not isinstance(output, str):
        output=output.decode('utf-8')

    lines = output.splitlines()

    job_ids = []
    job_users = []
    job_names = []
    job_slots = []
    job_statuses = []
    job_datetimes = []
    job_runtimes = []
    append_jobid = True
    append_jobname = False
    time_format = "%m/%d/%Y %H:%M:%S"
    now = datetime.now()
    for line in lines:

        if append_jobid is True:
            job_ids.append(line.split()[0])
            job_users.append(line.split()[3])
            job_statuses.append(line.split()[4])
            job_date = line.split()[5]
            job_time = line.split()[6]
            try:
                job_slots.append(int(line.split()[8]))
            except:
                job_slots.append(int(line.split()[7]))
            job_datetime = datetime.strptime(
                " ".join([job_date, job_time]),
                time_format)
            job_runtimes.append(str(now - job_datetime))
            job_datetimes.append(datetime.strftime(job_datetime, time_format))
            append_jobid = False
            append_jobname = True
            continue

        if append_jobname is True:
            job_names.append(line.split()[2])
            append_jobname = False
            continue

        if line == '--':
            append_jobid = True

    df = pd.DataFrame({
        'job_id': job_ids,
        'name': job_names,
        'user': job_users,
        'slots': job_slots,
        'status': job_statuses,
        'status_start': job_datetimes,
        'runtime': job_runtimes})
    if pattern is not None:
        df = df[df.name.str.contains(pattern)]
    if jids is not None:
        df = df[df.job_id.isin(jids)]
    return df[['job_id', 'name', 'slots', 'user', 'status', 'status_start',
               'runtime']]


def qstat_details(jids):
    """get more detailed qstat information

    Args:
        jids (list): list of jobs to get detailed qstat information from

    Returns:
        dictionary of detailted qstat values
    """
    jids = np.atleast_1d(jids)
    cmd = ["qstat", "-j", "%s" % ",".join([str(j) for j in jids])]

    def group_separator(line):
        delim = "".join(["=" for i in range(62)])
        return line == delim

    deets = subprocess.check_output(cmd)
    deets = deets.splitlines()
    jobid = 0
    jobdict = {}
    for key, group in itertools.groupby(deets, group_separator):
        for line in group:
            if group_separator(line):
                continue
            ws = line.split(":")
            k = ws[0].strip()
            v = ":".join(ws[1:]).strip()
            if k == 'job_number':
                v = int(v)
                jobdict[v] = {}
                jobid = v
            jobdict[jobid][k] = v
    return jobdict


def qstat_usage(jids):
    """get usage details for list of jobs

    Args:
        jids (list): list of jobs to get usage details for

    Returns:
        Usage details.
    """
    jids = np.atleast_1d(jids)
    deets = qstat_details(jids)
    usage = {}
    for jid, info in deets.iteritems():
        usage[jid] = {}
        usagestr = info['usage                 1']
        parsus = {u.split("=")[0]: u.split("=")[1]
                  for u in usagestr.split(", ")}
        usage[jid]['usage_str'] = usagestr
        usage[jid].update(parsus)
    return usage


def long_jobs(hour, min, sec, jobdf=None):
    """get list of jobs that have been running for longer than the specified
    time

    Args:
        hour (int): minimum hours of runtime for job filter
        min (int): minimum minutes of runtime for job filter
        sec (int): minimum seconds of runtime for job filter
        jobdf (DataFrame, optional): qstat DataFrame to filter. by default
            will get current qstat and filter it by (hour, min, sec). custom
            DataFrame may be provided by using jobdf

    Returns:
        DataFrame of jobs that have been running longer than the specified time
    """
    if jobdf is None:
        jobdf = qstat()
    else:
        jobdf = jobdf.copy()
    jobdf['runtime'] = jobdf.runtime.apply(
        lambda x: datetime.strptime(x, "%H:%M:%S.%f").time())
    return jobdf[jobdf.runtime >= time(hour, min, sec)]


def current_slot_usage(user=None):
    """number of slots used by current user

    Args:
        user (string): user name to check for usage

    Returns:
        integer number of slots in use by user
    """
    jobs = qstat(user=user, status="r")
    slots = jobs.slots.astype('int').sum()
    return slots


def get_holds(jid):
    """get all current holds for a given job

    Args:
        jid (int): job id to check for list of holds

    Returns:
        comma separated string of job id holds for a given string
    """
    p1 = subprocess.Popen(["qstat", "-j", str(jid)], stdout=subprocess.PIPE)
    p2 = subprocess.Popen(
        ["grep", "jid_predecessor_list:"],
        stdin=p1.stdout,
        stdout=subprocess.PIPE)
    p3 = subprocess.Popen(
        ['awk', '{print $2}'],
        stdin=p2.stdout,
        stdout=subprocess.PIPE)
    p1.stdout.close()
    p2.stdout.close()
    output, err = p3.communicate()
    p3.stdout.close()
    return output.rstrip('\n')


def add_holds(jid, hold_jid):
    """add new hold to an existing job

    Args:
        jid (int): job id of job to add holds to
        hold_jid (int): job id of job to add as hold to jid

    Returns:
        standard output of qalter command
    """
    current_holds = get_holds(jid)
    if len(current_holds) > 0:
        current_holds = ",".join([current_holds, str(hold_jid)])
    stdout = subprocess.check_output(
        ['qalter', str(jid), '-hold_jid', current_holds])
    return stdout


def reqsub(job_id):
    """resubmit a job with the same criteria as initially requested

    Args:
        job_id (int): which job_id to resubmitt

    Returns:
        new job_id of resubmitted job
    """
    return subprocess.check_output(['qmod', '-r', str(job_id)])


DEFAULT_CONDA_ENV_LOCATION = "~/.conda/envs"


def _find_conda_env(name):
    """
    Finds the given Conda environment on the local machine.
    The remote machine may have a different setup, in which case
    a rooted path will be clearer. This checks the CONDA_ENVS_PATH
    variable, if given.
    """
    if "CONDA_ENVS_PATH" in os.environ:
        paths = os.environ["CONDA_ENVS_PATH"].split(":")
    else:
        paths = [os.path.expanduser("~/.conda/envs")]
    for p in paths:
        base_dir = os.path.join(p, name)
        if os.path.exists(base_dir):
            logger.debug("find_conda_env base {}".format(base_dir))
            return base_dir
    else:
        return os.path.expanduser(
                "{}/{}".format(DEFAULT_CONDA_ENV_LOCATION, name))


def qsub(
        run_file,
        job_name,
        parameters=None,
        project=None,
        slots=4,
        memory=10,
        hold_pattern=None,
        holds=None,
        job_type='python',
        stdout=None,
        stderr=None,
        prepend_to_path=None,
        conda_env=None):
    """Submits job to Grid Engine Queue.
    This function provides a convenient way to call scripts for
    R, Python, and Stata using the job_type parameter.
    If the job_type is None, then it executes whatever path
    is in run_file, which may be a shell script or a binary.
    If the parameters argument is a list of lists of parameters or a
    generator of lists of parameters, then this will submit multiple jobs.

    Args:
        run_file (string): absolute path of script or binary to run
        job_name (string): what to call the job
        parameters (tuple or list, optional): arguments to pass to run_file.
        project (string, option): What project to submit the job under. Default
            is ihme_general.
        slots (int, optional): How many slots to request for the job.
            approximate using 1 core == 1 slot, 1 slot = 2GB of RAM
        memory (int, optional): How much ram to request for the job.
        hold_pattern (string, optional): looks up scheduled jobs with names
            matching the specified patten and sets them as holds for the
            requested job.
        holds (list, optional): explicit list of job ids to hold based on.
        job_type (string, optional): joint purpose argument for specifying what
            to pass into the shell_file. can be arbitrary string or one of the
            below options. default is 'python'
                'python':
                    Uses the default python on the path to execute run_file.
                'stata':
                    /usr/local/bin/stata-mp -b do run_file
                'R':
                    /usr/local/bin/R < runfile --no-save --args
                None:
                    This means the run_file is, itself, the executable.
        stdout (string, optional): where to pipe standard out to. default is
            /dev/null. Recognizes $HOME, $USER, $JOB_ID, $JOB_NAME, $HOSTNAME,
            and $TASK_ID.
        stderr (string, optional): where to pipe standard error to. default is
            /dev/null.  Recognizes $HOME, $USER, $JOB_ID, $JOB_NAME, $HOSTNAME,
            and $TASK_ID.
        prepend_to_path (string, optional): Copies the current shell's
            environment variables and prepends the given one. Without this,
            the PATH is typically very short (/usr/local/bin:/usr/bin).
        conda_env (string, optional): If the job_type is Python, this
            finds the Conda environment by with the given name on the
            submitting host. If conda_env
            is a rooted path (starts with "/"), then this uses the
            path as given.

    Returns:
        job_id of submitted job
    """
    assert slots > 0
    assert not (holds and hold_pattern)
    assert len(str(job_name)) > 0
    assert not (conda_env and not job_type == "python")
    assert not isinstance(parameters, str), (
        "'parameters' cannot be a string. Must be a list or a tuple.")

    # Set holds, if requested
    if hold_pattern is not None:
        holds = qstat(hold_pattern)[1]

    if isinstance(holds, str):
        holds = holds.replace(" ", "")
    elif isinstance(holds, collections.abc.Sequence):
        holds = ",".join([str(hold_job_id) for hold_job_id in holds])
    elif holds:
        logger.error("Holds not a string or list {}".format(holds))
        raise ValueError("Holds not a string or list {}".format(holds))
    else:
        pass  # No holds

    session = _drmaa_session()
    template = session.createJobTemplate()

    native = [
        "-P {}".format(project) if project else None,
        "-w n",  # Needed for mem_free to work. Turns off validation.
        "-pe multi_slot {}".format(str(slots)),
        "-l mem_free={!s}G".format(memory) if memory else None,
        "-hold_jid {}".format(holds) if holds else None,
    ]
    template.nativeSpecification = " ".join(
            [str(native_arg) for native_arg in native if native_arg])
    logger.debug("qsub native {}".format(template.nativeSpecification))
    template.jobName = UGE_NAME_POLICY.sub("", job_name)
    template.outputPath = ":" + (stdout or "/dev/null")
    template.errorPath = ":" + (stderr or "/dev/null")

    run_file = os.path.expanduser(run_file)
    environment_variables = dict()
    # Moved this out of Python section because it calls os.path.exist
    # which shouldn't be called in a loop.
    if conda_env:
        if not os.path.isabs(conda_env):
            python_base = _find_conda_env(conda_env)
            python_binary = os.path.join(python_base, "bin/python")
        else:
            python_base = conda_env
            python_binary = os.path.join(conda_env, "bin/python")
        r_path = os.path.join(python_base, "lib/R/lib")
        if "LD_LIBRARY_PATH" in os.environ:
            environment_variables["LD_LIBRARY_PATH"] = (
                "{}:{}".format(r_path, os.environ["LD_LIBRARY_PATH"]))
        else:
            environment_variables["LD_LIBRARY_PATH"] = r_path
    else:
        python_binary = "python"

    if prepend_to_path:
        path = "{}:{}".format(prepend_to_path, os.environ["PATH"])
        environment_variables["PATH"] = path

    if environment_variables:
        template.jobEnvironment = environment_variables
        logger.debug("qsub environment {}".format(template.jobEnvironment))

    if parameters:
        if isinstance(parameters, types.GeneratorType):
            pass
        elif isinstance(parameters[0], str):
            parameters = [parameters]
        elif isinstance(parameters[0], collections.abc.Sequence):
            pass
        else:
            parameters = [parameters]
    else:
        parameters = [list()]

    job_ids = list()
    for params in parameters:
        str_params = [str(bare_arg).strip() for bare_arg in params]
        shell_args = list()
        if job_type == "python":
            shell_args.append(python_binary)
            shell_args.append(run_file)
            if str_params:
                shell_args.extend(str_params)
        elif job_type == "stata":
            shell_args.extend(["/usr/local/bin/stata-mp", "-b", "do", run_file])
            if str_params:
                shell_args.extend(str_params)
        elif job_type == "R":
            shell_args.append("/usr/local/bin/R")
            # For R, arguments need to be a single entry in ARGV, so join them.
            r_args = "--args {}".format(" ".join(str_params))
            shell_args.extend(["--vanilla", "-f", run_file, r_args])
        elif job_type:
            raise ValueError("sge.qsub unknown job type {}".format(job_type))
        else:
            shell_args.append(run_file)
            if str_params:
                shell_args.extend(str_params)
        logger.info("qsub {}".format(shell_args))

        template.remoteCommand = shell_args[0]
        if len(shell_args) > 1:
            template.args = shell_args[1:]

        job_ids.append(session.runJob(template))

    if len(job_ids) == 1:
        return job_ids[0]
    else:
        return job_ids


def _wait_done(job_ids):
    """
    For unit tests. Ensures the jobs are done or running.
    If the queue is long, this will give spurious errors.
    """
    session = _drmaa_session()
    wait_duration = 5 * 60  # seconds
    try:
        session.synchronize(job_ids, wait_duration)
    except drmaa.errors.ExitTimeoutException:
        return False
    for status_check in job_ids:
        status = session.jobStatus(status_check)
        if status not in {drmaa.JobState.DONE, drmaa.JobState.RUNNING}:
            logger.error("job_id {} has status {}".format(status_check, status))
            raise RuntimeError("job status not done or running {}".format(
                    status_check))
    return True


def get_commit_hash(dir="."):
    """get the git commit hash for a given directory

    Args:
        dir (string): which directory to get the git hash for. defaults to
            current directory

    Returns:
        git commit hash
    """
    cmd = [
        'git',
        '--git-dir=%s/.git' % dir,
        '--work-tree=%s',
        'rev-parse',
        'HEAD']
    return subprocess.check_output(cmd).strip()


def get_branch(dir="."):
    """get the git branch for a given directory

    Args:
        dir (string): which directory to get the git commit for. defaults to
            current directory

    Returns:
        git commit branch
    """
    cmd = [
        'git',
        '--git-dir=%s/.git' % dir,
        '--work-tree=%s',
        'rev-parse',
        '--abbrev-ref',
        'HEAD']
    return subprocess.check_output(cmd).strip()


def git_dict(dir="."):
    """get a dictionary of the git branch and hash for given directory.

    Args:
        dir (string): which directory to get the dictionary for

    Returns:
        dictionary
    """

    branch = get_branch(dir)
    commit = get_commit_hash(dir)
    return {'branch': branch, 'commit': commit}
