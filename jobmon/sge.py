import os
import subprocess
from datetime import datetime, time
import pandas as pd
import numpy as np
import itertools


def true_path(file_or_dir=None, executable=None):
    """Get true path to file or executable"""
    if file_or_dir is not None:
        f = file_or_dir
    if executable is not None:
        f = subprocess.check_output(["which", executable])
    f = os.path.abspath(os.path.expanduser(f))
    f = f.strip(' \t\r\n')
    return f


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
    """get more detailted qstat information

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


def qsub(
        runfile,
        jobname,
        parameters=[],
        project=None,
        slots=4,
        memory=10,
        hold_pattern=None,
        holds=None,
        shfile=None,
        jobtype='python',
        stdout=None,
        stderr=None,
        prepend_to_path=None,
        conda_env=None):
    """Submit job to sun grid engine queue

    Args:
        runfile (string): absolute path of script to run
        jobname (string): what to call the job
        parameters (tuple or list, optional): arguments to pass to runfile.
        project (string, option): What project to submit the job under. Default
            is ihme_general.
        slots (int, optional): How many slots to request for the job.
            approximate using 1 core == 1 slot, 1 slot = 2GB of RAM
        memory (int, optional): How much ram to requestion for the job.
        hold_pattern (string, optional): looks up scheduled jobs with names
            matching the specified patten and sets them as holds for the
            requested job.
        holds (list, optional): explicit list of job ids to hold based on.
        shfile (string, optional): shell file to execute in job
        jobtype (string, optional): joint purpose argument for specifying what
            to pass into the shfile. can be arbitrary string or one of the
            below options. default is 'python'
                'python':
                    /ihme/code/central_comp/anaconda/bin/python runfile
                'stata':
                    /usr/local/bin/stata-mp -b do runfile
                'R':
                    /usr/local/bin/R < runfile --no-save --args
        stdout (string, optional): where to pipe standard out to. default is
            /dev/null
        stderr (string, optional): where to pipe standard error to. default is
            /dev/null
        prepend_to_path (string, optional): optionally prepend directory to
            environment path
        conda_env (string, optional): optionally select which conda environment
            to run python from.

    Returns:
        job_id of submitted job
    """

    # Set CPU and memory
    submission_params = [
        "qsub", "-pe", "multi_slot", str(slots), "-l", "mem_free=%sg" % memory]
    if project is not None:
        submission_params.extend(["-P", project])

    # Set holds, if requested
    if hold_pattern is not None:
        holds = qstat(hold_pattern)[1]

    if holds is not None and len(holds) > 0:
        if isinstance(holds, (list, tuple)):
            submission_params.extend(['-hold_jid', ",".join(holds)])
        else:
            submission_params.extend(['-hold_jid', str(holds)])

    # Set job name
    submission_params.extend(["-N", jobname])

    # Write stdout/stderr to files
    if stdout is not None:
        submission_params.extend(['-o', stdout])
    if stderr is not None:
        submission_params.extend(['-e', stderr])

    # Convert all parameters to strings
    assert (isinstance(parameters, (list, tuple)) and not
            isinstance(parameters, str)), (
        "'parameters' cannot be a string. Must be a list or a tuple.")
    parameters = [str(p) for p in parameters]
    parameters = [p.strip(' \t\r\n') for p in parameters]

    # Define script to run and pass parameters
    if shfile is None and conda_env is None:
        shfile = true_path(executable="submit_master.sh")
    elif shfile is None and conda_env is not None:
        shfile = true_path(executable="env_submit_master.sh")
    else:
        shfile = true_path(file_or_dir=shfile)
    runfile = os.path.expanduser(runfile)
    submission_params.append(shfile)
    if prepend_to_path is not None:
        submission_params.append(prepend_to_path)
    else:
        submission_params.append("")
    if conda_env is not None:
        submission_params.append(conda_env)
    if jobtype == "python":
        submission_params.append("python")
        submission_params.append(runfile)
    elif jobtype == "stata":
        submission_params.append("/usr/local/bin/stata-mp")
        submission_params.append("-b")
        submission_params.append("do")
        submission_params.append(runfile)
    elif jobtype == "R":
        submission_params.append("/usr/local/bin/R")
        submission_params.append("<")
        submission_params.append(runfile)
        submission_params.append("--no-save")
        submission_params.append("--args")
    elif jobtype is not None:
        submission_params.append(jobtype)
        submission_params.append(runfile)
    else:
        submission_params.append(runfile)

    # Creat full submission array
    submission_params.extend(parameters)
    print submission_params

    # Submit job
    submission_msg = subprocess.check_output(submission_params)
    print(submission_msg)
    jid = submission_msg.split()[2]
    return jid


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
