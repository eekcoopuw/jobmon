import os
import subprocess
from datetime import datetime, time
import pandas as pd
import numpy as np
import itertools

this_path = os.path.dirname(os.path.abspath(__file__))


def qstat(status=None, pattern=None, user=None):
    cmd = ["qstat", "-r"]
    if status is not None:
        cmd.extend(["-s", status])
    if user is not None:
        cmd.extend(["-u", user])
    if user == '"*"':
        p1 = subprocess.Popen(
            " ".join(cmd),
            stdout=subprocess.PIPE,
            shell=True)
    else:
        p1 = subprocess.Popen(cmd, stdout=subprocess.PIPE)

    if pattern is None:
        p2 = subprocess.Popen(
            ["grep", "Full jobname:", "-B1"],
            stdin=p1.stdout,
            stdout=subprocess.PIPE)
    else:
        p2 = subprocess.Popen(
            ["grep", "Full jobname:[[:blank:]]*"+pattern+"$", "-B1"],
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
            job_runtimes.append(str(now-job_datetime))
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
    return df[['job_id', 'name', 'slots', 'user', 'status', 'status_start',
               'runtime']]


def qstat_details(jids):
    jids = np.atleast_1d(jids)
    cmd = ["qstat", "-j",  "%s" % ",".join([str(j) for j in jids])]

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
    if jobdf is None:
        jobdf = qstat()
    else:
        jobdf = jobdf.copy()
    jobdf['runtime'] = jobdf.runtime.apply(
        lambda x: datetime.strptime(x, "%H:%M:%S.%f").time())
    return jobdf[jobdf.runtime >= time(hour, min, sec)]


def current_slot_usage(user=None):
    jobs = qstat(user=user, status="r")
    slots = jobs.slots.astype('int').sum()
    return slots


def get_holds(jid):
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
    current_holds = get_holds(jid)
    if len(current_holds) > 0:
        current_holds = ",".join([current_holds, str(hold_jid)])
    stdout = subprocess.check_output(
        ['qalter', str(jid), '-hold_jid', current_holds])
    return stdout


def reqsub(job_id):
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
        stderr=None):

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
    if shfile is None:
        shfile = "%s/submit_master.sh" % (this_path)
    else:
        shfile = os.path.abspath(os.path.expanduser(shfile))
    runfile = runfile.strip(' \t\r\n')
    runfile = os.path.abspath(os.path.expanduser(runfile))
    submission_params.append(shfile)
    if jobtype == "python":
        submission_params.append("/ihme/code/central_comp/anaconda/bin/python")
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
    else:
        submission_params.append(jobtype)
        submission_params.append(runfile)

    # Creat full submission array
    submission_params.extend(parameters)

    # Submit job
    submission_msg = subprocess.check_output(submission_params)
    print(submission_msg)
    jid = submission_msg.split()[2]
    return jid


def get_commit_hash(dir="."):
    cmd = [
        'git',
        '--git-dir=%s/.git' % dir,
        '--work-tree=%s',
        'rev-parse',
        'HEAD']
    return subprocess.check_output(cmd).strip()


def get_branch(dir="."):
    cmd = [
        'git',
        '--git-dir=%s/.git' % dir,
        '--work-tree=%s',
        'rev-parse',
        '--abbrev-ref',
        'HEAD']
    return subprocess.check_output(cmd).strip()


def git_dict(dir="."):
    branch = get_branch(dir)
    commit = get_commit_hash(dir)
    return {'branch': branch, 'commit': commit}
