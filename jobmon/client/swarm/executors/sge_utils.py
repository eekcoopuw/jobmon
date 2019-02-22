"""Interface to the dynamic resource manager (DRM), aka the scheduler."""

try:
    from collections.abc import Sequence
except ImportError:
    from collections import Sequence
from datetime import datetime
import itertools
import logging
import os
import re
import subprocess

import pandas as pd
import numpy as np


this_path = os.path.dirname(os.path.abspath(__file__))
logger = logging.getLogger(__name__)

# Comes from object_name in `man sge_types`. Also, * excluded.
UGE_NAME_POLICY = re.compile(
    "[.#\n\t\r /\\\[\]:'{}\|\(\)@%,*]|[\n\t\r /\\\[\]:'{}\|\(\)@%,*]")
STATA_BINARY = "/usr/local/bin/stata-mp"
R_BINARY = "/usr/local/bin/R"
DEFAULT_CONDA_ENV_LOCATION = "~/.conda/envs"


# TODO Should this be two separate functions?
def true_path(file_or_dir=None, executable=None):
    """Get true path to file or executable.

    Args:
        file_or_dir (str): partial file path, to be expanded as per the current
            user
        executable (str): the name of an executable, which will be resolved
            using "which"

    Specify one of the two arguments, not both.
    """
    if file_or_dir is not None:
        f = file_or_dir
    elif executable is not None:
        f = subprocess.check_output(["which", str(executable)])
    else:
        raise ValueError("true_path: file_or_dir and executable "
                         "cannot both be null")

    # Be careful, in python 3 check_output returns bytes
    if not isinstance(f, str):
        f = f.decode('utf-8')
    f = os.path.abspath(os.path.expanduser(f))
    return f.strip(' \t\r\n')


def get_project_limits(project):
    """This function uses the qconf call from the toolbox to get project_limits
    from the cluster.
    See /share/local/IT/scripts/cluster_projects_report_admin.sh
    """
    if not project:
        project = 'ihme_general'
    call = ("""qconf -srqs | egrep -A 1 -i "TRUE" | grep -i limit | grep """ +
            project + """| sort | sed -e "s/^.*limit//" -e "s/projects//g" -e "s/users//" -e "s/to slots//g" -e "s/ =/:/g"| tr -s " " | awk -F':' '{printf "%5d", $2}' | sort -k 2 -n -r | pr -W 95 -T -t --columns 1""")
    res = subprocess.check_output(call, shell=True)
    try:
        return int(res)
    except ValueError as e:
        logger.warning("Could not get project slot limits, if you are on the "
                       "dev cluster this is fine because there are no set "
                       "project limits. Res is {}. ValueError is {}"
                       .format(res, e))
    # enforces a default limit since dev does not have project limits
    return 200


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
        ["grep", "Full jobname:", "-B1", "-A1"],
        stdin=p1.stdout,
        stdout=subprocess.PIPE)

    p1.stdout.close()
    output, err = p2.communicate()
    p2.stdout.close()

    # Careful, python 2 vs 3 - bytes versus strings
    if not isinstance(output, str):
        output = output.decode('utf-8')

    lines = output.splitlines()

    job_ids = []
    job_users = []
    job_names = []
    job_slots = []
    job_statuses = []
    job_datetimes = []
    job_runtimes = []
    job_runtime_strs = []
    job_hosts = []
    linetype = "job_summary"
    time_format = "%m/%d/%Y %H:%M:%S"
    now = datetime.now()
    for line in lines:

        if line == '--':
            linetype = "job_summary"
            continue

        if linetype == "job_summary":
            job_ids.append(int(line.split()[0]))
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
            job_runtimes.append((now - job_datetime).total_seconds())
            job_runtime_strs.append(str(now - job_datetime))
            job_datetimes.append(datetime.strftime(job_datetime, time_format))
            linetype = "job_name"
            continue

        if linetype == "job_name":
            job_names.append(line.split()[2])
            linetype = "job_host"
            continue

        if linetype == "job_host":
            if "Master Queue" not in line:
                job_hosts.append("")
                continue
            host = line.split()[2].split("@")[1]
            job_hosts.append(host)
            lintype = "--"
            continue

    df = pd.DataFrame({
        'job_id': job_ids,
        'hostname': job_hosts,
        'name': job_names,
        'user': job_users,
        'slots': job_slots,
        'status': job_statuses,
        'status_start': job_datetimes,
        'runtime': job_runtime_strs,
        'runtime_seconds': job_runtimes})
    if pattern is not None:
        df = df[df.name.str.contains(pattern)]
    if jids is not None:
        df = df[df.job_id.isin(jids)]
    return df[['job_id', 'hostname', 'name', 'slots', 'user', 'status',
               'status_start', 'runtime', 'runtime_seconds']]


def qstat_details(jids):
    """get more detailed qstat information

    Args:
        jids (list): list of jobs to get detailed qstat information from

    Returns:
        dictionary of detailed qstat values
    """
    # Explored parsing the xml output instead of the raw qstat stdout, but gave
    # up after developing a headache trying to make sense of the schema.
    # Anecdotally, also found qstat -xml itself to be slower than normal qstat.
    # Found these useful to commiserate with:
    #
    #    http://wiki.gridengine.info/wiki/index.php/GridEngine_XML
    #    http://arc.liv.ac.uk/pipermail/sge-discuss/2013-August/000473.html
    #
    # Feel free to switch to XML if you prefer that structure, but my opinion
    # is it's more trouble than it's worth. IMHO - Beautiful Soup is great,
    # XML itself less so:
    #
    #    https://www.crummy.com/software/BeautifulSoup/bs4/doc/
    #
    # -- tom
    jids = np.atleast_1d(jids)
    cmd = ["qstat", "-j", "%s" % ",".join([str(j) for j in jids])]

    def group_separator(line):
        delim = "".join(["=" for i in range(62)])
        return line == delim

    deets = subprocess.check_output(cmd).decode("utf-8")
    deets = deets.splitlines()
    jobid = 0
    jobdict = {}
    for key, group in itertools.groupby(deets, group_separator):
        for line in group:
            if group_separator(line):
                continue
            ws = line.split(":")
            k = ws[0].strip()
            k = re.sub('\s*1', '', k)  # remove inexplicable __1s in qstat keys
            v = ":".join(ws[1:]).strip()
            if k == 'job_number':
                v = int(v)
                jobdict[v] = {}
                jobid = v
            jobdict[jobid][k] = v
    return jobdict


def convert_wallclock_to_seconds(wallclock_str):
    wc_list = wallclock_str.split(':')
    wallclock = (float(wc_list[-1]) + int(wc_list[-2]) * 60 +
                 int(wc_list[-3]) * 3600)  # seconds.milliseconds, minutes, hrs
    if len(wc_list) == 4:
        wallclock += (int(wc_list[-4]) * 86400)  # days
    elif len(wc_list) > 4:
        raise ValueError("Cant parse wallclock for logging. Contains more info"
                         " than days, hours, minutes, seconds, milliseconds")
    return wallclock


def qstat_usage(jids):
    """get usage details for list of jobs

    Args:
        jids (list): list of jobs to get usage details for

    Returns:
        Usage details.
    """
    jids = np.atleast_1d(jids)
    details = qstat_details(jids)
    usage = {}
    for jid, info in details.items():
        usage[jid] = {}
        usagestr = info['usage']
        parsus = {u.split("=")[0]: u.split("=")[1]
                  for u in usagestr.split(", ")}
        parsus['wallclock'] = convert_wallclock_to_seconds(parsus['wallclock'])
        usage[jid]['usage_str'] = usagestr
        usage[jid]['nodename'] = info['exec_host_list']
        usage[jid].update(parsus)
    return usage


def qdel(job_ids):
    jids = [str(jid) for jid in np.atleast_1d(job_ids)]
    stdout = subprocess.check_output(['qdel'] + jids)
    return stdout
