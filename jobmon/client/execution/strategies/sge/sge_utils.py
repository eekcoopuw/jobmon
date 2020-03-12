"""Interface to the dynamic resource manager (DRM), aka the scheduler."""

from datetime import datetime
import itertools
import os
import re
import subprocess
from typing import List, Dict, Tuple, Any
import numpy as np

from jobmon.client import ClientLogging as logging

this_path = os.path.dirname(os.path.abspath(__file__))
logger = logging.getLogger(__name__)

# Comes from object_name in `man sge_types`. Also, * excluded.
UGE_NAME_POLICY = re.compile(
    "[.#\n\t\r /\\\[\]:'{}\|\(\)@%,*]|[\n\t\r /\\\[\]:'{}\|\(\)@%,*]")
STATA_BINARY = "/usr/local/bin/stata-mp"
R_BINARY = "/usr/local/bin/R"
DEFAULT_CONDA_ENV_LOCATION = "~/.conda/envs"
SGE_UNKNOWN_ERROR = -9998


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


def qstat(status: str=None, pattern: str=None, user: str=None,
          jids: List[int]=None) -> Dict:
    """parse sge qstat information into a Dictionary keyed by executor_id
    (sge job id)

    Args:
        status (string, optional): status filter to use when running qstat
            command for job statuses
        pattern (string, optional): pattern filter to use when running qstat
            command for certain qstat job names
        user (string, optional): user filter to use when running qstat
            command for job users
        jids (list, optional): list of job ids to use when running qstat
            command

    Returns:
        Dictionary of qstat return values keyed by executor_id (sge job id)
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
            except Exception:
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
            continue

    # assuming that all arrays are the same length
    jobs = {}
    for index in range(len(job_ids)):
        if pattern is None or pattern in job_names[index]:
            if status is None or job_statuses[index] == status:
                if user is None or job_users[index] == user:
                    if jids is None or job_ids[index] in jids:
                        job = {'job_id': job_ids[index],
                               'hostname': job_hosts[index],
                               'name': job_names[index],
                               'user': job_users[index],
                               'slots': job_slots[index],
                               'status': job_statuses[index],
                               'status_start': job_datetimes[index],
                               'runtime': job_runtime_strs[index],
                               'runtime_seconds': job_runtimes[index]}
                        jobs[job_ids[index]] = job
    logger.debug(f"Lines: {lines}, Jobs: {jobs}")
    return jobs


def qstat_details(jids: List[int]):
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
    jids = [str(int(jid)) for jid in np.atleast_1d(job_ids)]
    stdout = subprocess.check_output(['qdel'] + jids)
    logger.debug(f"stdout from qdel: {stdout}")
    return stdout


def qacct_exit_status(jid: int) -> Tuple[int, str]:
    failed_reason = ""
    cmd1 = f"qacct -j {jid}"
    logger.debug("*** qacct command: " + cmd1)
    try:
        codes = {}
        res = subprocess.check_output(cmd1, shell=True, universal_newlines=True)
        res = res.split("\n")
        for line in res:
            if 'failed' in line or 'exit_status' in line:
                status_line = line.split()
                codes[status_line[0]] = status_line[1:]
        logger.debug(f"Exit code response: {codes}")
        if 'exit_status' not in codes:
            raise RuntimeError(f"Can't find 'exit_status' from qacct: {res}")
        if 'failed' not in codes:
            raise RuntimeError(f"Can't find 'failed' from qacct: {res}")
        exit_code = codes['exit_status'][0]
        if 'h_rt' in codes['failed']:
            failed_reason = 'over runtime'
        return int(exit_code), failed_reason
    except Exception as e:
        # In case the command execution failed, log error and return -1
        logger.error(str(e))
        return SGE_UNKNOWN_ERROR, str(e)


def qacct_hostname(jid: int) -> str:
    if jid is None:
        return None
    # For strange reason f string or format does not work
    cmd1 = "qacct -j %s |grep hostname|awk \'{print $2}\'" % jid
    logger.debug(cmd1)
    try:
        res = subprocess.check_output(
            cmd1, shell=True).decode("utf-8").replace("\n", "")
        return res
    except Exception as e:
        # In case the command execution failed, log error and return None
        logger.error(str(e))
        return None


def qstat_hostname(jid: int) -> str:
    if jid is None:
        return None
    # For strange reason f string or format does not work
    cmd1 = " qstat -j %s |grep exec_host_list | awk -F \':\' \'{print $2}\'" % jid
    logger.debug(cmd1)
    try:
        res = subprocess.check_output(
            cmd1, shell=True).decode("utf-8").replace("\n", "").strip()
        return res
    except Exception as e:
        # In case the command execution failed, log error and return -1
        logger.error(str(e))
        return None


def transform_mem_to_gb(mem_str: Any) -> float:
   # we allow both upper and lowercase g, m, t options
   # BUG g and G are not the same
   # For g vs G, please refer to https://docs.ukcloud.com/articles/other/other-ref-gib.html
   if mem_str is None:
       return 2
   if type(mem_str) in (float, int):
       return mem_str
   if mem_str[-1].lower() == "m":
       mem = float(mem_str[:-1])
       mem /= 1000
   elif mem_str[-2:].lower() == "mb":
       mem = float(mem_str[:-2])
       mem /= 1000
   elif mem_str[-1].lower() == "t":
       mem = float(mem_str[:-1])
       mem *= 1000
   elif mem_str[-2:].lower() == "tb":
       mem = float(mem_str[:-2])
       mem *= 1000
   elif mem_str[-1].lower() == "g":
       mem = float(mem_str[:-1])
   elif mem_str[-2:].lower() == "gb":
       mem = float(mem_str[:-2])
   else:
       mem = 1
       return mem
