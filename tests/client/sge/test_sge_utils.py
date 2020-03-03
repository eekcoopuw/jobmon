import getpass
import os
import os.path as path
import pytest
from unittest.mock import patch

from jobmon.client.execution.strategies.sge.sge_utils import true_path, convert_wallclock_to_seconds, qacct_exit_status, \
    qacct_hostname



def qacct_returns_h_rt(command, shell, universal_newlines):
    return '==============================================================' \
           '\nqname        all.q               \nhostname     gen-uge-archiv' \
           'e-p089.cluster.ihme.washington.edu\ngroup        Domain Users   ' \
           '     \nowner        nariss              \nproject      proj_tool' \
           's          \ndepartment   defaultdepartment   \njobname      myt' \
           'est              \njobnumber    11399639            \ntaskid    ' \
           '   undefined\npe_taskid    NONE                \naccount      sg' \
           'e                 \npriority     0                   \ncwd      ' \
           '    NONE                \nsubmit_host  gen-uge-submit-p01.hosts.' \
           'ihme.washington.edu\nsubmit_cmd   qsub -e /ihme/homes/nariss/log' \
           's/errors -o /ihme/homes/nariss/logs/output -q all.q -P proj_tool' \
           's -l fthread=2 -l m_mem_free=1G -l h_rt=00:30:00 -N mytest /ihme' \
           '/singularity-images/rstudio/shells/execR.sh -s /ihme/homes/naris' \
           's/test_r.r 5 100\nqsub_time    08/07/2019 16:55:33.829\nstart_ti' \
           'me   08/07/2019 16:55:36.043\nend_time     08/07/2019 16:55:36.8' \
           '55\ngranted_pe   NONE                \nslots        1           ' \
           '        \nfailed       44  : execd enforced h_rt limit    \ndele' \
           'ted_by   NONE\nexit_status  1377' \
           '                 \nru_wallclock 0.812        \nru_utime     0.1' \
           '38        \nru_stime     0.333        \nru_maxrss    18680      ' \
           '         \nru_ixrss     0                   \nru_ismrss    0    ' \
           '               \nru_idrss     0                   \nru_isrss    ' \
           ' 0                   \nru_minflt    27178               \nru_maj' \
           'flt    29                  \nru_nswap     0                   \n' \
           'ru_inblock   2746                \nru_oublock   80              ' \
           '    \nru_msgsnd    0                   \nru_msgrcv    0         ' \
           '          \nru_nsignals  0                   \nru_nvcsw     3072' \
           '                \nru_nivcsw    38                  \nwallclock  ' \
           '  0.967        \ncpu          0.472        \nmem          0.000 ' \
           '            \nio           0.000             \niow          0.00' \
           '0             \nioops        0                   \nmaxvmem      ' \
           '0.000\nmaxrss       0.000\nmaxpss       0.000\narid         unde' \
           'fined\njc_name      NONE\nbound_cores  NONE\n'


@pytest.mark.unittest
def test_true_path():
    with pytest.raises(ValueError) as exc_info:
        true_path()
    assert "cannot both" in str(exc_info.value)

    assert true_path("") == os.getcwd()
    assert getpass.getuser() in true_path("~/bin")
    assert true_path("blah").endswith("/blah")
    assert true_path(file_or_dir=".") == path.abspath(".")
    # the path differs based on the cluster but all are in /bin/time
    # (some are in /usr/bin/time)
    assert "/bin/time" in true_path(executable="time")


@pytest.mark.unittest
@pytest.mark.parametrize("wallclock_str, expect", [('10:11:50', 36710.0),
                                                   ('01:10:11:50', 123110.0),
                                                   ('01:10:11:50.15', 123110.15)])
def test_convert_wallclock(wallclock_str, expect):
    res = convert_wallclock_to_seconds(wallclock_str)
    assert res == expect


@pytest.mark.unittest
def test_qacct_exit_status():
    with patch("subprocess.check_output") as m_check_output:
        m_check_output.side_effect = qacct_returns_h_rt
        exit_status = qacct_exit_status(11399639)
        assert exit_status == (1377, 'over runtime')

