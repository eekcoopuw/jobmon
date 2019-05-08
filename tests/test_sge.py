import getpass
import os
import os.path as path
import subprocess

import pytest

# This import is needed for the monkeypatch
import jobmon.client.swarm.executors

from jobmon.client.swarm.executors.sge import SGEExecutor
import jobmon.client.swarm.executors.sge_utils as sge_utils


@pytest.mark.cluster
def test_true_path():
    with pytest.raises(ValueError) as exc_info:
        sge_utils.true_path()
    assert "cannot both" in str(exc_info)

    assert sge_utils.true_path("") == os.getcwd()
    assert getpass.getuser() in sge_utils.true_path("~/bin")
    assert sge_utils.true_path("blah").endswith("/blah")
    assert sge_utils.true_path(file_or_dir=".") == path.abspath(".")
    # the path differs based on the cluster but all are in /bin/time
    # (some are in /usr/bin/time)
    assert "/bin/time" in sge_utils.true_path(executable="time")


@pytest.mark.cluster
def test_project_limits():
    project = 'ihme_general'
    limit = sge_utils.get_project_limits(project)
    cluster_name = os.environ['SGE_CLUSTER_NAME']
    if cluster_name == 'prod':
        assert limit == 250
    elif cluster_name == 'cluster':
        assert limit == 10000  # limit is global limit on the fair cluster
    else:
        assert limit == 200


def test_convert_wallclock():
    wallclock_str = '10:11:50'
    res = sge_utils.convert_wallclock_to_seconds(wallclock_str)
    assert res == 36710.0


def test_convert_wallclock_with_days():
    wallclock_str = '01:10:11:50'
    res = sge_utils.convert_wallclock_to_seconds(wallclock_str)
    assert res == 123110.0


def test_convert_wallclock_with_milleseconds():
    wallclock_str = '01:10:11:50.15'
    res = sge_utils.convert_wallclock_to_seconds(wallclock_str)
    assert res == 123110.15


class BadSGEExecutor():
    """Mock the intercom interface for testing purposes, specifically
    to raise exceptions"""

    def get_usage_stats(self):
        raise subprocess.CalledProcessError(
            "No usage stats for you today laddy")


def test_bad_qstat_call(monkeypatch):
    monkeypatch.setattr(
        jobmon.client.swarm.executors.sge,
        "SGEExecutor",
        BadSGEExecutor)
    with pytest.raises(subprocess.CalledProcessError):
        s = SGEExecutor()
        s.get_usage_stats()
