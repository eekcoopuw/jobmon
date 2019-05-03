import getpass
import os
import os.path as path
import pytest

try:
    from jobmon.client.swarm.executors import sge_utils as sge
except KeyError:
    pass


@pytest.mark.cluster
def test_true_path():
    with pytest.raises(ValueError) as exc_info:
        sge.true_path()
    assert "cannot both" in str(exc_info)

    assert sge.true_path("") == os.getcwd()
    assert getpass.getuser() in sge.true_path("~/bin")
    assert sge.true_path("blah").endswith("/blah")
    assert sge.true_path(file_or_dir=".") == path.abspath(".")
    # the path differs based on the cluster but all are in /bin/time
    # (some are in /usr/bin/time)
    assert "/bin/time" in sge.true_path(executable="time")


@pytest.mark.cluster
def test_project_limits():
    project = 'ihme_general'
    limit = sge.get_project_limits(project)
    cluster_name = os.environ['SGE_CLUSTER_NAME']
    if cluster_name == 'prod':
        assert limit == 250
    elif cluster_name == 'cluster':
        assert limit == 10000  # limit is global limit on the fair cluster
    else:
        assert limit == 200


def test_convert_wallclock():
    wallclock_str = '10:11:50'
    res = sge.convert_wallclock_to_seconds(wallclock_str)
    assert res == 36710.0


def test_convert_wallclock_with_days():
    wallclock_str = '01:10:11:50'
    res = sge.convert_wallclock_to_seconds(wallclock_str)
    assert res == 123110.0


def test_convert_wallclock_with_milleseconds():
    wallclock_str = '01:10:11:50.15'
    res = sge.convert_wallclock_to_seconds(wallclock_str)
    assert res == 123110.15
