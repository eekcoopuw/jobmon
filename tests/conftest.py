import sys
import pytest
import uuid

from jobmon.central_job_monitor import CentralJobMonitor
from time import sleep


@pytest.fixture(scope='module')
def tmp_out_dir(tmpdir_factory):
    return tmpdir_factory.mktemp("dalynator_{}_".format(uuid.uuid4()))


@pytest.fixture(scope='function')
def central_jobmon(tmpdir_factory):
    monpath = tmpdir_factory.mktemp("jmdir")

    if sys.version_info > (3, 0):
        jm = CentralJobMonitor(str(monpath))
    else:
        jm = CentralJobMonitor(str(monpath), persistent=False)
    sleep(1)
    yield jm
    print("teardown fixture in {}".format(monpath))
    jm.stop_responder()
    sleep(1)
    assert not jm.responder_proc_is_alive()


@pytest.fixture
def central_jobmon_static_port(tmpdir_factory):
    monpath = tmpdir_factory.mktemp("jmdir")
    jm = CentralJobMonitor(str(monpath), port=3459)
    sleep(1)
    yield jm
    print("teardown fixture in {}".format(monpath))
    jm.stop_responder()
    sleep(1)
    assert not jm.responder_proc_is_alive()


def pytest_addoption(parser):
    """Add CLI options to run slow and cluster tests"""
    parser.addoption('--slow',
                     action='store_true',
                     default=False,
                     help='Also run slow tests')
    parser.addoption('--cluster',
                     action='store_true',
                     default=False,
                     help='Also run tests that can only run on the cluster')


def pytest_runtest_setup(item):
    """Skip tests if they are marked as slow and --slow is not given.
    Ditto for cluster"""
    check_skip(item, 'slow')
    check_skip(item, 'cluster')


def check_skip(item, mark):
    if getattr(item.obj, mark, None) and not item.config.getvalue(mark):
        pytest.skip('{} tests not requested'.format(mark))
