import sys
import pytest
import uuid

from jobmon.central_job_monitor import CentralJobMonitor
from time import sleep


@pytest.fixture(scope='module')
def tmp_out_dir(tmpdir_factory):
    return tmpdir_factory.mktemp("dalynator_{}_".format(uuid.uuid4()))


@pytest.fixture(scope='function')
def central_jobmon_cluster(tmpdir_factory):
    monpath = tmpdir_factory.mktemp("jmdir")

    if sys.version_info > (3, 0):
        jm = CentralJobMonitor(str(monpath))
    else:
        jm = CentralJobMonitor(str(monpath), persistent=False)
    sleep(1)
    yield jm
    print("teardown fixture in {}".format(monpath))
    jm.stop_responder()
    jm.stop_publisher()
    sleep(1)
    assert not jm.responder_proc_is_alive()


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
    jm.stop_publisher()
    sleep(1)
    assert not jm.responder_proc_is_alive()


@pytest.fixture(scope='module')
def central_jobmon_static_port(tmpdir_factory):
    monpath = tmpdir_factory.mktemp("jmdir")
    jm = CentralJobMonitor(str(monpath), port=3459, publisher_port=5678,
                           publish_job_state=True, persistent=False)
    sleep(1)
    yield jm
    print("teardown fixture in {}".format(monpath))
    jm.stop_responder()
    sleep(1)
    assert not jm.responder_proc_is_alive()
