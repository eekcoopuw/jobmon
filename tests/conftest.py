import pytest
from jobmon.central_job_monitor import CentralJobMonitor
from time import sleep


@pytest.fixture(scope='function')
def central_jobmon(tmpdir_factory):
    monpath = tmpdir_factory.mktemp("jmdir")
    jm = CentralJobMonitor(str(monpath))
    sleep(1)
    yield jm
    print("teardown fixture in {}".format(monpath))
    jm.stop_responder()
    sleep(1)
    assert not jm.responder_proc_is_alive()
