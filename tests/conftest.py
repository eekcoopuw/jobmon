import pytest
from jobmon.central_job_monitor import CentralJobMonitor
from multiprocessing import Process
from time import sleep


def start_cjm(sqlite_dir):
    CentralJobMonitor(sqlite_dir)


@pytest.fixture(scope='function')
def central_jobmon(tmpdir_factory):
    path = tmpdir_factory.mktemp("jmdir")
    print(path)
    proc = Process(target=start_cjm, args=(str(path),))
    proc.start()
    sleep(1)
    yield str(path)
    print("teardown fixture in {}".format(path))
    proc.terminate()
