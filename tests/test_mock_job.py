import os
import pytest
import signal
import subprocess as sp
from tests.mock_job import MockJob


def test_good_job():
    job = MockJob("good dog", 1, None)
    assert (job.run())


def test_bad_job():
    with pytest.raises(Exception) as exc:
        job = MockJob("bad dog", 1, "chew slippers")
        job.run()
    assert ("chew slippers" in str(exc.value))


# Test this in a subprocess because and check that it deliberately kills
# python!
def test_dead_job():
    dir = os.path.dirname(os.path.realpath(__file__))
    return_code = sp.call(
        ["python", dir + "/mock_job.py", "dead dog", "2", MockJob.DIE_COMMAND])

    assert (return_code == -1 * signal.SIGKILL)
