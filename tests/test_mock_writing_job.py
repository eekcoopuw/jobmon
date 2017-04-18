import os
import pytest
import signal
import subprocess as sp
from tests.mock_writing_job import MockWritingJob
from tests.mock_job import MockJob


def test_good_job(tmp_out_dir):
    file_1 = create_test_file(tmp_out_dir, "good_file_1")
    file_2 = create_test_file(tmp_out_dir, "good_file_2")

    job = MockWritingJob("good dog", 1, None, [file_1, file_2])
    assert (job.run())
    assert os.access(file_1,os.R_OK)
    assert os.access(file_2,os.R_OK)


def test_bad_job(tmp_out_dir):
    file_1 = create_test_file(tmp_out_dir, "bad_file")
    with pytest.raises(Exception) as exc:
        job = MockWritingJob("bad dog", 1, "chew slippers",[file_1])
        job.run()
    assert ("chew slippers" in str(exc.value))
    assert not os.access(file_1,os.R_OK)


# Test this in a subprocess because and check that it deliberately kills
# python!
def test_dead_job(tmp_out_dir):
    file_1 = create_test_file(tmp_out_dir,"dead_file")
    dir = os.path.dirname(os.path.realpath(__file__))
    return_code = sp.call(
        ["python", dir + "/mock_writing_job.py", "dead dog", "2", MockJob.DIE_COMMAND])

    assert (return_code == -1 * signal.SIGKILL)
    assert not os.access(file_1, os.R_OK)


def create_test_file(tmp_out_dir, filename):
    file = "{}/{}.mock".format(tmp_out_dir, filename)
    try:
        os.remove(file)
    except FileNotFoundError:
        # not there, no worries
        pass
    return file
