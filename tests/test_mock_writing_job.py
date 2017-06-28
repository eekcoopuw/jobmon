import os
import signal
import subprocess as sp

import pytest
from jobmon.mocks import mock_job
from jobmon.mocks.mock_job import MockJob
from jobmon.mocks.mock_writing_job import MockWritingJob


def test_good_job(tmp_out_dir):
    file_1 = create_test_file(tmp_out_dir, "good_file_1")
    file_2 = create_test_file(tmp_out_dir, "good_file_2")

    contents = "stuff in file"

    job = MockWritingJob("good dog", 1, None, [file_1, file_2], contents)
    assert (job.run())
    assert os.access(file_1, os.R_OK)
    assert _read_file(file_1) == contents

    assert os.access(file_2, os.R_OK)
    assert _read_file(file_2) == contents


def _read_file(name):
    f = open(name, "r")
    s = f.read()
    f.close()
    return s


def test_bad_job(tmp_out_dir):
    file_1 = create_test_file(tmp_out_dir, "bad_file")
    with pytest.raises(Exception) as exc:
        job = MockWritingJob("bad dog", 1, "chew slippers", [file_1])
        job.run()
    assert ("chew slippers" in str(exc.value))
    assert not os.access(file_1, os.R_OK)


# Test this in a subprocess because and check that it deliberately kills python!
def test_dead_job(tmp_out_dir):
    file_1 = create_test_file(tmp_out_dir, "dead_file")
    runfile = os.path.realpath(mock_job.__file__)
    return_code = sp.call(
        ["python", runfile, "dead dog", "2", MockJob.DIE_COMMAND])

    assert (return_code == -1 * signal.SIGKILL)
    assert not os.access(file_1, os.R_OK)


def create_test_file(tmp_out_dir, filename):
    file = "{}/{}.mock".format(tmp_out_dir, filename)
    try:
        os.remove(file)
    except:
        # not there, no worries
        pass
    return file
