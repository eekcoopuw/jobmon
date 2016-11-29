"""
Tests of jobmon/sge.py
"""
import glob
import logging
import getpass
import os
import os.path as path
import pytest
import jobmon.sge as sge


LOGGER = logging.getLogger("test_sge")


def test_true_path():
    with pytest.raises(ValueError) as exc_info:
        sge.true_path()
    assert "cannot both" in str(exc_info)

    assert sge.true_path("") == os.getcwd()
    assert getpass.getuser() in sge.true_path("~/bin")
    assert sge.true_path("blah").endswith("/blah")
    assert sge.true_path(file_or_dir="../tests")==path.abspath(".")

    assert sge.true_path(executable="time")=="/usr/bin/time"


def test_session_creation():
    logging.basicConfig(level=logging.DEBUG)
    s = sge._drmaa_session()
    assert s is not None
    assert s.drmsInfo is not None
    LOGGER.debug(s.drmsInfo)


def test_basic_submit():
    logging.basicConfig(level=logging.DEBUG)
    true_id = sge.qsub("/bin/true", "so true", memory=0, job_type=None)
    assert true_id

    mem_id = sge.qsub("/bin/true", "still true", slots=1,
                      project="proj_forecasting", memory=1, job_type=None)
    assert mem_id

    sleep_id = sge.qsub("/bin/sleep", "wh#@$@&(*!",
                        parameters=[50], job_type=None,
                        stdout="whatsleep.o$JOB_ID")
    wait_id_a = sge.qsub("/bin/true", "dsf897  ",
                         holds=[sleep_id], job_type=None)
    assert wait_id_a
    wait_id_b = sge.qsub("/bin/true", "flou;nd  ",
                         holds=[sleep_id], job_type=None)
    assert wait_id_b
    assert sge._wait_done([true_id, mem_id, sleep_id, wait_id_a, wait_id_b])
    assert not glob.glob(os.path.expanduser("~/sotrue*{}".format(true_id)))
    assert glob.glob(os.path.expanduser("~/whatsleep*{}".format(sleep_id)))


def test_path_manipulation():
    """
    Compares what happens when you specify a prepend_to_path versus
        when you don't. Look at the output files and diff them to see.
    """
    logging.basicConfig(level=logging.DEBUG)
    sort_a = sge.qsub(sge.true_path("env_vars.sh"), "env-a",
                      job_type=None, slots=1, memory=0,
                      stdout=os.path.abspath("env-a.out"))
    sort_b = sge.qsub(sge.true_path("env_vars.sh"), "env-b",
                      job_type=None, slots=1, memory=0,
                      prepend_to_path="~/DonaldDuck",
                      stdout=os.path.abspath("env-b.out"))
    assert sge._wait_done([sort_a, sort_b])
    assert "DonaldDuck" in open("env-b.out").read()


def test_python_submit():
    logging.basicConfig(level=logging.DEBUG)
    py_id = sge.qsub(sge.true_path("waiter.py"), ".hum",
                     parameters=[5], job_type="python")
    assert py_id

    env_id = sge.qsub(sge.true_path("waiter.py"), "#bug",
                     parameters=[3], job_type="python",
                      conda_env="fbd-0.1")
    assert env_id
    abs_id = sge.qsub(sge.true_path("waiter.py"), "#bug",
                     parameters=[3], job_type="python",
                      conda_env="/ihme/forecasting/envs/fbd-0.1")
    assert abs_id
    assert sge._wait_done([py_id, env_id, abs_id])


def test_r_submit():
    logging.basicConfig(level=logging.DEBUG)
    rscript_id = sge.qsub(sge.true_path("hi.R"), ".rstuff&*(",
                     parameters=[5], job_type="R")
    assert sge._wait_done([rscript_id])
