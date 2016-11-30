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


@pytest.mark.slowtest
def test_basic_submit():
    logging.basicConfig(level=logging.DEBUG)
    true_id = sge.qsub("/bin/true", "so true", memory=0)
    assert true_id

    mem_id = sge.qsub("/bin/true", "still true", slots=1,
                      project="proj_forecasting", memory=1)
    assert mem_id

    sleep_id = sge.qsub("/bin/sleep", "wh#@$@&(*!",
                        parameters=[50],
                        stdout="whatsleep.o$JOB_ID")
    wait_id_a = sge.qsub("/bin/true", "dsf897  ",
                         holds=[sleep_id], jobtype="plain")
    assert wait_id_a
    wait_id_b = sge.qsub("/bin/true", "flou;nd  ",
                         holds=[sleep_id])
    assert wait_id_b
    assert sge._wait_done([true_id, mem_id, sleep_id, wait_id_a, wait_id_b])
    assert not glob.glob(os.path.expanduser("~/sotrue*{}".format(true_id)))
    assert glob.glob(os.path.expanduser("~/whatsleep*{}".format(sleep_id)))


def example_job_args(tag):
    for idx in range(4):
        yield [tag, idx]


@pytest.mark.slowtest
def test_multi_submit():
    logging.basicConfig(level=logging.DEBUG)
    true_ids = sge.qsub("/bin/sleep", "howdy",
                        parameters=[[1], [2], [1]])
    assert len(true_ids)==3

    gen_ids = sge.qsub("/bin/true", "generated",
                       parameters=example_job_args("today"))
    assert len(gen_ids)==4
    assert sge._wait_done(true_ids + gen_ids)


@pytest.mark.slowtest
def test_path_manipulation():
    """
    Compares what happens when you specify a prepend_to_path versus
        when you don't. Look at the output files and diff them to see.
    """
    logging.basicConfig(level=logging.DEBUG)
    sort_a = sge.qsub(sge.true_path("env_vars.sh"), "env-a",
                      slots=1, memory=0,
                      stdout=os.path.abspath("env-a.out"))
    sort_b = sge.qsub(sge.true_path("env_vars.sh"), "env-b",
                      jobtype="shell", slots=1, memory=0,
                      prepend_to_path="~/DonaldDuck",
                      stdout=os.path.abspath("env-b.out"))
    assert sge._wait_done([sort_a, sort_b])
    assert "DonaldDuck" in open("env-b.out").read()


@pytest.mark.slowtest
def test_python_submit():
    logging.basicConfig(level=logging.DEBUG)
    py_id = sge.qsub(sge.true_path("waiter.py"), ".hum",
                     parameters=[5], jobtype="python")
    assert py_id

    env_id = sge.qsub(sge.true_path("waiter.py"), "#bug",
                      parameters=[3],
                      conda_env="fbd-0.1")
    assert env_id
    abs_id = sge.qsub(sge.true_path("waiter.py"), "#bug",
                      parameters=[3],
                      conda_env="/ihme/forecasting/envs/fbd-0.1")
    assert abs_id
    assert sge._wait_done([py_id, env_id, abs_id])


@pytest.mark.slowtest
def test_r_submit():
    logging.basicConfig(level=logging.DEBUG)
    rscript_id = sge.qsub(sge.true_path("hi.R"), ".rstuff&*(",
                          parameters=[5], jobtype="R")
    notype_id = sge.qsub(sge.true_path("hi.R"), "##rb+@",
                          parameters=[3])
    assert sge._wait_done([rscript_id, notype_id])


@pytest.mark.slowtest
def test_sh_submit():
    logging.basicConfig(level=logging.DEBUG)
    rscript_id = sge.qsub(sge.true_path("env_vars.sh"), ".rstuff&*(",
                          parameters=[5], jobtype="shell")
    notype_id = sge.qsub(sge.true_path("env_vars.sh"), "##rb+@",
                          parameters=[3])
    assert sge._wait_done([rscript_id, notype_id])


@pytest.mark.slowtest
def test_sh_loop():
    """
    This demonstrates that when you submit a shell file DRMAA
    doesn't copy the shell file. It uses a reference to the
    file and runs from whatever file is currently on disk.
    """
    logging.basicConfig(level=logging.DEBUG)
    jobs = list()
    for modify_idx in range(5):
        with open("modout.sh", "w") as shell_file:
            shell_file.write("echo I am {}\n".format(modify_idx))
        jobs.append(sge.qsub(sge.true_path("modout.sh"), "modshell",
                             stdout="out{}.txt".format(modify_idx),
                             stderr="err{}.txt".format(modify_idx)))
    assert sge._wait_done(jobs)
