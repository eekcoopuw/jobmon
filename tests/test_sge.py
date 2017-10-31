"""
Tests of jobmon/sge.py
"""
import glob
import getpass
import os
import os.path as path
import pytest

try:
    from jobmon import sge
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

    assert sge.true_path(executable="time") == "/usr/bin/time"


@pytest.mark.cluster
def test_session_creation():
    s = sge._drmaa_session()
    assert s is not None
    assert s.drmsInfo is not None


@pytest.mark.cluster
def test_basic_submit():
    true_id = sge.qsub("/bin/true", "so true", memory=0)
    assert true_id

    mem_id = sge.qsub("/bin/true", "still true", slots=1, memory=1)
    assert mem_id

    proj_id = sge.qsub("/bin/true", "still true", slots=1, memory=1,
                       project="proj_qlogins")
    fail_msg = ("Test failed: check that you have permission to run under "
                "'proj_qlogins' and that there are available jobs under this"
                " project")
    assert proj_id, fail_msg

    sleep_id = sge.qsub("/bin/sleep", "wh#@$@&(*!",
                        parameters=[50],
                        stdout="whatsleep.o$JOB_ID")
    wait_id_a = sge.qsub("/bin/true", "dsf897  ",
                         holds=[sleep_id], jobtype="plain")
    assert wait_id_a
    wait_id_b = sge.qsub("/bin/true", "flou;nd  ",
                         holds=[sleep_id])
    assert wait_id_b
    assert sge._wait_done(
        [true_id, mem_id, proj_id, sleep_id, wait_id_a, wait_id_b])
    assert not glob.glob(os.path.expanduser("~/sotrue*{}".format(true_id)))
    assert glob.glob(os.path.expanduser("~/whatsleep*{}".format(sleep_id)))


@pytest.mark.cluster
def test_bad_arguments():
    with pytest.raises(ValueError) as exc_info:
        _ = sge.qsub("/bin/true", "so true", memory=0, slots=0)
    assert "slots must be greater than zero" in str(exc_info)

    with pytest.raises(ValueError) as exc_info:
        _ = sge.qsub("/bin/true", "so true", memory=0, holds=[999], hold_pattern="foo*")
    assert "Cannot have both" in str(exc_info)

    with pytest.raises(ValueError) as exc_info:
        _ = sge.qsub("/bin/true", "so true", memory=0, parameters="read this and die")
    assert "parameters" in str(exc_info)


def example_job_args(tag):
    for idx in range(4):
        yield [tag, idx]


@pytest.mark.cluster
def test_multi_submit():
    true_ids = sge.qsub("/bin/sleep", "howdy",
                        parameters=[[1], [2], [1]])
    assert len(true_ids) == 3

    gen_ids = sge.qsub("/bin/true", "generated",
                       parameters=example_job_args("today"))
    assert len(gen_ids) == 4
    assert sge._wait_done(true_ids + gen_ids)


@pytest.mark.cluster
def test_path_manipulation():
    """
    Compares what happens when you specify a prepend_to_path versus
        when you don't. Look at the output files and diff them to see.
    """
    sort_a = sge.qsub(sge.true_path("tests/shellfiles/env_vars.sh"), "env-a",
                      slots=1, memory=0,
                      stdout=os.path.abspath("env-a.out"))
    sort_b = sge.qsub(sge.true_path("tests/shellfiles/env_vars.sh"), "env-b",
                      jobtype="shell", slots=1, memory=0,
                      prepend_to_path="~/DonaldDuck",
                      stdout=os.path.abspath("env-b.out"))
    assert sge._wait_done([sort_a, sort_b])
    assert "DonaldDuck" in open("env-b.out").read()


@pytest.mark.cluster
def test_python_submit():
    py_id = sge.qsub(sge.true_path("waiter.py"), ".hum",
                     parameters=[5], jobtype="python")
    assert py_id

    env_id = sge.qsub(sge.true_path("waiter.py"), "#bug",
                      parameters=[3], stderr="env_id.e$JOB_ID",
                      conda_env="fbd-0.1")
    assert env_id
    abs_id = sge.qsub(sge.true_path("waiter.py"), "#bug",
                      parameters=[3], stderr="abs_id.e$JOB_ID",
                      conda_env="/ihme/forecasting/envs/fbd-0.1")
    assert abs_id
    dones = list()
    for idx, v in enumerate([py_id, env_id, abs_id]):
        if not sge._wait_done([v]):
            dones.append((idx, v))
    assert not dones


@pytest.mark.cluster
def test_r_submit():
    rscript_id = sge.qsub(sge.true_path("hi.R"), ".rstuff&*(",
                          parameters=[5], jobtype="R")
    notype_id = sge.qsub(sge.true_path("hi.R"), "##rb+@",
                         parameters=[3])
    assert sge._wait_done([rscript_id, notype_id])


@pytest.mark.cluster
def test_sh_submit():
    rscript_id = sge.qsub(sge.true_path("tests/shellfiles/env_vars.sh"),
                          ".rstuff&*(", parameters=[5], jobtype="shell")
    notype_id = sge.qsub(sge.true_path("tests/shellfiles/env_vars.sh"),
                         "##rb+@", parameters=[3])
    assert sge._wait_done([rscript_id, notype_id])


@pytest.mark.cluster
def test_sh_loop():
    """
    This demonstrates that when you submit a shell file DRMAA
    doesn't copy the shell file. It uses a reference to the
    file and runs from whatever file is currently on disk.
    """
    jobs = list()
    for modify_idx in range(5):
        with open("modout.sh", "w") as shell_file:
            shell_file.write("echo I am {}\n".format(modify_idx))
        jobs.append(sge.qsub(sge.true_path("modout.sh"), "modshell",
                             stdout="out{}.txt".format(modify_idx),
                             stderr="err{}.txt".format(modify_idx)))
    assert sge._wait_done(jobs)


@pytest.mark.cluster
def test_sh_wrap():
    sh0_id = sge.qsub(sge.true_path("waiter.py"),
                      shfile=sge.true_path("sample.sh"),
                      stdout="shellwaitPython.txt",
                      stderr="shellwaitererr.txt",
                      jobname="shellwaiter")
    sh1_id = sge.qsub(sge.true_path("waiter.R"),
                      shfile=sge.true_path("sample.sh"),
                      stdout="shellwaitR.txt",
                      stderr="shellwaitererr.txt",
                      jobname="shellwaiter")
    sh2_id = sge.qsub(sge.true_path("waiter.do"),
                      shfile=sge.true_path("sample.sh"),
                      stdout="shellwaitStata.txt",
                      stderr="shellwaitererr.txt",
                      jobname="shellwaiter")
    assert sge._wait_done([sh0_id, sh1_id, sh2_id])
