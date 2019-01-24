import glob
import getpass
import os
import os.path as path
import pytest

try:
    from jobmon.client.swarm.executors import sge_utils as sge
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

    # the path differs based on the cluster but all are in /bin/time
    # (some are in /usr/bin/time)
    assert "/bin/time" in sge.true_path(executable="time")


@pytest.mark.cluster
def test_session_creation():
    s = sge._drmaa_session()
    assert s is not None
    assert s.drmsInfo is not None


@pytest.mark.skip(reason="qsub and _wait_done aren't actually used anymore")
@pytest.mark.cluster
def test_basic_submit():
    true_id = sge.qsub("/bin/true", "so true", memory=0,
                       project='proj_jenkins')
    assert true_id

    mem_id = sge.qsub("/bin/true", "still true", slots=1, memory=1,
                      project='proj_jenkins')
    assert mem_id

    proj_id = sge.qsub("/bin/true", "still true", slots=1, memory=1,
                       project="proj_jenkins")
    fail_msg = ("Test failed: check that you have permission to run under "
                "'proj_jenkins' and that there are available jobs under this"
                " project")
    assert proj_id, fail_msg

    sleep_id = sge.qsub("/bin/sleep", "wh#@$@&(*!",
                        parameters=[50],
                        stdout="whatsleep.o$JOB_ID", project='proj_jenkins')
    wait_id_a = sge.qsub("/bin/true", "dsf897  ",
                         holds=[sleep_id], jobtype="plain",
                         project='proj_jenkins')
    assert wait_id_a
    wait_id_b = sge.qsub("/bin/true", "flou;nd  ",
                         holds=[sleep_id], project='proj_jenkins')
    assert wait_id_b
    assert sge._wait_done(
        [true_id, mem_id, proj_id, sleep_id, wait_id_a, wait_id_b])
    assert not glob.glob(os.path.expanduser("~/sotrue*{}".format(true_id)))
    assert glob.glob(os.path.expanduser("~/whatsleep*{}".format(sleep_id)))


@pytest.mark.cluster
def test_bad_arguments():
    with pytest.raises(ValueError) as exc_info:
        sge.qsub("/bin/true", "so true", memory=0, slots=0,
                 project='proj_jenkins')
    assert "slots must be greater than zero" in str(exc_info)

    with pytest.raises(ValueError) as exc_info:
        sge.qsub("/bin/true", "so true", memory=0, holds=[999],
                 hold_pattern="foo*", project='proj_jenkins')
    assert "Cannot have both" in str(exc_info)

    with pytest.raises(ValueError) as exc_info:
        sge.qsub("/bin/true", "so true", memory=0,
                 parameters="read this and die", project='proj_jenkins')
    assert "parameters" in str(exc_info)


def example_job_args(tag):
    for idx in range(4):
        yield [tag, idx]


@pytest.mark.skip(reason="qsub and _wait_done aren't actually used anymore")
@pytest.mark.cluster
def test_multi_submit():
    true_ids = sge.qsub("/bin/sleep", "howdy",
                        parameters=[[1], [2], [1]], project='proj_jenkins')
    assert len(true_ids) == 3

    gen_ids = sge.qsub("/bin/true", "generated",
                       parameters=example_job_args("today"),
                       project='proj_jenkins')
    assert len(gen_ids) == 4
    assert sge._wait_done(true_ids + gen_ids)


@pytest.mark.skip(reason="qsub and _wait_done aren't actually used anymore")
@pytest.mark.cluster
def test_path_manipulation():
    """
    Compares what happens when you specify a prepend_to_path versus
        when you don't. Look at the output files and diff them to see.
    """
    sort_a = sge.qsub(sge.true_path("tests/shellfiles/env_vars.sh"), "env-a",
                      slots=1, memory=0,
                      stdout=os.path.abspath("env-a.out"),
                      project='proj_jenkins')
    sort_b = sge.qsub(sge.true_path("tests/shellfiles/env_vars.sh"), "env-b",
                      jobtype="shell", slots=1, memory=0,
                      prepend_to_path="~/DonaldDuck",
                      stdout=os.path.abspath("env-b.out"),
                      project='proj_jenkins')
    assert sge._wait_done([sort_a, sort_b])
    assert "DonaldDuck" in open("env-b.out").read()


@pytest.mark.skip(reason="qsub and _wait_done aren't actually used anymore")
@pytest.mark.cluster
def test_python_submit():
    py_id = sge.qsub(sge.true_path("waiter.py"), ".hum",
                     parameters=[5], jobtype="python", project='proj_jenkins')
    assert py_id

    env_id = sge.qsub(sge.true_path("waiter.py"), "#bug",
                      parameters=[3], stderr="env_id.e$JOB_ID",
                      conda_env="fbd-0.1", project='proj_jenkins')
    assert env_id
    abs_id = sge.qsub(sge.true_path("waiter.py"), "#bug",
                      parameters=[3], stderr="abs_id.e$JOB_ID",
                      conda_env="/ihme/forecasting/envs/fbd-0.1",
                      project='proj_jenkins')
    assert abs_id
    dones = list()
    for idx, v in enumerate([py_id, env_id, abs_id]):
        if not sge._wait_done([v]):
            dones.append((idx, v))
    assert not dones


@pytest.mark.skip(reason="qsub and _wait_done aren't actually used anymore")
@pytest.mark.cluster
def test_r_submit():
    rscript_id = sge.qsub(sge.true_path("hi.R"), ".rstuff&*(",
                          parameters=[5], jobtype="R", project='proj_jenkins')
    notype_id = sge.qsub(sge.true_path("hi.R"), "##rb+@",
                         parameters=[3], project='proj_jenkins')
    assert sge._wait_done([rscript_id, notype_id])


@pytest.mark.skip(reason="qsub and _wait_done aren't actually used anymore")
@pytest.mark.cluster
def test_sh_submit():
    rscript_id = sge.qsub(sge.true_path("tests/shellfiles/env_vars.sh"),
                          ".rstuff&*(", parameters=[5], jobtype="shell",
                          project='proj_jenkins')
    notype_id = sge.qsub(sge.true_path("tests/shellfiles/env_vars.sh"),
                         "##rb+@", parameters=[3], project='proj_jenkins')
    assert sge._wait_done([rscript_id, notype_id])


@pytest.mark.skip(reason="qsub and _wait_done aren't actually used anymore")
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
                             stderr="err{}.txt".format(modify_idx),
                             project='proj_jenkins'))
    assert sge._wait_done(jobs)


@pytest.mark.skip(reason="qsub and _wait_done aren't actually used anymore")
@pytest.mark.cluster
def test_sh_wrap():
    sh0_id = sge.qsub(sge.true_path("waiter.py"),
                      shfile=sge.true_path("sample.sh"),
                      stdout="shellwaitPython.txt",
                      stderr="shellwaitererr.txt",
                      jobname="shellwaiter",
                      project='proj_jenkins')

    sh1_id = sge.qsub(sge.true_path("waiter.R"),
                      shfile=sge.true_path("sample.sh"),
                      stdout="shellwaitR.txt",
                      stderr="shellwaitererr.txt",
                      jobname="shellwaiter",
                      project='proj_jenkins')

    sh2_id = sge.qsub(sge.true_path("waiter.do"),
                      shfile=sge.true_path("sample.sh"),
                      stdout="shellwaitStata.txt",
                      stderr="shellwaitererr.txt",
                      jobname="shellwaiter",
                      project='proj_jenkins')
    assert sge._wait_done([sh0_id, sh1_id, sh2_id])


def test_convert_wallclock():
    wallclock_str = '10:11:50'
    res = sge.convert_wallclock_to_seconds(wallclock_str)
    assert res == 36710.0


def test_convert_wallclock_with_days():
    wallclock_str = '01:10:11:50'
    res = sge.convert_wallclock_to_seconds(wallclock_str)
    assert res == 123110.0


def test_convert_wallclock_with_milleseconds():
    wallclock_str = '01:10:11:50.15'
    res = sge.convert_wallclock_to_seconds(wallclock_str)
    assert res == 123110.15
