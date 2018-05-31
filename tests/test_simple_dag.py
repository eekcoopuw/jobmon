import logging
import os
import pytest
from subprocess import check_output
from time import sleep

from cluster_utils.io import makedirs_safely

from jobmon import sge
from jobmon.session_scope import session_scope
from jobmon.models import Job, JobStatus, JobInstance, JobInstanceStatus
from jobmon.meta_models.task_dag import TaskDagMeta
from .mock_sleep_and_write_task import SleepAndWriteFileMockTask

logger = logging.getLogger(__name__)

# All Tests are written from the point of view of the Swarm, i.e the job
# controller in the application.  These are all "In Memory" tests - create all
# objects in memory and test that they work. The objects are created in the
# database, but testing that round-trip is not an explicit goal of these tests.

# These tests all use SleepAndWriteFileMockTask (which calls
# remote_sleep_and_write remotely)


def sge_submit_cmd_contains(jid, text):
    # Try this a couple of times... SGE is weird
    retries = 5
    while retries > 0:
        try:
            cmd = check_output(
                "qacct -j {} | grep submit_cmd".format(jid),
                shell=True).decode()
            break
        except Exception as e:
            print(e)
            try:
                cmd = check_output(
                    "qacct -j {} | grep submit_cmd".format(jid),
                    shell=True).decode()
                break
            except Exception as e:
                print(e)
            sleep(5 - retries)
            retries = retries - 1
            if retries == 0:
                raise RuntimeError("Attempted to use qacct to get command for "
                                   "jid {}. Giving up after 5 "
                                   "retries".format(jid))
    return text in cmd


def test_empty_dag(dag):
    """
    Create a dag with no Tasks. Call all the creation methods and check that it
    raises no Exceptions.
    """
    assert dag.name == "test_empty_dag"
    dag._execute()

    (rc, num_completed, num_previously_complete, num_failed) = dag._execute()

    assert rc
    assert num_previously_complete == 0
    assert num_completed == 0
    assert num_failed == 0


def test_one_task(tmp_out_dir, dag):
    """
    Create a dag with one Task and execute it
    """
    root_out_dir = "{}/mocks/test_one_task".format(tmp_out_dir)
    makedirs_safely(root_out_dir)
    command_script = sge.true_path("tests/remote_sleep_and_write.py")

    output_file_name = "{}/test_one_task/mock.out".format(tmp_out_dir)
    task = SleepAndWriteFileMockTask(
        command=("python {cs} --sleep_secs 1 --output_file_path {ofn} "
                 "--name {n}" .format(cs=command_script, ofn=output_file_name,
                                      n=output_file_name)))
    dag.add_task(task)
    os.makedirs("{}/test_one_task".format(tmp_out_dir))
    (rc, num_completed, num_previously_complete, num_failed) = dag._execute()

    assert rc
    assert num_completed == 1
    assert num_previously_complete == 0
    assert num_failed == 0
    assert task.status == JobStatus.DONE


def test_two_tasks_same_name_errors(tmp_out_dir, dag):
    """
    Create a dag with two Tasks, with the second task having the same hash_name
    as the first. Make sure that, upon adding the second task to the dag,
    TaskDag raises a ValueError
    """
    root_out_dir = "{}/mocks/test_two_tasks_same_name".format(tmp_out_dir)
    makedirs_safely(root_out_dir)
    command_script = sge.true_path("tests/remote_sleep_and_write.py")

    output_file_name = "{}/test_two_tasks_same_name/a.out".format(tmp_out_dir)
    task_a = SleepAndWriteFileMockTask(
        command=("python {cs} --sleep_secs 1 --output_file_path {ofn} "
                 "--name {n}".format(cs=command_script, ofn=output_file_name,
                                     n=output_file_name)))
    dag.add_task(task_a)

    task_a_again = SleepAndWriteFileMockTask(
        command=("python {cs} --sleep_secs 1 --output_file_path {ofn} "
                 "--name {n}".format(cs=command_script, ofn=output_file_name,
                                     n=output_file_name)))

    with pytest.raises(ValueError):
        dag.add_task(task_a_again)


def test_three_linear_tasks(tmp_out_dir, dag):
    """
    Create and execute a dag with three Tasks, one after another: a->b->c
    """
    root_out_dir = "{}/mocks/test_three_linear_tasks".format(tmp_out_dir)
    makedirs_safely(root_out_dir)
    command_script = sge.true_path("tests/remote_sleep_and_write.py")

    a_output_file_name = "{}/a.out".format(root_out_dir)
    task_a = SleepAndWriteFileMockTask(
        command=("python {cs} --sleep_secs 1 --output_file_path {ofn} "
                 "--name {n}".format(cs=command_script, ofn=a_output_file_name,
                                     n=a_output_file_name)),
        upstream_tasks=[]  # To be clear
    )
    dag.add_task(task_a)

    b_output_file_name = "{}/b.out".format(root_out_dir)
    task_b = SleepAndWriteFileMockTask(
        command=("python {cs} --sleep_secs 1 --output_file_path {ofn} "
                 "--name {n}".format(cs=command_script, ofn=b_output_file_name,
                                     n=b_output_file_name)),
        upstream_tasks=[task_a]
    )
    dag.add_task(task_b)

    c_output_file_name = "{}/c.out".format(root_out_dir)
    task_c = SleepAndWriteFileMockTask(
        command=("python {cs} --sleep_secs 1 --output_file_path {ofn} "
                 "--name {n}".format(cs=command_script, ofn=c_output_file_name,
                                     n=c_output_file_name)),
    )
    dag.add_task(task_c)
    task_c.add_upstream(task_b)  # Exercise add_upstream post-instantiation

    logger.debug("DAG: {}".format(dag))
    (rc, num_completed, num_previously_complete, num_failed) = dag._execute()
    assert rc
    assert num_completed == 3
    assert num_previously_complete == 0
    assert num_failed == 0
    assert dag.top_fringe == [task_a]

    all([task_a, task_b, task_c])

    # TBD validation


def test_fork_and_join_tasks(tmp_out_dir, dag):
    """
    Create a small fork and join dag with four phases:
     a->b[0..2]->c[0..2]->d
     and execute it
    """
    root_out_dir = "{}/mocks/test_fork_and_join_tasks".format(tmp_out_dir)
    makedirs_safely(root_out_dir)
    command_script = sge.true_path("tests/remote_sleep_and_write.py")

    output_file_name = "{}/a.out".format(root_out_dir)
    task_a = SleepAndWriteFileMockTask(
        command=("python {cs} --sleep_secs 1 --output_file_path {ofn} "
                 "--name {n}".format(cs=command_script, ofn=output_file_name,
                                     n=output_file_name))
    )
    dag.add_task(task_a)

    # The B's all have varying runtimes,
    task_b = {}
    for i in range(3):
        sleep_secs = 5 + i
        output_file_name = "{}/b-{}.out".format(root_out_dir, i)
        task_b[i] = SleepAndWriteFileMockTask(
            command=("python {cs} --sleep_secs {ss} --output_file_path {ofn} "
                     "--name {n}".format(cs=command_script, ss=sleep_secs,
                                         ofn=output_file_name,
                                         n=output_file_name)),
            upstream_tasks=[task_a]
        )
        dag.add_task(task_b[i])

    # Each c[i] depends exactly and only on b[i]
    # The c[i] runtimes invert the b's runtimes, hoping to smoke-out any race
    # conditions by creating a collision near d
    task_c = {}
    for i in range(3):
        sleep_secs = 5 - i
        output_file_name = "{}/c-{}.out".format(root_out_dir, i)
        task_c[i] = SleepAndWriteFileMockTask(
            command=("python {cs} --sleep_secs {ss} --output_file_path {ofn} "
                     "--name {n}".format(cs=command_script, ss=sleep_secs,
                                         ofn=output_file_name,
                                         n=output_file_name)),
            upstream_tasks=[task_b[i]]
        )
        dag.add_task(task_c[i])

    sleep_secs = 3
    output_file_name = "{}/d.out".format(root_out_dir)
    task_d = SleepAndWriteFileMockTask(
        command=("python {cs} --sleep_secs {ss} --output_file_path {ofn} "
                 "--name {n}".format(cs=command_script, ss=sleep_secs,
                                     ofn=output_file_name,
                                     n=output_file_name)),
        upstream_tasks=[task_c[i] for i in range(3)]
    )
    dag.add_task(task_d)

    logger.info("DAG: {}".format(dag))

    (rc, num_completed, num_previously_complete, num_failed) = dag._execute()

    assert rc
    assert num_completed == 1 + 3 + 3 + 1
    assert num_previously_complete == 0
    assert num_failed == 0

    assert task_a.status == JobStatus.DONE

    assert task_b[0].status == JobStatus.DONE
    assert task_b[1].status == JobStatus.DONE
    assert task_b[2].status == JobStatus.DONE

    assert task_c[0].status == JobStatus.DONE
    assert task_c[1].status == JobStatus.DONE
    assert task_c[2].status == JobStatus.DONE

    assert task_d.status == JobStatus.DONE


def test_fork_and_join_tasks_with_fatal_error(tmp_out_dir, dag):
    """
    Create the same small fork and join dag.
    One of the b-tasks (#1) fails consistently, so c[1] will never be ready.
    """
    root_out_dir = ("{}/mocks/test_fork_and_join_tasks_with_fatal_error"
                    .format(tmp_out_dir))
    makedirs_safely(root_out_dir)
    command_script = sge.true_path("tests/remote_sleep_and_write.py")

    output_file_name = "{}/a.out".format(root_out_dir)
    task_a = SleepAndWriteFileMockTask(
        command=("python {cs} --sleep_secs 1 --output_file_path {ofn} "
                 "--name {n}".format(cs=command_script, ofn=output_file_name,
                                     n=output_file_name))
    )
    dag.add_task(task_a)

    task_b = {}
    for i in range(3):
        output_file_name = "{}/b-{}.out".format(root_out_dir, i)
        # task b[1] will fail always
        task_b[i] = SleepAndWriteFileMockTask(
            command=("python {cs} --sleep_secs 1 --output_file_path {ofn} "
                     "--name {n}".format(cs=command_script,
                                         ofn=output_file_name,
                                         n=output_file_name)),
            upstream_tasks=[task_a],
            fail_always=(i == 1)
        )
        dag.add_task(task_b[i])

    task_c = {}
    for i in range(3):
        output_file_name = "{}/c-{}.out".format(root_out_dir, i)
        task_c[i] = SleepAndWriteFileMockTask(
            command=("python {cs} --sleep_secs 1 --output_file_path {ofn} "
                     "--name {n}".format(cs=command_script,
                                         ofn=output_file_name,
                                         n=output_file_name)),
            upstream_tasks=[task_b[i]]
        )
        dag.add_task(task_c[i])

    output_file_name = "{}/d.out".format(root_out_dir)
    task_d = SleepAndWriteFileMockTask(
        command=("python {cs} --sleep_secs 1 --output_file_path {ofn} "
                 "--name {n}".format(cs=command_script, ofn=output_file_name,
                                     n=output_file_name)),
        upstream_tasks=[task_c[i] for i in range(3)]
    )
    dag.add_task(task_d)

    logger.info("DAG: {}".format(dag))

    (rc, num_completed, num_previously_complete, num_failed) = dag._execute()

    assert not rc
    # a, b[0], b[2], c[0], c[2],  but not b[1], c[1], d
    assert num_completed == 1 + 2 + 2
    assert num_previously_complete == 0
    assert num_failed == 1  # b[1]

    assert task_a.status == JobStatus.DONE

    assert task_b[0].status == JobStatus.DONE
    assert task_b[1].status == JobStatus.ERROR_FATAL
    assert task_b[2].status == JobStatus.DONE

    assert task_c[0].status == JobStatus.DONE
    assert task_c[1].status == JobStatus.REGISTERED
    assert task_c[2].status == JobStatus.DONE

    assert task_d.status == JobStatus.REGISTERED


def test_fork_and_join_tasks_with_retryable_error(tmp_out_dir, dag):
    """
    Create the same fork and join dag with three Tasks a->b[0..3]->c and
    execute it.
    One of the b-tasks fails once, so the retry handler should cover that, and
    the whole DAG should complete
    """
    root_out_dir = ("{}/mocks/test_fork_and_join_tasks_with_retryable_error"
                    .format(tmp_out_dir))
    makedirs_safely(root_out_dir)
    command_script = sge.true_path("tests/remote_sleep_and_write.py")

    output_file_name = "{}/a.out".format(root_out_dir)
    task_a = SleepAndWriteFileMockTask(
        command=("python {cs} --sleep_secs 1 --output_file_path {ofn} "
                 "--name {n}".format(cs=command_script, ofn=output_file_name,
                                     n=output_file_name))
    )
    dag.add_task(task_a)

    task_b = {}
    for i in range(3):
        output_file_name = "{}/b-{}.out".format(root_out_dir, i)
        # task b[1] will fail
        task_b[i] = SleepAndWriteFileMockTask(
            command=("python {cs} --sleep_secs 1 --output_file_path {ofn} "
                     "--name {n}".format(cs=command_script,
                                         ofn=output_file_name,
                                         n=output_file_name)),
            upstream_tasks=[task_a],
            fail_count=1 if (i == 1) else 0
        )
        dag.add_task(task_b[i])

    task_c = {}
    for i in range(3):
        output_file_name = "{}/c-{}.out".format(root_out_dir, i)
        task_c[i] = SleepAndWriteFileMockTask(
            command=("python {cs} --sleep_secs 1 --output_file_path {ofn} "
                     "--name {n}".format(cs=command_script,
                                         ofn=output_file_name,
                                         n=output_file_name)),
            upstream_tasks=[task_b[i]]
        )
        dag.add_task(task_c[i])

    output_file_name = "{}/d.out".format(root_out_dir)
    task_d = SleepAndWriteFileMockTask(
        command=("python {cs} --sleep_secs 1 --output_file_path {ofn} "
                 "--name {n}".format(cs=command_script, ofn=output_file_name,
                                     n=output_file_name)),
        upstream_tasks=[task_c[i] for i in range(3)],
        fail_count=2
    )
    dag.add_task(task_d)

    logger.info("DAG: {}".format(dag))

    (rc, num_completed, num_previously_complete, num_failed) = dag._execute()

    assert rc
    assert num_completed == 1 + 3 + 3 + 1
    assert num_previously_complete == 0
    assert num_failed == 0

    assert task_a.status == JobStatus.DONE

    assert task_b[0].status == JobStatus.DONE
    assert task_b[1].status == JobStatus.DONE
    assert task_b[2].status == JobStatus.DONE

    assert task_c[0].status == JobStatus.DONE
    assert task_c[1].status == JobStatus.DONE
    assert task_c[2].status == JobStatus.DONE

    assert task_d.status == JobStatus.DONE

    # Check that the failed task's nodename + pgid got propagated to
    # its retry instance
    with session_scope() as session:
        job = session.query(Job).filter_by(job_id=task_b[1].job_id).first()
        done_ji = [ji for ji in job.job_instances
                   if ji.status == JobInstanceStatus.DONE][0]
        err_ji = [ji for ji in job.job_instances
                   if ji.status == JobInstanceStatus.ERROR][0]

        err_nodename = err_ji.nodename
        err_pgid = err_ji.process_group_id
        done_sge_id = done_ji.executor_id
    kill_nn_text = "--last_nodename {}".format(err_nodename)
    kill_pgid_text = "--last_pgid {}".format(err_pgid)
    assert sge_submit_cmd_contains(done_sge_id, kill_nn_text)
    assert sge_submit_cmd_contains(done_sge_id, kill_pgid_text)


def test_bushy_dag(tmp_out_dir, dag):
    """
    Similar to the a small fork and join dag but with connections between early
    and late phases:
       a->b[0..2]->c[0..2]->d
    And also:
       c depends on a
       d depends on b
    """
    root_out_dir = "{}/mocks/test_fork_and_join_tasks".format(tmp_out_dir)
    makedirs_safely(root_out_dir)
    command_script = sge.true_path("tests/remote_sleep_and_write.py")

    output_file_name = "{}/a.out".format(root_out_dir)
    task_a = SleepAndWriteFileMockTask(
        command=("python {cs} --sleep_secs 1 --output_file_path {ofn} "
                 "--name {n}".format(cs=command_script, ofn=output_file_name,
                                     n=output_file_name)))
    dag.add_task(task_a)

    # The B's all have varying runtimes,
    task_b = {}
    for i in range(3):
        sleep_secs = 5 + i
        output_file_name = "{}/b-{}.out".format(root_out_dir, i)
        task_b[i] = SleepAndWriteFileMockTask(
            command=("python {cs} --sleep_secs {ss} --output_file_path {ofn} "
                     "--name {n}".format(cs=command_script, ss=sleep_secs,
                                         ofn=output_file_name,
                                         n=output_file_name)),
            upstream_tasks=[task_a]
        )
        dag.add_task(task_b[i])

    # Each c[i] depends exactly and only on b[i]
    # The c[i] runtimes invert the b's runtimes, hoping to smoke-out any race
    # conditions by creating a collision near d
    task_c = {}
    for i in range(3):
        sleep_secs = 5 - i
        output_file_name = "{}/c-{}.out".format(root_out_dir, i)
        task_c[i] = SleepAndWriteFileMockTask(
            command=("python {cs} --sleep_secs {ss} --output_file_path {ofn} "
                     "--name {n}".format(cs=command_script, ss=sleep_secs,
                                         ofn=output_file_name,
                                         n=output_file_name)),
            upstream_tasks=[task_b[i], task_a]
        )
        dag.add_task(task_c[i])

    b_and_c = [task_b[i] for i in range(3)]
    b_and_c += [task_c[i] for i in range(3)]
    sleep_secs = 3
    output_file_name = "{}/d.out".format(root_out_dir)
    task_d = SleepAndWriteFileMockTask(
        command=("python {cs} --sleep_secs {ss} --output_file_path {ofn} "
                 "--name {n}".format(cs=command_script, ss=sleep_secs,
                                     ofn=output_file_name,
                                     n=output_file_name)),
        upstream_tasks=b_and_c
    )
    dag.add_task(task_d)

    logger.info("DAG: {}".format(dag))

    (rc, num_completed, num_previously_complete, num_failed) = dag._execute()

    # TODO: How to check that nothing was started before its upstream were
    # done?
    # Could we read database? Unfortunately not - submitted_date is initial
    # creation, not qsub status_date is date of last change.
    # Could we listen to job-instance state transitions?

    assert rc
    assert num_completed == 1 + 3 + 3 + 1
    assert num_previously_complete == 0
    assert num_failed == 0

    assert task_a.status == JobStatus.DONE

    assert task_b[0].status == JobStatus.DONE
    assert task_b[1].status == JobStatus.DONE
    assert task_b[2].status == JobStatus.DONE

    assert task_c[0].status == JobStatus.DONE
    assert task_c[1].status == JobStatus.DONE
    assert task_c[2].status == JobStatus.DONE

    assert task_d.status == JobStatus.DONE


def test_dag_logging(tmp_out_dir, dag):
    """
    Create a dag with one Task and execute it, and make sure logs show up in db

    This is in a separate test from the jsm-specifc logging test, as this test
    runs the jobmon pipeline as it would be run from the client perspective,
    and makes sure the qstat usage details are automatically updated in the db,
    as well as the created_date for the dag
    """
    root_out_dir = "{}/mocks/test_dag_logging".format(tmp_out_dir)
    makedirs_safely(root_out_dir)
    command_script = sge.true_path("tests/remote_sleep_and_write.py")

    output_file_name = "{}/test_dag_logging/mock.out".format(tmp_out_dir)
    task = SleepAndWriteFileMockTask(
        command=("python {cs} --sleep_secs 1 --output_file_path {ofn} "
                 "--name {n}" .format(cs=command_script, ofn=output_file_name,
                                      n=output_file_name)))
    dag.add_task(task)
    os.makedirs("{}/test_dag_logging".format(tmp_out_dir))
    (rc, num_completed, num_previously_complete, num_failed) = dag._execute()

    with session_scope() as session:
        ji = session.query(JobInstance).first()
        assert ji.usage_str  # all these should exist and not be empty
        assert ji.maxvmem
        assert ji.cpu
        assert ji.io
        assert ji.nodename
        assert ':' not in ji.wallclock  # wallclock should be in seconds

        td = session.query(TaskDagMeta).first()
        print(td.created_date)
        assert td.created_date  # this should not be empty
