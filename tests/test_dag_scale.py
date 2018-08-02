import pytest

from cluster_utils.io import makedirs_safely

from jobmon import sge
from .mock_sleep_and_write_task import SleepAndWriteFileMockTask


@pytest.mark.skip(reason="Too big to run by default, only run when "
                  "specifically requested")
def test_burdenator_scale(db_cfg, real_jsm_jqs, task_dag_manager, tmp_out_dir):
    """
    Create and execute  a big four-phase fork and join dag with expanding and
    contracting phase sizes.
    No "guard" tasks between phases, so they can just roll through:
     a[0..N]->b[0..3*N]->c[0..N]->d[0..N/10]

     Runtimes vary a little - uses modulo arithmetic (%) and relative primes
     for the jitter. Realtive primes should create "interesting" patterns of
     timing collisions.

     N is big, e.g. 10,000
    """

    N = 10000
    root_out_dir = "{}/mocks/test_burdenator_scale".format(tmp_out_dir)
    makedirs_safely(root_out_dir)
    dag = task_dag_manager.create_task_dag(name="test_burdenator_scale")
    command_script = sge.true_path("tests/remote_sleep_and_write.py")

    task_a = {}
    for i in range(N):
        output_file_name = "{}/a-{}.out".format(root_out_dir, i)
        task_a[i] = SleepAndWriteFileMockTask(
            command=("python {cs} --sleep_secs 1 --output_file_path {ofn} "
                     "--name {n}".format(cs=command_script,
                                         ofn=output_file_name,
                                         n=output_file_name)))
        dag.add_task(task_a[i])

    # The B's all have varying run times. One a-task fans out to 3 b-tasks
    task_b = {}
    for i in range(3 * N):
        sleep_secs = 5 + i % 7  # Add a bit of jitter to the runtimes
        output_file_name = "{}/b-{}.out".format(root_out_dir, i)
        task_b[i] = SleepAndWriteFileMockTask(
            command=("python {cs} --sleep_secs {ss} --output_file_path {ofn} "
                     "--name {n}".format(cs=command_script, ss=sleep_secs,
                                         ofn=output_file_name,
                                         n=output_file_name)),
            upstream_tasks=[task_a[i // 3]]  # // is truncating int division
        )
        dag.add_task(task_b[i])

    # Each c[i] depends on three b's

    task_c = {}
    for i in range(N):
        sleep_secs = 5 + i % 5,
        output_file_name = "{}/c-{}.out".format(root_out_dir, i)
        task_c[i] = SleepAndWriteFileMockTask(
            command=("python {cs} --sleep_secs {ss} --output_file_path {ofn} "
                     "--name {n}".format(cs=command_script, ss=sleep_secs,
                                         ofn=output_file_name,
                                         n=output_file_name)),
            upstream_tasks=[task_b[3 * i], task_b[3 * i + 1],
                            task_b[3 * i + 2]]
        )
        dag.add_task(task_c[i])

    task_d = {}
    for i in range(N // 10):
        sleep_secs = 3 + i % 7,
        output_file_name = "{}/d-{}.out".format(root_out_dir, i)
        task_d[i] = SleepAndWriteFileMockTask(
            command=("python {cs} --sleep_secs {ss} --output_file_path {ofn} "
                     "--name {n}".format(cs=command_script, ss=sleep_secs,
                                         ofn=output_file_name,
                                         n=output_file_name)),
            upstream_tasks=[task_c[i + j] for j in range(10)]
        )
        dag.add_task(task_d[i])

    logger.info("DAG: {}".format(dag))

    (rc, num_completed, num_failed) = dag._execute()

    assert rc
    assert num_completed == N + 3 * N + N + (N // 10)
    assert num_failed == 0
