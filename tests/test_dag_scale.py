import logging
import pytest

from cluster_utils.io import makedirs_safely

from .mock_sleep_and_write_task import SleepAndWriteFileMockTask

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@pytest.mark.skip(reason="Too big to run by default, only run shen specifically requested")
def test_burdenator_scale(db_cfg, jsm_jqs, job_dag_manager, tmp_out_dir):
    """
    Create and execute  a big four-phase fork and join dag with expanding and contracting phase sizes.
    No "guard" tasks between phases, so they can just roll through:
     a[0..N]->b[0..3*N]->c[0..N]->d[0..N/10]

     Runtimes vary a little - uses modulo arithmetic (%) and relative primes for the jitter. Realtive primes should
     create "interesting" patterns of timing collisions.

     N is big, e.g. 10,000
    """

    N = 10000
    root_out_dir = "{}/mocks/test_burdenator_scaleC".format(tmp_out_dir)
    makedirs_safely(root_out_dir)
    dag = job_dag_manager.create_job_dag(name="test_burdenator_scale")

    task_a = {}
    for i in range(N):
        task_a[i] = SleepAndWriteFileMockTask(
            sleep_secs=1,
            output_file_name="{}/a-{}.out".format(root_out_dir, i)
        )
        dag.add_task(task_a[i])

    # The B's all have varying run times. One a-task fans out to 3 b-tasks
    task_b = {}
    for i in range(3 * N):
        task_b[i] = SleepAndWriteFileMockTask(
            sleep_secs=5 + i % 7,  # Add a bit of jitter to the runtimes
            output_file_name="{}/b-{}.out".format(root_out_dir, i),
            upstream_tasks=[task_a[i // 3]]  # // is truncating integer division
        )
        dag.add_task(task_b[i])

    # Each c[i] depends on three b's

    task_c = {}
    for i in range(N):
        task_c[i] = SleepAndWriteFileMockTask(
            sleep_secs=5 + i % 5,
            output_file_name="{}/c-{}.out".format(root_out_dir, i),
            upstream_tasks=[task_b[3 * i], task_b[3 * i + 1], task_b[3 * i + 2]]
        )
        dag.add_task(task_c[i])

    task_d = {}
    for i in range(N // 10):
        task_d[i] = SleepAndWriteFileMockTask(
            sleep_secs=3 + i % 7,
            output_file_name="{}/d-{}.out".format(root_out_dir, i),
            upstream_tasks=[task_c[i + j] for j in range(10)]
        )
        dag.add_task(task_d[i])

    logger.info("DAG: {}".format(dag))

    (rc, num_completed, num_failed) = dag.execute()

    assert rc
    assert num_completed == N + 3 * N + N + (N // 10)
    assert num_failed == 0
