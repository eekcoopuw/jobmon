import os
import pytest
from time import sleep

from jobmon.client import client_config
from jobmon.client.swarm.executors import sge_utils as sge
from jobmon.client.swarm.executors.dummy import DummyExecutor
from jobmon.client.swarm.executors.sge import SGEExecutor
from jobmon.client.swarm.job_management.job_list_manager import JobListManager
from jobmon.client.swarm.job_management.executor_job import ExecutorJob
from jobmon.client.swarm.workflow.executable_task import ExecutableTask as Task
from jobmon.client.swarm.workflow.bash_task import BashTask

from tests.conftest import teardown_db
from tests.timeout_and_skip import timeout_and_skip
from functools import partial

path_to_file = os.path.dirname(__file__)


@pytest.fixture(scope='function')
def job_list_manager_dummy(real_dag_id):
    # We don't want this queueing jobs in conflict with the SGE daemons...
    # but we do need it to subscribe to status updates for reconciliation
    # tests. Start this thread manually.
    old_heartbeat_interval = client_config.heartbeat_interval
    client_config.heartbeat_interval = 5
    executor = DummyExecutor()
    jlm = JobListManager(real_dag_id, executor=executor,
                         start_daemons=False)
    yield jlm
    jlm.disconnect()
    client_config.heartbeat_interval = old_heartbeat_interval


@pytest.fixture(scope='function')
def job_list_manager_reconciliation(real_dag_id):
    executor = SGEExecutor(project="proj_tools")
    jlm = JobListManager(real_dag_id, executor=executor,
                         start_daemons=True)
    yield jlm
    jlm.disconnect()


def test_reconciler_dummy(db_cfg, job_list_manager_dummy):
    """Creates a job instance, gets an executor id so it can be in submitted
    to the batch executor state, and then it will never be run (it will miss
    its report by date and the reconciler will kill it)"""
    # Queue a job
    task = Task(command="ls", num_cores="1", name="dummyfbb", max_attempts=1)
    job = job_list_manager_dummy.bind_task(task)
    job_list_manager_dummy.adjust_resources_and_queue(job)
    jif = job_list_manager_dummy.job_instance_factory
    jir = job_list_manager_dummy.job_inst_reconciler

    # How long we wait for a JI to report it is running before reconciler moves
    # it to error state.
    jif.next_report_increment = 5
    job_list_manager_dummy.job_instance_factory.instantiate_queued_jobs()

    # Since we are using the 'dummy' executor, we never actually do
    # any work. The job gets moved to lost_track during reconciliation
    state = ''
    maxretries = 10
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    app.app_context().push()
    sql = "SELECT status FROM job_instance"
    i = 0
    while i < maxretries:
        i += 1
        res = DB.session.execute(sql).fetchone()
        DB.session.commit()
        if res[0] == "B":
            state = "B"
            break
        sleep(5)
    if state != "B":
        raise Exception("The init status failed to be set to B")

    # job will move into lost track because it never logs a heartbeat
    i = 0
    while i < maxretries:
        jir.reconcile()
        i += 1
        res = DB.session.execute(sql).fetchone()
        DB.session.commit()
        if res[0] != "B":
            break
        sleep(5)
    assert res[0] == "U"

    # because we only allow 1 attempt job will move to E after job instance
    # moves to U
    job_list_manager_dummy._sync()
    assert len(job_list_manager_dummy.all_error) > 0


def test_reconciler_sge(db_cfg, jlm_sge_daemon):
    # Queue a job
    task = Task(command=sge.true_path(f"{path_to_file}/shellfiles/sleep.sh"),
                name="sleepyjob_pass", num_cores=1)

    job = jlm_sge_daemon.bind_task(task)
    jlm_sge_daemon.adjust_resources_and_queue(job)

    # Give the job_state_manager some time to process the error message
    # This test job just sleeps for 60s, so it should not be missing
    # DO NOT put in a while-True loop
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    app.app_context().push()
    sql = "SELECT status FROM job_instance"
    # Test should not fail in the first 60 seconds
    for i in range(0, 30):
        res = DB.session.execute(sql).fetchone()
        DB.session.commit()
        jir = jlm_sge_daemon.job_inst_reconciler
        jir.reconcile()
        jlm_sge_daemon._sync()
        if res is not None:
            assert res[0] != "E"
        assert len(jlm_sge_daemon.all_error) == 0
        sleep(2)
    teardown_db(db_cfg)


def test_reconciler_sge_new_heartbeats(db_cfg, jlm_sge_daemon):
    jir = jlm_sge_daemon.job_inst_reconciler
    jif = jlm_sge_daemon.job_instance_factory

    task = BashTask(command="sleep 5", name="heartbeat_sleeper", num_cores=1,
                    max_runtime_seconds=500)
    job = jlm_sge_daemon.bind_task(task)
    jlm_sge_daemon.adjust_resources_and_queue(job)

    jif.instantiate_queued_jobs()
    jir.reconcile()
    jlm_sge_daemon._sync()
    count = 0
    while len(jlm_sge_daemon.all_done) < 1 and count < 10:
        sleep(50)
        jir.reconcile()
        jlm_sge_daemon.last_sync = None
        jlm_sge_daemon._sync()
        count += 1
    assert jlm_sge_daemon.all_done
    job_id = jlm_sge_daemon.all_done.pop().job_id
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        query = """
        SELECT submitted_date, report_by_date
        FROM job_instance
        WHERE job_id = {}""".format(job_id)
        res = DB.session.execute(query).fetchone()
        DB.session.commit()
    start, end = res
    assert start < end  # indicating at least one heartbeat got logged
    teardown_db(db_cfg)


def test_reconciler_sge_dag_heartbeats(db_cfg, jlm_sge_daemon):
    dag_id = jlm_sge_daemon.dag_id
    jir = jlm_sge_daemon.job_inst_reconciler
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        query = """
        SELECT heartbeat_date
        FROM task_dag
        WHERE dag_id = {}""".format(dag_id)
        res = DB.session.execute(query).fetchone()
        DB.session.commit()
        start = res
    # Sleep to ensure that the timestamps will be different
    sleep(2)
    jir.reconcile()
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        query = """
        SELECT heartbeat_date
        FROM task_dag
        WHERE dag_id = {}""".format(dag_id)
        res = DB.session.execute(query).fetchone()
        DB.session.commit()
        end = res
    assert start[0] < end[0]
    teardown_db(db_cfg)


def test_reconciler_sge_timeout(db_cfg, jlm_sge_daemon):
    # Flush the error queue to avoid false positives from other tests
    jlm_sge_daemon.all_error = set()

    # Queue a test job
    task = Task(command=sge.true_path(f"{path_to_file}/shellfiles/sleep.sh"),
                name="sleepyjob_fail", max_attempts=3, max_runtime_seconds=3,
                num_cores=1)
    job = jlm_sge_daemon.bind_task(task)
    jlm_sge_daemon.adjust_resources_and_queue(job)

    # Give the SGE scheduler some time to get the job scheduled and for the
    # reconciliation daemon to kill the job.
    # The sleepy job tries to sleep for 60 seconds, but times out after 3
    # seconds (well, when the reconciler runs, typically every 10 seconds)
    # 60
    timeout_and_skip(20, 200, 1, "sleepyjob_fail", partial(
        reconciler_sge_timeout_check,
        job_list_manager_reconciliation=jlm_sge_daemon,
        dag_id=jlm_sge_daemon.dag_id,
        job_id=job.job_id,
        db_cfg=db_cfg))
    teardown_db(db_cfg)


def reconciler_sge_timeout_check(db_cfg, job_list_manager_reconciliation,
                                 dag_id, job_id):
    jlm = job_list_manager_reconciliation
    jobs = jlm.get_job_statuses()
    completed, failed, adjusting = jlm.parse_adjusting_done_and_errors(jobs)
    if adjusting:
        for task in adjusting:
            task.executor_parameters = partial(jlm.adjust_resources,
                                               task)  # change callable
            jlm.adjust_resources_and_queue(task)
    if len(job_list_manager_reconciliation.all_error) == 1:
        assert job_id in [
            j.job_id for j in job_list_manager_reconciliation.all_error]

        # The job should have been tried 3 times...
        app = db_cfg["app"]
        DB = db_cfg["DB"]
        with app.app_context():
            query = f"select num_attempts from job where job_id = {job_id}"
            res = DB.session.execute(query).fetchone()
            DB.session.commit()
        assert res[0] == 3
        return True
    else:
        return False


# def test_ignore_qw_in_timeouts(jlm_sge_daemon, db_cfg):
#     # Qsub a long running job -> queue another job that waits on it,
#     # to simulate a hqw -> set the timeout for that hqw job to something
#     # short... make sure that job doesn't actually get killed
#     # TBD I don't think that has been implemented.
#     task = Task(command=sge.true_path(f"{path_to_file}/shellfiles/sleep.sh"),
#                 name="sleepyjob", max_attempts=3, max_runtime_seconds=3,
#                 num_cores=1)
#     job = jlm_sge_daemon.bind_task(task)
#     jlm_sge_daemon.adjust_resources_and_queue(job)

#     # Give the SGE scheduler some time to get the job scheduled and for the
#     # reconciliation daemon to kill the job
#     # The sleepy job tries to sleep for 60 seconds, but times out after 3
#     # seconds

#     timeout_and_skip(10, 200, 1, "sleepyjob", partial(
#         reconciler_sge_timeout_check,
#         job_list_manager_reconciliation=jlm_sge_daemon,
#         dag_id=jlm_sge_daemon.dag_id,
#         job_id=job.job_id,
#         db_cfg=db_cfg))


def test_queued_for_instantiation(db_cfg, jlm_sge_no_daemon):
    teardown_db(db_cfg)
    test_jif = jlm_sge_no_daemon.job_instance_factory
    test_jif.n_queued_jobs = 3

    tasks = []
    for i in range(20):
        task = BashTask(command=f"sleep {i}", num_cores=1)
        tasks.append(task)
        job = jlm_sge_no_daemon.bind_task(task)
        jlm_sge_no_daemon.adjust_resources_and_queue(job)

    # comparing results and times of old query vs new query
    rc, response = test_jif.requester.send_request(
        app_route=f'/dag/{test_jif.dag_id}/queued_jobs/1000',
        message={},
        request_type='get')
    all_jobs = [
        ExecutorJob.from_wire(j, test_jif.executor.__class__.__name__)
        for j in response['job_dcts']]

    # now new query that should only return 3 jobs
    select_jobs = test_jif._get_jobs_queued_for_instantiation()

    assert len(select_jobs) == 3
    assert len(all_jobs) == 20
    for i in range(3):
        assert select_jobs[i].job_id == (i + 1)
    teardown_db(db_cfg)
