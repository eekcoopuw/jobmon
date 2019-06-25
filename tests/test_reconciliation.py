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
from jobmon.client.utils import kill_remote_process
from jobmon.server.jobmonLogging import jobmonLogging as logging

from tests.timeout_and_skip import timeout_and_skip
from functools import partial

logger = logging.getLogger(__name__)


@pytest.fixture(scope='function')
def job_list_manager_dummy(real_dag_id):
    # We don't want this queueing jobs in conflict with the SGE daemons...
    # but we do need it to subscribe to status updates for reconciliation
    # tests. Start this thread manually.
    old_heartbeat_interval = client_config.heartbeat_interval
    client_config.heartbeat_interval = 5
    executor = DummyExecutor()
    jlm = JobListManager(real_dag_id, executor=executor,
                         start_daemons=False, interrupt_on_error=False)
    yield jlm
    jlm.disconnect()
    client_config.heartbeat_interval = old_heartbeat_interval


@pytest.fixture(scope='function')
def job_list_manager_reconciliation(real_dag_id):
    executor = SGEExecutor(project="proj_tools")
    jlm = JobListManager(real_dag_id, executor=executor,
                         start_daemons=True,
                         interrupt_on_error=False)
    yield jlm
    jlm.disconnect()


@pytest.mark.skip(reason="ssh problems on buster or fixtures from other tests "
                         "interfering, to be addressed in GBDSCI-1802")
def test_reconciler_running_ji_disappears(job_list_manager_reconciliation,
                                          db_cfg):
    """ensures that if a job silently dies (so it doesn't throw an interrupt,
     but would not be present if you ran qstat), it stops logging heartbeats
     and the reconciler handles it"""
    job_list_manager_reconciliation._sync()
    job_list_manager_reconciliation.all_error = set()
    job_list_manager_reconciliation.all_done = set()
    jir = job_list_manager_reconciliation.job_inst_reconciler
    jif = job_list_manager_reconciliation.job_instance_factory
    jif.interrupt_on_error = True

    task = BashTask(command="sleep 300", name="heartbeat_sleeper",
                    num_cores=1,
                    mem_free="2G",
                    max_runtime_seconds='1000',
                    max_attempts=1)

    job = job_list_manager_reconciliation.bind_task(task)
    job_list_manager_reconciliation.queue_job(job)
    instantiated = jif.instantiate_queued_jobs()
    jid = instantiated[0]
    status = query_until_running(db_cfg, jid).status
    while status != 'R':
        res = query_until_running(db_cfg, jid)
        status = res.status
    pid = res.process_group_id
    hostname = res.nodename
    # wait until it starts running, and then silently kill it
    exit_code, out, err = kill_remote_process(hostname=hostname, pid=pid)
    qstat_out = sge.qstat()
    for jid in qstat_out.keys():
        assert 'heartbeat_sleeper' not in qstat_out[jid]['name']

    # job should not log a heartbeat so it should error out eventually
    count = 0
    while len(job_list_manager_reconciliation.all_error) < 1 and count < 3:
        count += 1
        sleep(30)
        jir.reconcile()
        job_list_manager_reconciliation._sync()
    assert job_list_manager_reconciliation.all_error


def query_until_running(db_cfg, jid):
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        query = """
                   SELECT process_group_id, nodename, status
                   FROM job_instance
                   WHERE job_instance_id = {jid}
               """.format(jid=jid)
        res = DB.session.execute(query).fetchone()
        DB.session.commit()
    return res


def test_reconciler_dummy(db_cfg, job_list_manager_dummy):
    """Creates a job instance, gets an executor id so it can be in submitted
    to the batch executor state, and then it will never be run (it will miss
    its report by date and the reconciler will kill it)"""

    # Flush the error queue to avoid false positives from other tests
    job_list_manager_dummy.all_error = set()

    # Queue a job
    task = Task(command="ls", num_cores="1", name="dummyfbb", max_attempts=1)
    job = job_list_manager_dummy.bind_task(task)
    job_list_manager_dummy.queue_job(job)
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


def test_reconciler_sge(db_cfg, job_list_manager_reconciliation):
    # Flush the error queue to avoid false positives from other tests
    job_list_manager_reconciliation.all_error = set()

    # Queue a job
    task = Task(command=sge.true_path("tests/shellfiles/sleep.sh"),
                name="sleepyjob_pass", num_cores=1)
    job = job_list_manager_reconciliation.bind_task(task)
    job_list_manager_reconciliation.queue_job(job)

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
        jir = job_list_manager_reconciliation.job_inst_reconciler
        jir.reconcile()
        job_list_manager_reconciliation._sync()
        if res is not None:
            assert res[0] != "E"
        assert len(job_list_manager_reconciliation.all_error) == 0
        sleep(2)


def test_reconciler_sge_new_heartbeats(job_list_manager_reconciliation, db_cfg
                                       ):
    """ensures that the jobs have logged new heartbeats while running"""
    job_list_manager_reconciliation.all_error = set()
    jir = job_list_manager_reconciliation.job_inst_reconciler
    jif = job_list_manager_reconciliation.job_instance_factory

    task = BashTask(command="sleep 5", name="heartbeat_sleeper", num_cores=1,
                    max_runtime_seconds=500)
    job = job_list_manager_reconciliation.bind_task(task)
    job_list_manager_reconciliation.queue_job(job)

    jif.instantiate_queued_jobs()
    jir.reconcile()
    job_list_manager_reconciliation._sync()
    count = 0
    while len(job_list_manager_reconciliation.all_done) < 1 and count < 10:
        sleep(50)
        jir.reconcile()
        job_list_manager_reconciliation.last_sync = None
        job_list_manager_reconciliation._sync()
        count += 1
    assert job_list_manager_reconciliation.all_done
    job_id = job_list_manager_reconciliation.all_done.pop().job_id
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


def test_reconciler_sge_timeout(job_list_manager_reconciliation, db_cfg):
    # Flush the error queue to avoid false positives from other tests
    job_list_manager_reconciliation.all_error = set()

    # Queue a test job
    task = Task(command=sge.true_path("tests/shellfiles/sleep.sh"),
                name="sleepyjob_fail", max_attempts=3, max_runtime_seconds=3,
                num_cores=1)
    job = job_list_manager_reconciliation.bind_task(task)
    job_list_manager_reconciliation.queue_job(job)

    # Give the SGE scheduler some time to get the job scheduled and for the
    # reconciliation daemon to kill the job.
    # The sleepy job tries to sleep for 60 seconds, but times out after 3
    # seconds (well, when the reconciler runs, typically every 10 seconds)
    # 60
    timeout_and_skip(20, 200, 1, "sleepyjob_fail", partial(
        reconciler_sge_timeout_check,
        job_list_manager_reconciliation=job_list_manager_reconciliation,
        dag_id=job_list_manager_reconciliation.dag_id,
        job_id=job.job_id,
        db_cfg=db_cfg))


def reconciler_sge_timeout_check(job_list_manager_reconciliation, dag_id,
                                 job_id, db_cfg):
    job_list_manager_reconciliation._sync()
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


def test_ignore_qw_in_timeouts(job_list_manager_reconciliation, db_cfg):
    # Flush the error queue to avoid false positives from other tests
    job_list_manager_reconciliation.all_error = set()

    # Qsub a long running job -> queue another job that waits on it,
    # to simulate a hqw -> set the timeout for that hqw job to something
    # short... make sure that job doesn't actually get killed
    # TBD I don't think that has been implemented.
    task = Task(command=sge.true_path("tests/shellfiles/sleep.sh"),
                name="sleepyjob", max_attempts=3, max_runtime_seconds=3,
                num_cores=1)
    job = job_list_manager_reconciliation.bind_task(task)
    job_list_manager_reconciliation.queue_job(job)

    # Give the SGE scheduler some time to get the job scheduled and for the
    # reconciliation daemon to kill the job
    # The sleepy job tries to sleep for 60 seconds, but times out after 3
    # seconds

    timeout_and_skip(10, 200, 1, "sleepyjob", partial(
        ignore_qw_in_timeouts_check,
        job_list_manager_reconciliation=job_list_manager_reconciliation,
        dag_id=job_list_manager_reconciliation.dag_id,
        job_id=job.job_id,
        db_cfg=db_cfg))


def ignore_qw_in_timeouts_check(job_list_manager_reconciliation, dag_id,
                                job_id, db_cfg):
    job_list_manager_reconciliation._sync()
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


@pytest.fixture(scope='function')
def sge_jlm_for_queues(real_dag_id, tmpdir_factory):
    """This creates a job_list_manager that uses the SGEExecutor, does
    start the JobInstanceFactory and JobReconciler threads, and does not
    interrupt on error. It has short reconciliation intervals so that the
    tests run faster than in production.
    """
    from jobmon.client.swarm.executors.sge import SGEExecutor
    from jobmon.client.swarm.job_management.job_list_manager import \
        JobListManager

    elogdir = str(tmpdir_factory.mktemp("elogs"))
    ologdir = str(tmpdir_factory.mktemp("ologs"))

    executor = SGEExecutor(stderr=elogdir, stdout=ologdir,
                           project='proj_tools')
    jlm = JobListManager(real_dag_id, executor=executor, start_daemons=False,
                         job_instantiation_interval=10,
                         interrupt_on_error=True, n_queued_jobs=3)
    yield jlm
    jlm.disconnect()


def test_queued_for_instantiation(sge_jlm_for_queues):

    test_jif = sge_jlm_for_queues.job_instance_factory

    tasks = []
    for i in range(20):
        task = BashTask(command=f"sleep {i}", num_cores=1)
        tasks.append(task)
        job = sge_jlm_for_queues.bind_task(task)
        sge_jlm_for_queues.queue_job(job)

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
