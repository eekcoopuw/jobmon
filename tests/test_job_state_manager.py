import pytest
from queue import Empty
import socket

from sqlalchemy.exc import OperationalError
from datetime import datetime

from jobmon.database import session_scope
from jobmon.models import InvalidStateTransition, Job, JobInstanceErrorLog, \
    JobInstanceStatus, JobStatus, JobInstance


@pytest.fixture(scope='function')
def commit_hooked_jsm(jsm_jqs):
    """Add a commit hook to the JSM's database session, so we
    can intercept Error Logging and force transaction failures to test
    downstream error handling"""

    jsm, _ = jsm_jqs

    from sqlalchemy import event
    from jobmon.database import Session

    @event.listens_for(Session, 'before_commit')
    def inspect_on_done_or_error(session):
        if any(session.dirty):
            done_jobs = [j for j in session.dirty
                         if isinstance(j, Job) and
                         j.status == JobStatus.DONE]
            done_jobs = [j for j in done_jobs
                         if not any([ji for ji in j.job_instances
                                    if ji.executor_id < 0])]
            if any(done_jobs):
                raise OperationalError("Test hook", "", "")
        if any(session.new):
            errors_to_log = [o for o in session.new
                             if isinstance(o, JobInstanceErrorLog) and
                             o.description != "skip_error_hook"]
            if any(errors_to_log):
                raise OperationalError("Test hook", "", "")
    yield jsm
    event.remove(Session, 'before_commit', inspect_on_done_or_error)


def test_jsm_valid_done(jsm_jqs, dag_id):
    jsm, jqs = jsm_jqs

    _, job_id = jsm.add_job("bar", 'hash', "baz", dag_id)
    jsm.queue_job(job_id)

    _, job_instance_id = jsm.add_job_instance(job_id, 'dummy_exec')
    jsm.log_executor_id(job_instance_id, 12345)
    jsm.log_running(job_instance_id)
    jsm.log_usage(job_instance_id, usage_str='used resources',
                  wallclock='0', maxvmem='1g', cpu='00:00:00', io='1')
    jsm.log_done(job_instance_id)


def test_jsm_valid_error(jsm_jqs):
    jsm, jqs = jsm_jqs

    _, dag_id = jsm.add_task_dag("mocks", "pytest user", "dag_hash",
                                 datetime.utcnow())
    _, job_id = jsm.add_job("bar", "hash", "baz", dag_id)
    jsm.queue_job(job_id)


    _, job_instance_id = jsm.add_job_instance(job_id, 'dummy_exec')
    jsm.log_executor_id(job_instance_id, 12345)
    jsm.log_running(job_instance_id)
    jsm.log_error(job_instance_id, "this is an error message")


def test_invalid_transition(jsm_jqs):
    jsm, jqs = jsm_jqs

    _, dag_id = jsm.add_task_dag("mocks", "pytest user", "dag_hash",
                                 datetime.utcnow())
    _, job_id = jsm.add_job("bar", "hash", "baz", dag_id)

    with pytest.raises(InvalidStateTransition):
        _, job_instance_id = jsm.add_job_instance(job_id, 'dummy_exec')


def test_single_publish_on_error(dag_id, job_list_manager_sub,
                                 commit_hooked_jsm):
    """Ensures that status publications for errors only happen once the
    database transaction has been committed"""

    jsm = commit_hooked_jsm

    _, job_id = jsm.add_job("bar", "hash", "baz", dag_id)
    jsm.queue_job(job_id)

    _, job_instance_id = jsm.add_job_instance(job_id, 'dummy_exec')
    jsm.log_executor_id(job_instance_id, 12345)
    jsm.log_running(job_instance_id)

    # Force state where double publish was happening
    with pytest.raises(OperationalError):
        jsm.log_error(job_instance_id, "force transaction failure")
    with pytest.raises(Empty):
        updates = job_list_manager_sub.block_until_any_done_or_error(5)

    jsm.log_error(job_instance_id, "skip_error_hook")
    updates = job_list_manager_sub.block_until_any_done_or_error(5)
    assert (job_id, JobStatus.ERROR_FATAL) in updates


def test_single_publish_on_done(dag_id, job_list_manager_sub,
                                commit_hooked_jsm):
    """Ensures that status publications for DONE only happen once the
    database transaction has been committed"""

    jsm = commit_hooked_jsm

    # Note we need (2) attempts here, the first of which we will
    # use to force a transaction failure and the second of which
    # should transact successfully
    _, job_id = jsm.add_job("bar", "hash", "baz", dag_id, max_attempts=2)
    jsm.queue_job(job_id)

    _, job_instance_id = jsm.add_job_instance(job_id, 'dummy_exec')
    jsm.log_executor_id(job_instance_id, 12345)
    jsm.log_running(job_instance_id)
    jsm.log_usage(job_instance_id, usage_str='used resources',
                  wallclock='0', maxvmem='1g', cpu='00:00:00', io='1')
    with pytest.raises(OperationalError):
        jsm.log_done(job_instance_id)
    # Force state where double publish could happen, if not dependent
    # on successful commit of transaction scope
    with pytest.raises(Empty):
        updates = job_list_manager_sub.block_until_any_done_or_error(5)

    # Try again... skipping the done hook
    jsm.log_error(job_instance_id, "skip_error_hook")
    _, job_instance_id = jsm.add_job_instance(job_id, 'skip_done_hook')
    jsm.log_executor_id(job_instance_id, -1)
    jsm.log_running(job_instance_id)
    jsm.log_usage(job_instance_id, usage_str='used resources',
                  wallclock='0', maxvmem='1g', cpu='00:00:00', io='1')
    jsm.log_done(job_instance_id)
    updates = job_list_manager_sub.block_until_any_done_or_error(5)
    assert (job_id, JobStatus.DONE) in updates


def test_jsm_log_usage(jsm_jqs, dag_id):
    jsm, jqs = jsm_jqs

    _, job_id = jsm.add_job("bar", "hash", "baz", dag_id)
    jsm.queue_job(job_id)

    _, job_instance_id = jsm.add_job_instance(job_id, 'dummy_exec')
    jsm.log_executor_id(job_instance_id, 12345)
    jsm.log_running(job_instance_id)
    jsm.log_usage(job_instance_id, usage_str='used resources',
                  nodename=socket.gethostname(), wallclock='0',
                  maxvmem='1g', cpu='00:00:00', io='1')
    # open new session on the db and ensure job stats are being loggged
    with session_scope() as session:
        ji = session.query(JobInstance).filter(
            JobInstance.job_instance_id == job_instance_id).first()
        assert ji.usage_str == 'used resources'
        assert ji.wallclock == '0'
        assert ji.maxvmem == '1g'
        assert ji.cpu == '00:00:00'
        assert ji.io == '1'
        assert ji.nodename == socket.gethostname()
    jsm.log_done(job_instance_id)


def test_job_reset(jsm_jqs, dag_id):
    jsm, jqs = jsm_jqs

    _, job_id = jsm.add_job("bar", 'hash', "baz", dag_id, max_attempts=3)
    jsm.queue_job(job_id)

    # Create a couple of job instances
    _, ji1 = jsm.add_job_instance(job_id, 'dummy_exec')
    jsm.log_executor_id(ji1, 12345)
    jsm.log_running(ji1)
    jsm.log_error(ji1, "error 1")

    _, ji2 = jsm.add_job_instance(job_id, 'dummy_exec')
    jsm.log_executor_id(ji2, 12346)
    jsm.log_running(ji2)
    jsm.log_error(ji2, "error 1")

    _, ji3 = jsm.add_job_instance(job_id, 'dummy_exec')
    jsm.log_executor_id(ji3, 12347)
    jsm.log_running(ji3)

    # Reset the job to REGISTERED
    jsm.reset_job(job_id)

    with session_scope() as session:
        jobs = session.query(Job).filter_by(dag_id=dag_id, job_id=job_id).all()
        assert len(jobs) == 1
        job = jobs[0]
        assert job.status == JobStatus.REGISTERED
        assert job.num_attempts == 0
        assert len(job.job_instances) == 3
        assert all([ji.status == JobInstanceStatus.ERROR
                    for ji in job.job_instances])
        errors = [e for ji in job.job_instances for e in ji.errors]

        # The (2) original errors, plus a RESET error for each of the (3)
        # jis... It's a little aggressive, but it's the safe way to ensure
        # job_instances don't hang around in unknown states upon RESET
        assert len(errors) == 5
