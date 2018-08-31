import getpass
import hashlib
import os
import pytest
import random
import socket
from queue import Empty

from sqlalchemy.exc import OperationalError
from datetime import datetime

from jobmon.database import session_scope
from jobmon.models import InvalidStateTransition, Job, JobInstanceErrorLog, \
    JobInstanceStatus, JobStatus, JobInstance
from jobmon.workflow.executable_task import ExecutableTask


HASH = 12345
SECOND_HASH = 12346

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


def test_get_workflow_run_id(jsm_jqs, dag_id):
    jsm, _ = jsm_jqs
    user = getpass.getuser()
    _, job_dct = jsm.add_job("bar", HASH, "baz", dag_id)
    job = Job.from_wire(job_dct)
    wf = jsm.add_workflow(
        dag_id, "args_{}".format(random.randint(1, 1e7)),
        hashlib.sha1('hash_{}'.format(random.randint(1, 1e7))
                     .encode('utf-8')).hexdigest(), "test", user)
    wf_run_id = jsm.add_workflow_run(wf[1]['id'], user, socket.gethostname(),
                                     000, None, None, 'proj_jenkins', '', ''
                                     )[1]
    print(dag_id)
    assert wf_run_id == jsm._get_workflow_run_id(job.job_id)


def test_get_workflow_run_id_no_workflow(jsm_jqs):
    jsm, _ = jsm_jqs
    _, dag_id = jsm.add_task_dag("testing", "pytest user", "new_dag_hash",
                                 datetime.utcnow())
    _, job_dct = jsm.add_job("foobar", SECOND_HASH, "baz", dag_id)
    job = Job.from_wire(job_dct)
    assert not jsm._get_workflow_run_id(job.job_id)


def test_jsm_valid_done(jsm_jqs, dag_id):
    jsm, jqs = jsm_jqs

    _, job_dct = jsm.add_job("bar", HASH, "baz", dag_id)
    job = Job.from_wire(job_dct)
    jsm.queue_job(job.job_id)


    _, job_instance_id = jsm.add_job_instance(job.job_id, 'dummy_exec')
    jsm.log_executor_id(job_instance_id, 12345)
    jsm.log_running(job_instance_id, socket.gethostname(), os.getpid())
    jsm.log_usage(job_instance_id, usage_str='used resources',
                  wallclock='0', maxrss='1g', cpu='00:00:00', io='1')
    jsm.log_done(job_instance_id)


def test_jsm_valid_error(jsm_jqs):
    jsm, jqs = jsm_jqs

    _, dag_id = jsm.add_task_dag("mocks", "pytest user", "dag_hash",
                                 datetime.utcnow())
    _, job_dct = jsm.add_job("bar", HASH, "baz", dag_id)
    job = Job.from_wire(job_dct)
    jsm.queue_job(job.job_id)

    _, job_instance_id = jsm.add_job_instance(job.job_id, 'dummy_exec')
    jsm.log_executor_id(job_instance_id, 12345)
    jsm.log_running(job_instance_id, socket.gethostname(), os.getpid())
    jsm.log_error(job_instance_id, "this is an error message")


def test_invalid_transition(jsm_jqs):
    jsm, jqs = jsm_jqs

    _, dag_id = jsm.add_task_dag("mocks", "pytest user", "dag_hash",
                                 datetime.utcnow())
    _, job_dct = jsm.add_job("bar", HASH, "baz", dag_id)
    job = Job.from_wire(job_dct)

    with pytest.raises(InvalidStateTransition):
        _, job_instance_id = jsm.add_job_instance(job.job_id, 'dummy_exec')


def test_single_publish_on_error(dag_id, job_list_manager_sub,
                                 commit_hooked_jsm):
    """Ensures that status publications for errors only happen once the
    database transaction has been committed"""

    jsm = commit_hooked_jsm

    task = ExecutableTask(command="bar", name="baz", max_attempts=1)
    job = job_list_manager_sub.bind_task(task)._job
    jsm.queue_job(job.job_id)

    _, job_instance_id = jsm.add_job_instance(job.job_id, 'dummy_exec')
    jsm.log_executor_id(job_instance_id, 12345)
    jsm.log_running(job_instance_id, socket.gethostname(), os.getpid())

    # Force state where double publish was happening
    with pytest.raises(OperationalError):
        jsm.log_error(job_instance_id, "force transaction failure")
    with pytest.raises(Empty):
        updates = job_list_manager_sub.block_until_any_done_or_error(5)

    jsm.log_error(job_instance_id, "skip_error_hook")
    _, failed = job_list_manager_sub.block_until_any_done_or_error(5)
    assert failed[0].job_id == job.job_id
    assert failed[0].status == JobStatus.ERROR_FATAL


def test_single_publish_on_done(dag_id, job_list_manager_sub,
                                commit_hooked_jsm):
    """Ensures that status publications for DONE only happen once the
    database transaction has been committed"""

    jsm = commit_hooked_jsm

    # Note we need (2) attempts here, the first of which we will
    # use to force a transaction failure and the second of which
    # should transact successfully
    task = ExecutableTask(command="bar", name="baz", max_attempts=2)
    job = job_list_manager_sub.bind_task(task)._job
    jsm.queue_job(job.job_id)

    _, job_instance_id = jsm.add_job_instance(job.job_id, 'dummy_exec')
    jsm.log_executor_id(job_instance_id, 12345)
    jsm.log_running(job_instance_id, socket.gethostname(), os.getpid())
    jsm.log_usage(job_instance_id, usage_str='used resources',
                  wallclock='0', maxrss='1g', cpu='00:00:00', io='1')
    with pytest.raises(OperationalError):
        jsm.log_done(job_instance_id)
    # Force state where double publish could happen, if not dependent
    # on successful commit of transaction scope
    with pytest.raises(Empty):
        updates = job_list_manager_sub.block_until_any_done_or_error(5)

    # Try again... skipping the done hook
    jsm.log_error(job_instance_id, "skip_error_hook")
    _, job_instance_id = jsm.add_job_instance(job.job_id, 'skip_done_hook')
    jsm.log_executor_id(job_instance_id, -1)
    jsm.log_running(job_instance_id, socket.gethostname(), os.getpid())
    jsm.log_usage(job_instance_id, usage_str='used resources',
                  wallclock='0', maxrss='1g', cpu='00:00:00', io='1')
    jsm.log_done(job_instance_id)
    done, _ = job_list_manager_sub.block_until_any_done_or_error(5)
    assert done[0].job_id == job.job_id
    assert done[0].status == JobStatus.DONE


def test_jsm_log_usage(jsm_jqs, dag_id):
    jsm, jqs = jsm_jqs

    _, job_dct = jsm.add_job("bar", HASH, "baz", dag_id)
    job = Job.from_wire(job_dct)
    jsm.queue_job(job.job_id)

    _, job_instance_id = jsm.add_job_instance(job.job_id, 'dummy_exec')
    jsm.log_executor_id(job_instance_id, 12345)
    jsm.log_running(job_instance_id, socket.gethostname(), os.getpid())
    jsm.log_usage(job_instance_id, usage_str='used resources', wallclock='0',
                  maxrss='1g', cpu='00:00:00', io='1')
    # open new session on the db and ensure job stats are being loggged
    dict_of_attributes = {job_attribute.WALLCLOCK: '0' , job_attribute.CPU: "00:00:00",
                          job_attribute.IO: "1", job_attribute.MAXRSS: "1g"}

    with session_scope() as session:
        ji = session.query(JobInstance).filter(
            JobInstance.job_instance_id == job_instance_id).first()
        assert ji.usage_str == 'used resources'
        assert ji.wallclock == '0'
        assert ji.maxrss == '1g'
        assert ji.cpu == '00:00:00'
        assert ji.io == '1'
        assert ji.nodename == socket.gethostname()

        #checking that job attributes were also set with the usage data
        # job_attribute_query = session.execute("""
        #                                 SELECT job_attribute.id,
        #                                        job_attribute.job_id,
        #                                        job_attribute.attribute_type,
        #                                        job_attribute.value
        #                                 FROM job_attribute
        #                                 JOIN job
        #                                 ON job_attribute.job_id=job.job_id
        #                                 WHERE job_attribute.job_id={id}
        #                                 """.format(id=job.job_id))
        # attribute_entries = job_attribute_query.fetchall()
        # for entry in attribute_entries:
        #     attribute_entry_type = entry.attribute_type
        #     attribute_entry_value = entry.value
        #     assert dict_of_attributes[attribute_entry_type] == attribute_entry_value

    jsm.log_done(job_instance_id)

    with session_scope() as session:
        job_attribute_query = session.execute("""
                                                SELECT job_attribute.id,
                                                       job_attribute.job_id,
                                                       job_attribute.attribute_type,
                                                       job_attribute.value
                                                FROM job_attribute
                                                JOIN job
                                                ON job_attribute.job_id=job.job_id
                                                WHERE job_attribute.job_id={id}
                                                """.format(id=job.job_id))
        attribute_entries = job_attribute_query.fetchall()
        for entry in attribute_entries:
            attribute_entry_type = entry.attribute_type
            attribute_entry_value = entry.value
            assert dict_of_attributes[attribute_entry_type] == attribute_entry_value


def test_job_reset(jsm_jqs, dag_id):
    jsm, jqs = jsm_jqs

    _, job_dct = jsm.add_job("bar", HASH, "baz", dag_id, max_attempts=3)
    job = Job.from_wire(job_dct)
    jsm.queue_job(job.job_id)

    # Create a couple of job instances
    _, ji1 = jsm.add_job_instance(job.job_id, 'dummy_exec')
    jsm.log_executor_id(ji1, 12345)
    jsm.log_running(ji1, socket.gethostname(), os.getpid())
    jsm.log_error(ji1, "error 1")

    _, ji2 = jsm.add_job_instance(job.job_id, 'dummy_exec')
    jsm.log_executor_id(ji2, 12346)
    jsm.log_running(ji2, socket.gethostname(), os.getpid())
    jsm.log_error(ji2, "error 1")

    _, ji3 = jsm.add_job_instance(job.job_id, 'dummy_exec')
    jsm.log_executor_id(ji3, 12347)
    jsm.log_running(ji3, socket.gethostname(), os.getpid())

    # Reset the job to REGISTERED
    jsm.reset_job(job.job_id)

    with session_scope() as session:
        jobs = session.query(Job).filter_by(dag_id=dag_id,
                                            job_id=job.job_id).all()
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
