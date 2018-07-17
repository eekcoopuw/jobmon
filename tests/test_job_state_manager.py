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
from jobmon.config import config
from jobmon import requester
from jobmon.models import InvalidStateTransition, Job, JobInstanceErrorLog, \
    JobInstanceStatus, JobStatus, JobInstance
from jobmon.workflow.executable_task import ExecutableTask
from jobmon.workflow.workflow import WorkflowDAO


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
    user = getpass.getuser()
    jsm, _ = jsm_jqs
    req = requester.Requester(config.jsm_port)
    # add job
    _, response = req.send_request(
        app_route='/add_job',
        message={'name': 'bar',
                 'job_hash': HASH,
                 'command': 'baz',
                 'dag_id': str(dag_id)},
        request_type='post')
    job = Job.from_wire(response['job_dct'])

    # add workflow
    _, response = req.send_request(
        app_route='/add_workflow',
        message={'dag_id': str(dag_id),
                 'workflow_args': "args_{}".format(random.randint(1, 1e7)),
                 'workflow_hash': hashlib.sha1('hash_{}'
                                               .format(random.randint(1, 1e7))
                                               .encode('utf-8')).hexdigest(),
                 'name': 'test',
                 'user': user},
        request_type='post')
    wf = WorkflowDAO.from_wire(response['workflow_dct'])

    # add workflow_run_id
    _, response = req.send_request(
        app_route='/add_workflow_run',
        message={'workflow_id': str(wf.id),
                 'user': user,
                 'hostname': socket.gethostname(),
                 'pid': '000',
                 'stderr': "",
                 'stdout': "",
                 'project': 'proj_jenkins',
                 'slack_channel': "",
                 'working_dir': ""},
        request_type='post')
    wf_run_id = response['workflow_run_id']
    # make sure that the wf run that was just created matches the one that
    # jsm._get_workflow_run_id gets
    from jobmon.services.job_state_manager import _get_workflow_run_id
    assert wf_run_id == _get_workflow_run_id(job.job_id)


def test_get_workflow_run_id_no_workflow(jsm_jqs):
    jsm, _ = jsm_jqs
    req = requester.Requester(config.jsm_port)
    rc, response = req.send_request(
        app_route='/add_task_dag',
        message={'name': 'testing dag', 'user': 'pytest_user',
                 'dag_hash': 'new_dag_hash',
                 'created_date': str(datetime.utcnow())},
        request_type='post')
    dag_id = response['dag_id']

    _, response = req.send_request(
        app_route='/add_job',
        message={'name': 'foobar',
                 'job_hash': str(SECOND_HASH),
                 'command': 'baz',
                 'dag_id': str(dag_id)},
        request_type='post')
    job = Job.from_wire(response['job_dct'])
    from jobmon.services.job_state_manager import _get_workflow_run_id
    assert not _get_workflow_run_id(job.job_id)


def test_jsm_valid_done(jsm_jqs, dag_id):
    jsm, jqs = jsm_jqs

    req = requester.Requester(config.jsm_port)
    # add job
    _, response = req.send_request(
        app_route='/add_job',
        message={'name': 'bar',
                 'job_hash': HASH,
                 'command': 'baz',
                 'dag_id': str(dag_id)},
        request_type='post')
    job = Job.from_wire(response['job_dct'])

    # queue job
    _, response = req.send_request(
        app_route='/queue_job',
        message={'job_id': str(job.job_id)},
        request_type='post')

    # add job instance
    _, response = req.send_request(
        app_route='/add_job_instance',
        message={'job_id': str(job.job_id),
                 'executor_type': 'dummy_exec'},
        request_type='post')
    job_instance_id = response['job_instance_id']

    # do job logging
    req.send_request(
        app_route='log_executor_id',
        message={'job_instance_id': str(job_instance_id),
                 'executor_id': str(12345)},
        request_type='post')
    req.send_request(
        app_route='log_running',
        message={'job_instance_id': str(job_instance_id),
                 'nodename': socket.gethostname(),
                 'process_group_id': str(os.getpid())},
        request_type='post')
    req.send_request(
        app_route='log_usage',
        message={'job_instance_id': str(job_instance_id),
                 'usage_str': 'used resources',
                 'wallclock': '0',
                 'maxvmem': '1g',
                 'cpu': '00:00:00',
                 'io': '1'},
        request_type='post')
    req.send_request(
        app_route='log_done',
        message={'job_instance_id': str(job_instance_id)},
        request_type='post')


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
                  wallclock='0', maxvmem='1g', cpu='00:00:00', io='1')
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
                  wallclock='0', maxvmem='1g', cpu='00:00:00', io='1')
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
