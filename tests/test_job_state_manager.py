import getpass
import hashlib
import os
import pytest
import random
import logging
import socket

from sqlalchemy.exc import OperationalError
from datetime import datetime

from jobmon.client.the_client_config import get_the_client_config
from jobmon.client.requester import Requester
from jobmon.models.job import InvalidStateTransition, Job
from jobmon.models.job_instance_error_log import JobInstanceErrorLog
from jobmon.models.job_instance_status import JobInstanceStatus
from jobmon.models.job_status import JobStatus
from jobmon.models.job_instance import JobInstance
from jobmon.client.swarm.workflow.workflow import WorkflowDAO


HASH = 12345
SECOND_HASH = 12346


logger = logging.getLogger(__name__)


@pytest.fixture(scope='function')
def commit_hooked_jsm(jsm_jqs):
    """Add a commit hook to the JSM's database session, so we
    can intercept Error Logging and force transaction failures to test
    downstream error handling
    """
    jsm, _ = jsm_jqs

    from sqlalchemy import event
    from jobmon.server.database import Session

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


def test_get_workflow_run_id(real_dag_id):
    from jobmon.server.services.job_state_manager.job_state_manager import \
        _get_workflow_run_id
    user = getpass.getuser()
    req = Requester(get_the_client_config(), 'jsm')
    # add job
    _, response = req.send_request(
        app_route='/job',
        message={'name': 'bar',
                 'job_hash': HASH,
                 'command': 'baz',
                 'dag_id': str(real_dag_id)},
        request_type='post')
    job = Job.from_wire(response['job_dct'])

    # add workflow
    _, response = req.send_request(
        app_route='/workflow',
        message={'dag_id': str(real_dag_id),
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
        app_route='/workflow_run',
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
    assert wf_run_id == _get_workflow_run_id(job.job_id)


def test_get_workflow_run_id_no_workflow(real_dag_id):
    from jobmon.server.services.job_state_manager.job_state_manager import \
        _get_workflow_run_id
    req = Requester(get_the_client_config(), 'jsm')
    rc, response = req.send_request(
        app_route='/task_dag',
        message={'name': 'testing dag', 'user': 'pytest_user',
                 'dag_hash': 'new_dag_hash',
                 'created_date': str(datetime.utcnow())},
        request_type='post')
    dag_id = response['dag_id']

    _, response = req.send_request(
        app_route='/job',
        message={'name': 'foobar',
                 'job_hash': str(SECOND_HASH),
                 'command': 'baz',
                 'dag_id': str(dag_id)},
        request_type='post')
    job = Job.from_wire(response['job_dct'])
    assert not _get_workflow_run_id(job.job_id)


def test_jsm_valid_done(real_dag_id):
    req = Requester(get_the_client_config(), 'jsm')
    # add job
    _, response = req.send_request(
        app_route='/job',
        message={'name': 'bar',
                 'job_hash': HASH,
                 'command': 'baz',
                 'dag_id': str(real_dag_id)},
        request_type='post')
    job = Job.from_wire(response['job_dct'])

    # queue job
    req.send_request(
        app_route='/job/{}/queue'.format(job.job_id),
        message={},
        request_type='post')

    # add job instance
    _, response = req.send_request(
        app_route='/job_instance',
        message={'job_id': str(job.job_id),
                 'executor_type': 'dummy_exec'},
        request_type='post')
    job_instance_id = response['job_instance_id']

    # do job logging
    req.send_request(
        app_route='/job_instance/{}/log_executor_id'.format(job_instance_id),
        message={'executor_id': str(12345)},
        request_type='post')
    req.send_request(
        app_route='/job_instance/{}/log_running'.format(job_instance_id),
        message={'nodename': socket.gethostname(),
                 'process_group_id': str(os.getpid())},
        request_type='post')
    req.send_request(
        app_route='/job_instance/{}/log_usage'.format(job_instance_id),
        message={'usage_str': 'used resources',
                 'wallclock': '0',
                 'maxvmem': '1g',
                 'cpu': '00:00:00',
                 'io': '1'},
        request_type='post')
    req.send_request(
        app_route='/job_instance/{}/log_done'.format(job_instance_id),
        message={'job_instance_id': str(job_instance_id)},
        request_type='post')


def test_jsm_valid_error(real_dag_id):
    req = Requester(get_the_client_config(), 'jsm')

    # add job
    _, response = req.send_request(
        app_route='/job',
        message={'name': 'bar',
                 'job_hash': HASH,
                 'command': 'baz',
                 'dag_id': str(real_dag_id)},
        request_type='post')
    job = Job.from_wire(response['job_dct'])

    # queue job
    req.send_request(
        app_route='/job/{}/queue'.format(job.job_id),
        message={},
        request_type='post')

    # add job instance
    _, response = req.send_request(
        app_route='/job_instance',
        message={'job_id': str(job.job_id),
                 'executor_type': 'dummy_exec'},
        request_type='post')
    job_instance_id = response['job_instance_id']

    # do job logging
    req.send_request(
        app_route='/job_instance/{}/log_executor_id'.format(job_instance_id),
        message={'executor_id': str(12345)},
        request_type='post')
    req.send_request(
        app_route='/job_instance/{}/log_running'.format(job_instance_id),
        message={'nodename': socket.gethostname(),
                 'process_group_id': str(os.getpid())},
        request_type='post')
    req.send_request(
        app_route='/job_instance/{}/log_error'.format(job_instance_id),
        message={'error_message': "this is an error message"},
        request_type='post')


def test_invalid_transition(dag_id):

    req = Requester(get_the_client_config(), 'jsm')

    # add dag
    rc, response = req.send_request(
        app_route='/task_dag',
        message={'name': 'mocks', 'user': 'pytest_user',
                 'dag_hash': 'dag_hash',
                 'created_date': str(datetime.utcnow())},
        request_type='post')
    dag_id = response['dag_id']

    # add job
    _, response = req.send_request(
        app_route='/job',
        message={'name': 'bar',
                 'job_hash': HASH,
                 'command': 'baz',
                 'dag_id': str(dag_id)},
        request_type='post')
    job = Job.from_wire(response['job_dct'])

    # InvalidStateTransition gets raised cuz the orig ji was Instantiated
    # and then this command tries to transition it's state backwards to G
    with pytest.raises(InvalidStateTransition):
        rc, response = req.send_request(
            app_route='/job_instance',
            message={'job_id': str(job.job_id),
                     'executor_type': 'dummy_exec'},
            request_type='post')


def test_jsm_log_usage(real_dag_id):
    req = Requester(get_the_client_config(), 'jsm')

    _, response = req.send_request(
        app_route='/job',
        message={'name': 'bar',
                 'job_hash': HASH,
                 'command': 'baz',
                 'dag_id': str(real_dag_id)},
        request_type='post')
    job = Job.from_wire(response['job_dct'])
    req.send_request(
        app_route='/job/{}/queue'.format(job.job_id),
        message={},
        request_type='post')

    rc, response = req.send_request(
        app_route='/job_instance',
        message={'job_id': str(job.job_id),
                 'executor_type': 'dummy_exec'},
        request_type='post')
    job_instance_id = response['job_instance_id']
    req.send_request(
        app_route='/job_instance/{}/log_executor_id'.format(job_instance_id),
        message={'executor_id': str(12345)},
        request_type='post')
    req.send_request(
        app_route='/job_instance/{}/log_running'.format(job_instance_id),
        message={'nodename': socket.gethostname(),
                 'process_group_id': str(os.getpid())},
        request_type='post')
    req.send_request(
        app_route='/job_instance/{}/log_usage'.format(job_instance_id),
        message={'usage_str': 'used resources',
                 'wallclock': '0',
                 'maxvmem': '1g',
                 'cpu': '00:00:00',
                 'io': '1'},
        request_type='post')
    # open new session on the db and ensure job stats are being loggged
    from jobmon.server.database import session_scope
    with session_scope() as session:
        ji = session.query(JobInstance).filter(
            JobInstance.job_instance_id == job_instance_id).first()
        assert ji.usage_str == 'used resources'
        assert ji.wallclock == '0'
        assert ji.maxrss == '1g'
        assert ji.cpu == '00:00:00'
        assert ji.io == '1'
        assert ji.nodename == socket.gethostname()
    req.send_request(
        app_route='/job_instance/{}/log_done'.format(job_instance_id),
        message={},
        request_type='post')


def test_job_reset(real_dag_id):

    req = Requester(get_the_client_config(), 'jsm')

    _, response = req.send_request(
        app_route='/job',
        message={'name': 'bar',
                 'job_hash': HASH,
                 'command': 'baz',
                 'dag_id': str(real_dag_id),
                 'max_attempts': '3'},
        request_type='post')
    job = Job.from_wire(response['job_dct'])
    req.send_request(
        app_route='/job/{}/queue'.format(job.job_id),
        message={},
        request_type='post')

    # Create a couple of job instances
    rc, response = req.send_request(
        app_route='/job_instance',
        message={'job_id': str(job.job_id),
                 'executor_type': 'dummy_exec'},
        request_type='post')
    ji1 = response['job_instance_id']
    req.send_request(
        app_route='/job_instance/{}/log_executor_id'.format(ji1),
        message={'executor_id': str(12345)},
        request_type='post')
    req.send_request(
        app_route='/job_instance/{}/log_running'.format(ji1),
        message={'nodename': socket.gethostname(),
                 'process_group_id': str(os.getpid())},
        request_type='post')
    req.send_request(
        app_route='/job_instance/{}/log_error'.format(ji1),
        message={'error_message': "error 1"},
        request_type='post')

    # second job instance
    rc, response = req.send_request(
        app_route='/job_instance',
        message={'job_id': str(job.job_id),
                 'executor_type': 'dummy_exec'},
        request_type='post')
    ji2 = response['job_instance_id']
    req.send_request(
        app_route='/job_instance/{}/log_executor_id'.format(ji2),
        message={'executor_id': str(12346)},
        request_type='post')
    req.send_request(
        app_route='/job_instance/{}/log_running'.format(ji2),
        message={'nodename': socket.gethostname(),
                 'process_group_id': str(os.getpid())},
        request_type='post')
    req.send_request(
        app_route='/job_instance/{}/log_error'.format(ji2),
        message={'error_message': "error 1"},
        request_type='post')

    # third job instance
    rc, response = req.send_request(
        app_route='/job_instance',
        message={'job_id': str(job.job_id),
                 'executor_type': 'dummy_exec'},
        request_type='post')
    ji3 = response['job_instance_id']
    req.send_request(
        app_route='/job_instance/{}/log_executor_id'.format(ji3),
        message={'executor_id': str(12347)},
        request_type='post')
    req.send_request(
        app_route='/job_instance/{}/log_running'.format(ji3),
        message={'nodename': socket.gethostname(),
                 'process_group_id': str(os.getpid())},
        request_type='post')

    # Reset the job to REGISTERED
    req.send_request(
        app_route='/job/{}/reset'.format(job.job_id),
        message={},
        request_type='post')

    from jobmon.server.database import session_scope
    with session_scope() as session:
        jobs = session.query(Job).filter_by(dag_id=real_dag_id,
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


def test_jsm_submit_job_attr(jsm_jqs, dag_id):
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
    dict_of_attributes = {job_attribute.WALLCLOCK: '0', job_attribute.CPU: "00:00:00",
                          job_attribute.IO: "1", job_attribute.MAXRSS: "1g"}

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
