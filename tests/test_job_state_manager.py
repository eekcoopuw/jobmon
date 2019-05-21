import getpass
import hashlib
import os
import pytest
import random
import socket

from sqlalchemy.exc import OperationalError
from datetime import datetime

from jobmon.client import shared_requester as req
from jobmon.models.job import Job
from jobmon.models.exceptions import InvalidStateTransition
from jobmon.models.job_instance_error_log import JobInstanceErrorLog
from jobmon.models.job_instance_status import JobInstanceStatus
from jobmon.models.job_status import JobStatus
from jobmon.models.job_instance import JobInstance
from jobmon.models.workflow import Workflow
from jobmon.models.attributes.constants import job_attribute
from jobmon.server.jobmonLogging import jobmonLogging as logging


HASH = 12345
SECOND_HASH = 12346


logger = logging.getLogger(__name__)


@pytest.fixture(scope='function')
def commit_hooked_jsm(jsm_jqs):
    """Add a commit hook to the JSM's database session, so we
    can intercept Error Logging and force transaction failures to test
    downstream error handling
    """
    DB = db_cfg["DB"]
    jsm, _ = jsm_jqs

    from sqlalchemy import event

    sessionmaker = DB.create_session()

    @event.listens_for(sessionmaker, 'before_commit')
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
    event.remove(sessionmaker, 'before_commit', inspect_on_done_or_error)


def test_get_workflow_run_id(db_cfg, real_dag_id):
    from jobmon.server.job_state_manager.job_state_manager import \
        _get_workflow_run_id
    user = getpass.getuser()
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
    wf = Workflow.from_wire(response['workflow_dct'])

    # add workflow_run_id
    _, response = req.send_request(
        app_route='/workflow_run',
        message={'workflow_id': str(wf.id),
                 'user': user,
                 'hostname': socket.getfqdn(),
                 'pid': '000',
                 'stderr': "",
                 'stdout': "",
                 'project': 'proj_tools',
                 'slack_channel': "",
                 'executor_class': 'SGEExecutor',
                 'working_dir': "",
                 'resource_adjustment': "0.5"},
        request_type='post')
    wf_run_id = response['workflow_run_id']
    # make sure that the wf run that was just created matches the one that
    # jsm._get_workflow_run_id gets
    app = db_cfg["app"]
    with app.app_context():
        assert wf_run_id == _get_workflow_run_id(job.job_id)


def test_get_workflow_run_id_no_workflow(real_dag_id, db_cfg):
    from jobmon.server.job_state_manager.job_state_manager import \
        _get_workflow_run_id
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
    app = db_cfg["app"]
    with app.app_context():
        assert not _get_workflow_run_id(job.job_id)


def test_jsm_valid_done(real_dag_id):
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
        message={'executor_id': str(12345),
                 'next_report_increment': 15},
        request_type='post')
    req.send_request(
        app_route='/job_instance/{}/log_running'.format(job_instance_id),
        message={'nodename': socket.getfqdn(),
                 'process_group_id': str(os.getpid()),
                 'next_report_increment': 120},
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
        message={'job_instance_id': str(job_instance_id), 'nodename': socket.getfqdn()},
        request_type='post')


def test_jsm_valid_error(real_dag_id):

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
        message={'executor_id': str(12345),
                 'next_report_increment': 15},
        request_type='post')
    req.send_request(
        app_route='/job_instance/{}/log_running'.format(job_instance_id),
        message={'nodename': socket.getfqdn(),
                 'process_group_id': str(os.getpid()),
                 'next_report_increment': 120},
        request_type='post')
    req.send_request(
        app_route='/job_instance/{}/log_error'.format(job_instance_id),
        message={'error_message': "this is an error message",
                 'executor_id': str(12345),
                 'exit_status': 2,
                 'nodename': socket.getfqdn()},
        request_type='post')


def test_invalid_transition(dag_id):

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


def test_untimely_transition(real_dag_id, db_cfg):
    from jobmon.server.job_state_manager.job_state_manager import \
        _get_job_instance

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

    # the job hits a race and reports running before the executor logs
    # the UGE id
    req.send_request(
        app_route='/job_instance/{}/log_running'.format(job_instance_id),
        message={'nodename': socket.getfqdn(),
                 'process_group_id': str(os.getpid()),
                 'next_report_increment': 120},
        request_type='post')

    # try and go backward to submitted and make sure state stays in running
    req.send_request(
        app_route='/job_instance/{}/log_executor_id'.format(job_instance_id),
        message={'executor_id': str(12345),
                 'next_report_increment': 15},
        request_type='post')

    DB = db_cfg["DB"]
    app = db_cfg["app"]
    with app.app_context():
        ji = _get_job_instance(DB.session, job_instance_id)
        assert ji.status == JobInstanceStatus.RUNNING
        assert ji.executor_id == 12345


def test_jsm_log_usage(db_cfg, real_dag_id):

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
        message={'executor_id': str(12345),
                 'next_report_increment': 15},
        request_type='post')
    req.send_request(
        app_route='/job_instance/{}/log_running'.format(job_instance_id),
        message={'nodename': socket.getfqdn(),
                 'process_group_id': str(os.getpid()),
                 'next_report_increment': 120},
        request_type='post')
    req.send_request(
        app_route='/job_instance/{}/log_usage'.format(job_instance_id),
        message={'usage_str': 'used resources',
                 'wallclock': '0',
                 'maxrss': '1g',
                 'cpu': '00:00:00',
                 'io': '1'},
        request_type='post')
    # open new session on the db and ensure job stats are being loggged
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        ji = DB.session.query(JobInstance).filter(
            JobInstance.job_instance_id == job_instance_id).first()
        assert ji.usage_str == 'used resources'
        assert ji.wallclock == '0'
        assert ji.maxrss == '1g'
        assert ji.cpu == '00:00:00'
        assert ji.io == '1'
        assert ji.nodename == socket.getfqdn()
        DB.session.commit()
    req.send_request(
        app_route='/job_instance/{}/log_done'.format(job_instance_id),
        message={'nodename': socket.getfqdn()},
        request_type='post')


def test_job_reset(db_cfg, real_dag_id):

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
        message={'executor_id': str(12345),
                 'next_report_increment': 15},
        request_type='post')
    req.send_request(
        app_route='/job_instance/{}/log_running'.format(ji1),
        message={'nodename': socket.getfqdn(),
                 'process_group_id': str(os.getpid()),
                 'next_report_increment': 120},
        request_type='post')
    req.send_request(
        app_route='/job_instance/{}/log_error'.format(ji1),
        message={'error_message': "error 1",
                 'executor_id': str(12345),
                 'exit_status': 1,
                 'nodename': socket.getfqdn()},
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
        message={'executor_id': str(12345),
                 'next_report_increment': 15},
        request_type='post')
    req.send_request(
        app_route='/job_instance/{}/log_running'.format(ji2),
        message={'nodename': socket.getfqdn(),
                 'process_group_id': str(os.getpid()),
                 'next_report_increment': 120},
        request_type='post')
    req.send_request(
        app_route='/job_instance/{}/log_error'.format(ji2),
        message={'error_message': "error 1",
                 'executor_id': str(12345),
                 'exit_status': 1,
                 'nodename': socket.getfqdn()},
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
        message={'executor_id': str(12345),
                 'next_report_increment': 15},
        request_type='post')
    req.send_request(
        app_route='/job_instance/{}/log_running'.format(ji3),
        message={'nodename': socket.getfqdn(),
                 'process_group_id': str(os.getpid()),
                 'next_report_increment': 120},
        request_type='post')

    # Reset the job to REGISTERED
    req.send_request(
        app_route='/job/{}/reset'.format(job.job_id),
        message={},
        request_type='post')

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        jobs = DB.session.query(Job).filter_by(dag_id=real_dag_id,
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
        DB.session.commit()


def test_jsm_submit_job_attr(db_cfg, real_dag_id):

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

    # Create a job instance
    rc, response = req.send_request(
        app_route='/job_instance',
        message={'job_id': str(job.job_id),
                 'executor_type': 'dummy_exec'},
        request_type='post')
    ji = response['job_instance_id']
    req.send_request(
        app_route='/job_instance/{}/log_executor_id'.format(ji),
        message={'executor_id': str(12345),
                 'next_report_increment': 15},
        request_type='post')
    req.send_request(
        app_route='/job_instance/{}/log_running'.format(ji),
        message={'nodename': socket.getfqdn(),
                 'process_group_id': str(os.getpid()),
                 'next_report_increment': 120},
        request_type='post')

    req.send_request(
        app_route='/job_instance/{}/log_usage'.format(ji),
        message={'usage_str': 'used resources',
                 'wallclock': '0',
                 'maxrss': '1g',
                 'cpu': '00:00:00',
                 'io': '1'},
        request_type='post')

    # open new session on the db and ensure job stats are being loggged
    dict_of_attributes = {job_attribute.USAGE_STR: 'used resources',
                          job_attribute.WALLCLOCK: '0',
                          job_attribute.CPU: "00:00:00",
                          job_attribute.IO: "1",
                          job_attribute.MAXRSS: "1g"}

    req.send_request(
        app_route='/job_instance/{}/log_done'.format(ji),
        message={'nodename': socket.getfqdn()},
        request_type='post')

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        job_attribute_query = DB.session.execute("""
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
            assert (dict_of_attributes[attribute_entry_type] ==
                    attribute_entry_value)
        DB.session.commit()


testdata: dict = (
    ('CRITICAL', 'CRITICAL'),
    ('error', 'ERROR'),
    ('WarNING', 'WARNING'),
    ('iNFO', 'INFO'),
    ('DEBUg', 'DEBUG'),
    ('whatever', 'NOTSET')
)


@pytest.mark.parametrize("level,expected", testdata)
def test_dynamic_change_log_level(level: str, expected: str):
    # set log level to <level>
    rc, response = req.send_request(
        app_route='/log_level/{}'.format(level),
        message={},
        request_type='post')
    assert rc == 200
    # check log level is <expected>
    rc, response = req.send_request(
        app_route='/log_level',
        message={},
        request_type='get')
    assert rc == 200

    assert response['level'] == expected


def test_syslog_parameter():
    # get syslog status
    rc, response = req.send_request(
        app_route='/syslog_status',
        message={},
        request_type='get'
    )
    assert rc == 200
    assert response['syslog'] is False

    # try to attach a wrong port and expect failure
    rc, response = req.send_request(
        app_route='/attach_remote_syslog/debug/127.0.0.1/port/tcp',
        message={},
        request_type='post')
    assert rc == 400

    # get syslog status
    rc, response = req.send_request(
        app_route='/syslog_status',
        message={},
        request_type='get'
    )
    assert rc == 200
    assert response['syslog'] is False

    # try to attach a wrong port and expect failure
    rc, response = req.send_request(
        app_route='/attach_remote_syslog/debug/127.0.0.1/12345/tcp',
        message={},
        request_type='post')
    assert rc == 200

    # get syslog status
    rc, response = req.send_request(
        app_route='/syslog_status',
        message={},
        request_type='get'
    )
    assert rc == 200
    assert response['syslog']


def test_error_logger(real_jsm_jqs):
    # assert route returns no errors
    rc, response = req.send_request(
        app_route='/error_logger',
        message={"traceback": "foo bar baz"},
        request_type='post'
    )
    assert rc == 200


def test_set_flask_log_level_seperately(real_dag_id):
    print("----------------------------default------------------------")
    _, response = req.send_request(
        app_route='/job',
        message={'name': 'bar',
                 'job_hash': HASH,
                 'command': 'baz',
                 'dag_id': str(real_dag_id)},
        request_type='post')
    print("-------------------------flask info-----------------------")
    rc, response = req.send_request(
        app_route='/log_level/info',
        message={'loggers': ['flask']},
        request_type='post')
    assert rc == 200
    _, response = req.send_request(
        app_route='/job',
        message={'name': 'bar',
                 'job_hash': HASH,
                 'command': 'baz',
                 'dag_id': str(real_dag_id)},
        request_type='post')
    print("-------------------------all info-----------------------")
    rc, response = req.send_request(
        app_route='/log_level/info',
        message={'loggers': ['flask', 'jobmonServer']},
        request_type='post')
    assert rc == 200
    _, response = req.send_request(
        app_route='/job',
        message={'name': 'bar',
                 'job_hash': HASH,
                 'command': 'baz',
                 'dag_id': str(real_dag_id)},
        request_type='post')
    print("-------------------------flask error-----------------------")
    rc, response = req.send_request(
        app_route='/log_level/error',
        message={'loggers': ['flask']},
        request_type='post')
    assert rc == 200
    _, response = req.send_request(
        app_route='/job',
        message={'name': 'bar',
                 'job_hash': HASH,
                 'command': 'baz',
                 'dag_id': str(real_dag_id)},
        request_type='post')


def test_change_job_resources(db_cfg, real_dag_id):
    """ test that resources can be set and then changed and show up properly
    in the DB"""
    _, response = req.send_request(
        app_route='/job',
        message={'name': 'bar',
                 'job_hash': HASH,
                 'command': 'baz',
                 'dag_id': str(real_dag_id),
                 'max_attempts': '3'},
        request_type='post')
    job = Job.from_wire(response['job_dct'])
    _, response = req.send_request(
        app_route=f'/job/{job.job_id}/change_resources',
        message={'num_cores': '3',
                 'max_runtime_seconds': '20',
                 'mem_free': '2G'},
        request_type='put'
    )
    DB = db_cfg["DB"]
    app = db_cfg["app"]
    with app.app_context():
        query = """SELECT max_runtime_seconds, mem_free, num_cores
                   FROM job
                   WHERE job_id={job_id}""".format(job_id=job.job_id)
        runtime, mem, cores = DB.session.execute(query).fetchall()[0]
        assert runtime == 20
        assert mem == '2G'
        assert cores == 3
        DB.session.commit()

    _, response = req.send_request(
        app_route=f'/job/{job.job_id}/change_resources',
        message={'num_cores': '2'},
        request_type='put'
    )
    with app.app_context():
        query = """SELECT max_runtime_seconds, mem_free, num_cores
                   FROM job
                   WHERE job_id={job_id}""".format(job_id=job.job_id)
        runtime, mem, cores = DB.session.execute(query).fetchall()[0]
        assert runtime == 20
        assert mem == '2G'
        assert cores == 2
        DB.session.commit()


def test_executor_id_logging(db_cfg, real_dag_id):
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
        message={'executor_id': str(12345),
                 'next_report_increment': 15},
        request_type='post')
    req.send_request(
        app_route='/job_instance/{}/log_running'.format(job_instance_id),
        message={'nodename': socket.getfqdn(),
                 'process_group_id': str(os.getpid()),
                 'next_report_increment': 120,
                 'executor_id': str(54321)},
        request_type='post')
    req.send_request(
        app_route='/job_instance/{}/log_usage'.format(job_instance_id),
        message={'usage_str': 'used resources',
                 'wallclock': '0',
                 'maxrss': '1g',
                 'cpu': '00:00:00',
                 'io': '1'},
        request_type='post')
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        ji = DB.session.query(JobInstance).filter(
            JobInstance.job_instance_id == job_instance_id).first()
        assert ji.nodename == socket.getfqdn()
        assert ji.executor_id == 54321
        DB.session.commit()
    req.send_request(
        app_route='/job_instance/{}/log_report_by'.format(job_instance_id),
        message={'next_report_increment': 120,
                 'executor_id': str(55555)},
        request_type='post')
    with app.app_context():
        ji = DB.session.query(JobInstance).filter(
            JobInstance.job_instance_id == job_instance_id).first()
        assert ji.status == 'R'
        assert ji.executor_id == 55555
        DB.session.commit()
    req.send_request(
        app_route='/job_instance/{}/log_done'.format(job_instance_id),
        message={'nodename': socket.getfqdn(), 'executor_id': str(98765)},
        request_type='post')
    with app.app_context():
        ji = DB.session.query(JobInstance).filter(
            JobInstance.job_instance_id == job_instance_id).first()
        assert ji.status == 'D'
        assert ji.executor_id == 98765
        DB.session.commit()


def test_get_executor_id(real_dag_id):
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
        message={'executor_id': str(12345),
                 'next_report_increment': 15},
        request_type='post')
    req.send_request(
        app_route='/job_instance/{}/log_running'.format(job_instance_id),
        message={'nodename': socket.getfqdn(),
                 'process_group_id': str(os.getpid()),
                 'next_report_increment': 120},
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
        message={'job_instance_id': str(job_instance_id), 'nodename': socket.getfqdn()},
        request_type='post')

    # Check executor_id
    rc, response = req.send_request(
        app_route='/job_instance/{}/get_executor_id'.format(job_instance_id),
        message={},
        request_type='get'
    )
    assert rc == 200
    assert int(response['executor_id']) == 12345


def test_get_nodename(real_dag_id):
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
        message={'executor_id': str(12345),
                 'next_report_increment': 15},
        request_type='post')
    req.send_request(
        app_route='/job_instance/{}/log_running'.format(job_instance_id),
        message={'nodename': "mimi.ilovecat.org",
                 'process_group_id': str(os.getpid()),
                 'next_report_increment': 120},
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

    # Check nodename
    rc, response = req.send_request(
        app_route='/job_instance/{}/get_nodename'.format(job_instance_id),
        message={},
        request_type='get'
    )
    assert rc == 200
    assert response['nodename'] == "mimi.ilovecat.org"