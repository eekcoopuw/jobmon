from datetime import datetime
import getpass
import hashlib
import os
import random
import socket
from time import sleep
import pytest

from jobmon import config
from jobmon.client import shared_requester as req
from jobmon.client.swarm.job_management.swarm_job import SwarmJob
from jobmon.models.exceptions import InvalidStateTransition
from jobmon.models.executor_parameter_set_type import ExecutorParameterSetType
from jobmon.models.job import Job
from jobmon.models.job_instance_status import JobInstanceStatus
from jobmon.models.job_status import JobStatus
from jobmon.models.job_instance import JobInstance
from jobmon.models.workflow import Workflow
from jobmon.models.attributes.constants import job_attribute
from jobmon.server.server_logging import jobmonLogging as logging
from jobmon.serializers import SerializeExecutorJobInstance

HASH = 12345
SECOND_HASH = 12346


logger = logging.getLogger(__name__)


@pytest.fixture(scope='session')
def jsm_jqs(ephemera):
    """This sets up the JSM/JQS using the test_client which is a
    fake server
    """
    from jobmon.server import create_app
    from jobmon.server.config import ServerConfig

    # The create_app call sets up database connections
    server_config = ServerConfig(
        db_host=ephemera["DB_HOST"],
        db_port=ephemera["DB_PORT"],
        db_user=ephemera["DB_USER"],
        db_pass=ephemera["DB_PASS"],
        db_name=ephemera["DB_NAME"],
        wf_slack_channel=None,
        node_slack_channel=None,
        slack_token=None)
    app = create_app(server_config)
    app.config['TESTING'] = True
    client = app.test_client()
    yield client, client


def get_flask_content(response):
    """The function called by the no_request_jsm_jqs to query the fake
    test_client for a response
    """
    if 'application/json' in response.headers.get('Content-Type'):
        content = response.json
    elif 'text/html' in response.headers.get('Content-Type'):
        content = response.data
    else:
        content = response.content
    return response.status_code, content


@pytest.fixture(scope='function')
def no_requests_jsm_jqs(monkeypatch, jsm_jqs):
    """This function monkeypatches the requests library to use the
    test_client
    """
    import requests
    from jobmon.client import requester
    jsm_client, jqs_client = jsm_jqs

    def get_jqs(url, params, headers):
        url = "/" + url.split('/')[-1]
        return jqs_client.get(path=url, query_string=params, headers=headers)
    monkeypatch.setattr(requests, 'get', get_jqs)
    monkeypatch.setattr(requester, 'get_content', get_flask_content)

    def post_jsm(url, json, headers):
        url = "/" + url.split('/')[-1]
        return jsm_client.post(url, json=json, headers=headers)
    monkeypatch.setattr(requests, 'post', post_jsm)
    monkeypatch.setattr(requester, 'get_content', get_flask_content)


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
    swarm_job = SwarmJob.from_wire(response['job_dct'])

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
                 'working_dir': ""},
        request_type='post')
    wf_run_id = response['workflow_run_id']
    # make sure that the wf run that was just created matches the one that
    # jsm._get_workflow_run_id gets
    app = db_cfg["app"]
    DB = db_cfg["DB"]

    with app.app_context():
        job = DB.session.query(Job).filter_by(job_id=swarm_job.job_id).first()
        assert wf_run_id == _get_workflow_run_id(job)


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
    swarm_job = SwarmJob.from_wire(response['job_dct'])
    app = db_cfg["app"]
    DB = db_cfg["DB"]

    with app.app_context():
        job = DB.session.query(Job).filter_by(job_id=swarm_job.job_id).first()
        assert not _get_workflow_run_id(job)


def test_jsm_valid_done(real_dag_id, db_cfg):
    # add job
    _, response = req.send_request(
        app_route='/job',
        message={'name': 'bar',
                 'job_hash': HASH,
                 'command': 'baz',
                 'dag_id': str(real_dag_id)},
        request_type='post')
    swarm_job = SwarmJob.from_wire(response['job_dct'])

    # queue job
    req.send_request(
        app_route='/job/{}/queue'.format(swarm_job.job_id),
        message={},
        request_type='post')

    # add job instance
    _, response = req.send_request(
        app_route='/job_instance',
        message={'job_id': str(swarm_job.job_id),
                 'executor_type': 'dummy_exec'},
        request_type='post')
    job_instance_id = SerializeExecutorJobInstance.kwargs_from_wire(
        response['job_instance'])["job_instance_id"]

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
        message={'job_instance_id': str(job_instance_id),
                 'nodename': socket.getfqdn()},
        request_type='post')


def test_jsm_valid_error(real_dag_id, db_cfg):

    # add job
    _, response = req.send_request(
        app_route='/job',
        message={'name': 'bar',
                 'job_hash': HASH,
                 'command': 'baz',
                 'dag_id': str(real_dag_id)},
        request_type='post')
    swarm_job = SwarmJob.from_wire(response['job_dct'])

    # queue job
    req.send_request(
        app_route='/job/{}/queue'.format(swarm_job.job_id),
        message={},
        request_type='post')

    # add job instance
    _, response = req.send_request(
        app_route='/job_instance',
        message={'job_id': str(swarm_job.job_id),
                 'executor_type': 'dummy_exec'},
        request_type='post')
    job_instance_id = SerializeExecutorJobInstance.kwargs_from_wire(
        response['job_instance'])["job_instance_id"]

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
        app_route=f'/job_instance/{job_instance_id}/log_error_worker_node',
        message={'error_message': "this is an error message",
                 'executor_id': str(12345),
                 'error_state': JobInstanceStatus.ERROR,
                 'nodename': socket.getfqdn()},
        request_type='post')
    req.send_request(
        app_route='/log_oom/{}'.format(str(12345)),
        message={'error_message': "this is an error message",
                 'task_id': str(123),
                 'exit_status': 2,
                 'nodename': socket.getfqdn()},
        request_type='post')


def test_invalid_transition(real_dag_id, no_requests_jsm_jqs):

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
    swarm_job = SwarmJob.from_wire(response['job_dct'])

    # InvalidStateTransition gets raised cuz the orig ji was Instantiated
    # and then this command tries to transition it's state backwards to G
    with pytest.raises(InvalidStateTransition):
        rc, response = req.send_request(
            app_route='/job_instance',
            message={'job_id': str(swarm_job.job_id),
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
    swarm_job = SwarmJob.from_wire(response['job_dct'])

    # queue job
    req.send_request(
        app_route='/job/{}/queue'.format(swarm_job.job_id),
        message={},
        request_type='post')

    # add job instance
    _, response = req.send_request(
        app_route='/job_instance',
        message={'job_id': str(swarm_job.job_id),
                 'executor_type': 'dummy_exec'},
        request_type='post')
    job_instance_id = SerializeExecutorJobInstance.kwargs_from_wire(
        response['job_instance'])["job_instance_id"]

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
    swarm_job = SwarmJob.from_wire(response['job_dct'])
    req.send_request(
        app_route='/job/{}/queue'.format(swarm_job.job_id),
        message={},
        request_type='post')

    rc, response = req.send_request(
        app_route='/job_instance',
        message={'job_id': str(swarm_job.job_id),
                 'executor_type': 'dummy_exec'},
        request_type='post')
    job_instance_id = SerializeExecutorJobInstance.kwargs_from_wire(
        response['job_instance'])["job_instance_id"]

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
    swarm_job = SwarmJob.from_wire(response['job_dct'])
    req.send_request(
        app_route='/job/{}/queue'.format(swarm_job.job_id),
        message={},
        request_type='post')

    # Create a couple of job instances
    rc, response = req.send_request(
        app_route='/job_instance',
        message={'job_id': str(swarm_job.job_id),
                 'executor_type': 'dummy_exec'},
        request_type='post')
    ji1 = SerializeExecutorJobInstance.kwargs_from_wire(
        response['job_instance'])["job_instance_id"]
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
        app_route='/job_instance/{}/log_error_worker_node'.format(ji1),
        message={'error_message': "error 1",
                 'executor_id': str(12345),
                 'error_state': JobInstanceStatus.ERROR,
                 'nodename': socket.getfqdn()},
        request_type='post')

    # second job instance
    rc, response = req.send_request(
        app_route='/job_instance',
        message={'job_id': str(swarm_job.job_id),
                 'executor_type': 'dummy_exec'},
        request_type='post')
    ji2 = SerializeExecutorJobInstance.kwargs_from_wire(
        response['job_instance'])["job_instance_id"]
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
        app_route='/job_instance/{}/log_error_worker_node'.format(ji2),
        message={'error_message': "error 1",
                 'executor_id': str(12345),
                 'error_state': JobInstanceStatus.ERROR,
                 'nodename': socket.getfqdn()},
        request_type='post')

    # third job instance
    rc, response = req.send_request(
        app_route='/job_instance',
        message={'job_id': str(swarm_job.job_id),
                 'executor_type': 'dummy_exec'},
        request_type='post')
    ji3 = SerializeExecutorJobInstance.kwargs_from_wire(
        response['job_instance'])["job_instance_id"]
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
        app_route='/job/{}/reset'.format(swarm_job.job_id),
        message={},
        request_type='post')

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        jobs = DB.session.query(Job).filter_by(dag_id=real_dag_id,
                                               job_id=swarm_job.job_id).all()
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
    swarm_job = SwarmJob.from_wire(response['job_dct'])
    req.send_request(
        app_route='/job/{}/queue'.format(swarm_job.job_id),
        message={},
        request_type='post')

    # Create a job instance
    rc, response = req.send_request(
        app_route='/job_instance',
        message={'job_id': str(swarm_job.job_id),
                 'executor_type': 'dummy_exec'},
        request_type='post')
    ji = SerializeExecutorJobInstance.kwargs_from_wire(
        response['job_instance'])["job_instance_id"]
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
        job_attribute_query = DB.session.execute(
            """
            SELECT job_attribute.id,
                   job_attribute.job_id,
                   job_attribute.attribute_type,
                   job_attribute.value
            FROM job_attribute
            JOIN job
            ON job_attribute.job_id=job.job_id
            WHERE job_attribute.job_id={id}
            """.format(id=swarm_job.job_id))
        attribute_entries = job_attribute_query.fetchall()
        for entry in attribute_entries:
            attribute_entry_type = entry.attribute_type
            attribute_entry_value = entry.value
            assert (dict_of_attributes[attribute_entry_type] ==
                    attribute_entry_value)
        DB.session.commit()


def test_syslog_parameter(env_var):
    # get syslog status
    rc, response = req.send_request(
        app_route='/syslog_status',
        message={},
        request_type='get'
    )
    assert rc == 200
    assert response['syslog'] == config.use_rsyslog

    # try to attach a wrong port and expect failure
    rc, response = req.send_request(
        app_route='/attach_remote_syslog/debug/127.0.0.1/port/tcp',
        message={},
        request_type='post')
    assert rc == 400


def test_error_logger(env_var):
    # assert route returns no errors
    rc, response = req.send_request(
        app_route='/error_logger',
        message={"traceback": "foo bar baz"},
        request_type='post'
    )
    assert rc == 200


def test_set_flask_log_level_seperately(db_cfg, real_dag_id):
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
    swarm_job = SwarmJob.from_wire(response['job_dct'])
    _, response = req.send_request(
        app_route=f'/job/{swarm_job.job_id}/update_resources',
        message={'parameter_set_type': ExecutorParameterSetType.ADJUSTED,
                 'num_cores': '3',
                 'max_runtime_seconds': '20',
                 'm_mem_free': 2},
        request_type='post'
    )
    DB = db_cfg["DB"]
    app = db_cfg["app"]
    with app.app_context():
        query = """SELECT max_runtime_seconds, m_mem_free, num_cores
                   FROM job JOIN executor_parameter_set
                   ON job.executor_parameter_set_id = executor_parameter_set.id
                   WHERE job.job_id={job_id}""".format(job_id=swarm_job.job_id)
        runtime, mem, cores = DB.session.execute(query).fetchall()[0]
        assert runtime == 20
        assert mem == 2
        assert cores == 3
        DB.session.commit()


def test_executor_id_logging(db_cfg, real_dag_id):
    _, response = req.send_request(
        app_route='/job',
        message={'name': 'bar',
                 'job_hash': HASH,
                 'command': 'baz',
                 'dag_id': str(real_dag_id)},
        request_type='post')
    swarm_job = SwarmJob.from_wire(response['job_dct'])
    req.send_request(
        app_route='/job/{}/queue'.format(swarm_job.job_id),
        message={},
        request_type='post')

    rc, response = req.send_request(
        app_route='/job_instance',
        message={'job_id': str(swarm_job.job_id),
                 'executor_type': 'dummy_exec'},
        request_type='post')
    job_instance_id = SerializeExecutorJobInstance.kwargs_from_wire(
        response['job_instance'])["job_instance_id"]
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


def test_on_transition_get_kill(real_dag_id, db_cfg):
    # add job
    _, response = req.send_request(
        app_route='/job',
        message={'name': 'bar',
                 'job_hash': HASH,
                 'command': 'baz',
                 'dag_id': str(real_dag_id)},
        request_type='post')
    swarm_job = SwarmJob.from_wire(response['job_dct'])

    # queue job
    req.send_request(
        app_route='/job/{}/queue'.format(swarm_job.job_id),
        message={},
        request_type='post')

    # add job instance
    _, response = req.send_request(
        app_route='/job_instance',
        message={'job_id': str(swarm_job.job_id),
                 'executor_type': 'dummy_exec'},
        request_type='post')
    job_instance_id = SerializeExecutorJobInstance.kwargs_from_wire(
        response['job_instance'])["job_instance_id"]

    DB = db_cfg["DB"]
    app = db_cfg["app"]
    with app.app_context():
        DB.session.execute("""
            UPDATE job_instance
            SET job_instance.status='W'
            WHERE job_instance_id = {}""".format(job_instance_id))
        DB.session.commit()

    # the job does not get registered properly and is set to 'W', then it
    # tries to log running and gets a message to kill itself
    _, resp = req.send_request(
        app_route='/job_instance/{}/log_running'.format(job_instance_id),
        message={'nodename': socket.getfqdn(),
                 'process_group_id': str(os.getpid()),
                 'next_report_increment': 120},
        request_type='post')
    assert resp['message'] == 'kill self'


def test_log_error_reconciler(db_cfg, real_dag_id):
    next_report_increment = 10
    # add job
    _, response = req.send_request(
        app_route='/job',
        message={'name': 'bar',
                 'job_hash': HASH,
                 'command': 'baz',
                 'dag_id': str(real_dag_id)},
        request_type='post')
    swarm_job = SwarmJob.from_wire(response['job_dct'])

    # queue job
    req.send_request(
        app_route='/job/{}/queue'.format(swarm_job.job_id),
        message={},
        request_type='post')

    # add job instance
    _, response = req.send_request(
        app_route='/job_instance',
        message={'job_id': str(swarm_job.job_id),
                 'executor_type': 'dummy_exec'},
        request_type='post')
    job_instance_id = SerializeExecutorJobInstance.kwargs_from_wire(
        response['job_instance'])["job_instance_id"]
    req.send_request(
        app_route=f'/job_instance/{job_instance_id}/log_executor_id',
        message={'executor_id': str(12345),
                 'next_report_increment': next_report_increment},
        request_type='post')

    # try to log unknown immediately. Should do nothing because we aren't
    # beyond the report_by date: utcnow() + 'next_report_increment'
    req.send_request(
        app_route=f'/job_instance/{job_instance_id}/log_error_reconciler',
        message={
            "error_message": "foo",
            "error_state": JobInstanceStatus.UNKNOWN_ERROR
        },
        request_type='post')
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        job_instance = DB.session.query(JobInstance).filter(
            JobInstance.job_instance_id == job_instance_id).first()
        assert job_instance.status == (
            JobInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR)

    # sleep till we are passed the next report increment then try again
    sleep(next_report_increment)
    req.send_request(
        app_route=f'/job_instance/{job_instance_id}/log_error_reconciler',
        message={
            "error_message": "foo",
            "error_state": JobInstanceStatus.UNKNOWN_ERROR
        },
        request_type='post')
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        job_instance = DB.session.query(JobInstance).filter(
            JobInstance.job_instance_id == job_instance_id).first()
        assert job_instance.status == (
            JobInstanceStatus.UNKNOWN_ERROR)


def test_get_executor_id(db_cfg, real_dag_id):
    # add job
    _, response = req.send_request(
        app_route='/job',
        message={'name': 'bar',
                 'job_hash': HASH,
                 'command': 'baz',
                 'dag_id': str(real_dag_id)},
        request_type='post')
    swarm_job = SwarmJob.from_wire(response['job_dct'])

    # queue job
    req.send_request(
        app_route='/job/{}/queue'.format(swarm_job.job_id),
        message={},
        request_type='post')

    # add job instance
    _, response = req.send_request(
        app_route='/job_instance',
        message={'job_id': str(swarm_job.job_id),
                 'executor_type': 'dummy_exec'},
        request_type='post')
    job_instance_id = response['job_instance'][0]

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
        message={'job_instance_id': str(job_instance_id),
                 'nodename': socket.getfqdn()},
        request_type='post')

    # Check executor_id
    rc, response = req.send_request(
        app_route='/job_instance/{}/get_executor_id'.format(job_instance_id),
        message={},
        request_type='get'
    )
    assert rc == 200
    assert int(response['executor_id']) == 12345


def test_get_nodename(db_cfg, real_dag_id):
    # add job
    _, response = req.send_request(
        app_route='/job',
        message={'name': 'bar',
                 'job_hash': HASH,
                 'command': 'baz',
                 'dag_id': str(real_dag_id)},
        request_type='post')
    swarm_job = SwarmJob.from_wire(response['job_dct'])

    # queue job
    req.send_request(
        app_route='/job/{}/queue'.format(swarm_job.job_id),
        message={},
        request_type='post')

    # add job instance
    _, response = req.send_request(
        app_route='/job_instance',
        message={'job_id': str(swarm_job.job_id),
                 'executor_type': 'dummy_exec'},
        request_type='post')
    job_instance_id = response['job_instance'][0]

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
