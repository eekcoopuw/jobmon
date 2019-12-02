import os
import pytest

import socket
from time import sleep
from jobmon.client import shared_requester as req
from jobmon.client.swarm.job_management.swarm_job import SwarmJob
from jobmon.models.job_instance_status import JobInstanceStatus
from jobmon.models.workflow import Workflow
import logging
from jobmon.serializers import SerializeExecutorJobInstance


logger = logging.getLogger(__name__)


def test_job_status(db_cfg, real_dag_id):
    _, response = req.send_request(
        app_route='/job',
        message={'name': 'bar',
                 'job_hash': 12334,
                 'command': 'baz',
                 'dag_id': str(real_dag_id)},
        request_type='post')
    swarm_job = SwarmJob.from_wire(response['job_dct'])

    _, response = req.send_request(app_route=f'/dag/{real_dag_id}/job_status',
                                   message={}, request_type='get')
    assert response['job_dcts'][0][1] == '12334' and \
           len(response['job_dcts'][0]) == 3


def setup_running_job(db_cfg, real_dag_id):
    _, response = req.send_request(
        app_route='/job',
        message={'name': 'bar',
                 'job_hash': 12334,
                 'command': 'baz',
                 'dag_id': str(real_dag_id)},
        request_type='post')
    swarm_job = SwarmJob.from_wire(response['job_dct'])

    req.send_request(
        app_route=f'/job/{swarm_job.job_id}/update_resources',
        message={
            'parameter_set_type': 'O',
            'max_runtime_seconds': 2,
            'context_args': '{}',
            'queue': 'all.q',
            'num_cores': 2,
            'm_mem_free': 1,
            'j_resource': False,
            'resource_scales': "{'m_mem_free': 0.5, 'max_runtime_seconds': 0.5}",
            'hard_limits': False},
        request_type='post')

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


def test_timed_out_jobs(db_cfg, real_dag_id):
    setup_running_job(db_cfg, real_dag_id)

    sleep(3)  # sleep 3 seconds to exceed the max runtime

    _, response = req.send_request(
        app_route=f'/dag/{real_dag_id}/get_timed_out_executor_ids',
        message={},
        request_type='get')

    assert response['jiid_exid_tuples'][0] == [1, 12345]


def test_get_job_instances_by_status(db_cfg, real_dag_id):
    setup_running_job(db_cfg, real_dag_id)

    _, response = req.send_request(
        app_route=f'/dag/{real_dag_id}/get_job_instances_by_status',
        message={'status': JobInstanceStatus.RUNNING},
        request_type='get')

    assert response['job_instances'][0]['status'] == 'R'
    assert response['job_instances'][0]['dag_id'] == real_dag_id


def test_lost_job_instances(db_cfg, real_dag_id):
    setup_running_job(db_cfg, real_dag_id)

    # dag heartbeat
    req.send_request(
        app_route='/task_dag/{}/log_heartbeat'.format(real_dag_id),
        message={},
        request_type='post')

    # change report-by date to be expired
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        new_report_by = f"""UPDATE job_instance
                           SET report_by_date = 
                           SUBTIME(UTC_TIMESTAMP(), SEC_TO_TIME(60))
                           WHERE dag_id = {real_dag_id}
                        """
        DB.session.execute(new_report_by)
        DB.session.commit()

    _, response = req.send_request(
        app_route=f'/dag/{real_dag_id}/get_suspicious_job_instances',
        message={},
        request_type='get'
    )
    assert len(response['job_instances']) == 1
    assert response['job_instances'][0][1] == 12345


def create_workflow_runs(db_cfg, real_dag_id):
    rc, response = req.send_request(
        app_route='/workflow',
        message={'dag_id': real_dag_id,
                 'workflow_args': 'test_dup_args',
                 'workflow_hash': '123fe34gr',
                 'name': 'dup_args',
                 'description': '',
                 'user': 'user'},
        request_type='post')
    wf = Workflow.from_wire(response['workflow_dct'])

    rc, response = req.send_request(
        app_route='/workflow_run',
        message={'workflow_id': wf.id,
                 'user': 'user',
                 'hostname': 'test.host.ihme.washington.edu',
                 'pid': 123,
                 'stderr': '/',
                 'stdout': '/',
                 'project': 'proj_tools',
                 'slack_channel': '',
                 'executor_class': 'SGEExecutor',
                 'working_dir': '/'},
        request_type='post')
    wfr_id1 = response['workflow_run_id']

    rc, response = req.send_request(
        app_route='/workflow_run',
        message={'workflow_id': wf.id,
                 'user': 'user',
                 'hostname': 'test.host.ihme.washington.edu',
                 'pid': 123,
                 'stderr': '/',
                 'stdout': '/',
                 'project': 'proj_tools',
                 'slack_channel': '',
                 'executor_class': 'SGEExecutor',
                 'working_dir': '/'},
        request_type='post')
    wfr_id2 = response['workflow_run_id']
    return wf.id, wf.workflow_args, wfr_id1, wfr_id2


def test_workflow_args(db_cfg, real_dag_id):
    wf_id, wf_args, _, _ = create_workflow_runs(db_cfg, real_dag_id)

    rc, response = req.send_request(
        app_route='/workflow/workflow_args',
        message={'workflow_args': str(wf_args)},
        request_type='get'
    )
    assert response['workflow_hashes'][0][0] == '123fe34gr'


def test_job_inst_wf_run(db_cfg, real_dag_id):
    wf_id, wf_args, wfr1, wfr2 = create_workflow_runs(db_cfg, real_dag_id)
    setup_running_job(db_cfg, real_dag_id)

    rc, response = req.send_request(
        app_route=f'/workflow_run/{wfr1}/job_instance',
        message={},
        request_type='get'
    )
    assert len(response['job_instances']) == 0

    rc, response = req.send_request(
        app_route=f'/workflow_run/{wfr2}/job_instance',
        message={},
        request_type='get'
    )
    assert response['job_instances'][0][1] == 12345


def test_workflow_status(real_dag_id):
    rc, response = req.send_request(
        app_route='/workflow',
        message={'dag_id': real_dag_id,
                 'workflow_args': 'test_dup_args',
                 'workflow_hash': '123fe34gr',
                 'name': 'dup_args',
                 'description': '',
                 'user': 'user'},
        request_type='post')

    wf = Workflow.from_wire(response['workflow_dct'])
    code, response = req.send_request(
        app_route='/workflow/{}/status'.format(wf.id),
        message={},
        request_type='get')
    assert response == 'C'
