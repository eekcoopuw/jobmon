import os

import pytest

from jobmon.requester import Requester
from jobmon.models import Status
from jobmon.job import Job, SGEJob
from jobmon.exceptions import ReturnCodes


def test_req_jobmon_pair(central_jobmon):
    req = Requester(central_jobmon.out_dir)

    # Test basic connection
    resp = req.send_request({'action': 'alive'})
    assert resp[0] == 0


def test_invalid_req_args():
    with pytest.raises(ValueError):
        Requester(out_dir='some_dir', monitor_host='localhost', port=3459)
    with pytest.raises(ValueError):
        Requester(out_dir=None, monitor_host=None, port=None)
    with pytest.raises(ValueError):
        Requester(monitor_host='localhost')
    with pytest.raises(ValueError):
        Requester(monitor_port=1234)


def test_static_port_req_mon_pair(central_jobmon_static_port):
    req = Requester(monitor_host='localhost', port=3459)

    # Test basic connection
    resp = req.send_request({'action': 'alive'})
    assert resp[0] == 0


def test_job_registration_update(central_jobmon):
    req = Requester(central_jobmon.out_dir)

    # Test job creation and status updating
    req.send_request({'action': 'register_job'})
    jr2 = req.send_request({'action': 'register_job',
                            'kwargs': {'name': 'a test job'}})
    jr2_id = jr2[1]
    up_resp = req.send_request({'action': 'update_job_status',
                                'kwargs': {'jid': jr2_id,
                                           'status_id': Status.FAILED}})
    assert up_resp == [0, 2, Status.FAILED]


def test_sge_job_registration(central_jobmon):
    req = Requester(central_jobmon.out_dir)

    # Test job creation and status updating
    req.send_request({'action': 'register_job'})
    jr2 = req.send_request({'action': 'register_job',
                            'kwargs': {'name': 'a test job'}})
    jr2_id = jr2[1]
    up_resp = req.send_request({'action': 'update_job_status',
                                'kwargs': {'jid': jr2_id,
                                           'status_id': Status.FAILED}})
    assert up_resp == [0, 2, Status.FAILED]


def test_job_mon_pair(central_jobmon):
    j = Job(central_jobmon.out_dir)
    j.log_started()
    j.log_completed()


def test_sgejob_mon_pair(central_jobmon):
    os.environ["JOB_ID"] = "1234"
    os.environ["JOB_NAME"] = "job1"
    j1 = SGEJob(central_jobmon.out_dir)
    os.environ["JOB_ID"] = "5678"
    os.environ["JOB_NAME"] = "job2"
    j2 = SGEJob(central_jobmon.out_dir)
    j1.log_started()
    assert (
        [j.monitored_jid for j in
         central_jobmon.jobs_with_status(Status.RUNNING)] == [1])
    j2.log_started()
    assert (
        [j.monitored_jid for j in
         central_jobmon.jobs_with_status(Status.RUNNING)] == [1, 2])
    j1.log_completed()
    assert (
        [j.monitored_jid for j in
         central_jobmon.jobs_with_status(Status.RUNNING)] == [2])
    assert (
        [j.monitored_jid for j in
         central_jobmon.jobs_with_status(Status.COMPLETE)] == [1])


def test_monitor_job_by_status_query(central_jobmon):
    req = Requester(central_jobmon.out_dir)

    # Test job registration and status updating
    req.send_request({'action': 'register_job'})
    req.send_request({'action': 'register_job'})
    req.send_request({'action': 'register_job'})

    assert (
        [j.monitored_jid for j in
         central_jobmon.jobs_with_status(Status.SUBMITTED)] == [1, 2, 3])

    req.send_request({'action': 'register_job'})
    assert (
        [j.monitored_jid for j in
         central_jobmon.jobs_with_status(Status.SUBMITTED)] == [1, 2, 3, 4])

    # Update a job's status and check that it gets committed to persistent
    # store
    req.send_request({'action': 'update_job_status',
                      'kwargs': {'jid': 2, 'status_id': Status.FAILED}})
    assert (
        [j.monitored_jid for j in
         central_jobmon.jobs_with_status(Status.SUBMITTED)] == [1, 3, 4])
    assert (
        [j.monitored_jid for j in
         central_jobmon.jobs_with_status(Status.FAILED)] == [2])


def test_get_job_information_query(central_jobmon):
    req = Requester(central_jobmon.out_dir)

    # Test job registration with sge-id's.  I like prime numbers.
    os.environ["JOB_ID"] = "17"
    os.environ["JOB_NAME"] = "job1"
    j17 = SGEJob(central_jobmon.out_dir)

    job_info = central_jobmon._action_get_sgejob_information(17)
    assert job_info[0] == ReturnCodes.OK
    status = job_info[1]['current_status']
    assert status == Status.SUBMITTED

    # No job with ID 99
    job_info = central_jobmon._action_get_sgejob_information(99)
    assert job_info[0] == ReturnCodes.NO_RESULTS

    # Update a job's status and check that it gets committed to persistent
    # store
    j17.log_failed()
    job_info = central_jobmon._action_get_sgejob_information(17)
    assert job_info[0] == ReturnCodes.OK
    status = job_info[1]['current_status']
    assert status == Status.FAILED
