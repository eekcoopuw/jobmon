import pytest
from jobmon.requester import Requester
from jobmon.models import Status


def test_req_jobmon_pair(central_jobmon):
    req = Requester(central_jobmon.out_dir)

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

    # Update a jobs status and check that it gets commited to persistent
    # store
    req.send_request({'action': 'update_job_status',
                      'kwargs': {'jid': 2, 'status_id': Status.FAILED}})
    assert (
        [j.monitored_jid for j in
         central_jobmon.jobs_with_status(Status.SUBMITTED)] == [1, 3, 4])
    assert (
        [j.monitored_jid for j in
         central_jobmon.jobs_with_status(Status.FAILED)] == [2])
