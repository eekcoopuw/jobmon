import os
import pytest
import sys

from jobmon.connection_config import ConnectionConfig
from jobmon.subscriber import Subscriber
from jobmon.publisher import PublisherTopics
from jobmon.requester import Requester
from jobmon.models import Status
from jobmon.exceptions import ReturnCodes
from jobmon.job import Job

try:
    from jobmon.executors.sge_exec import SGEJobInstance
except KeyError:
    pass


def test_req_jobmon_pair(central_jobmon):
    req = Requester(monitor_connection=ConnectionConfig(central_jobmon.out_dir))

    # Test basic connection
    resp = req.send_request({'action': 'alive'})
    assert resp[0] == 0


def test_invalid_req_args():

    with pytest.raises(ValueError):
        Requester(ConnectionConfig(monitor_dir="some_dir", monitor_host='localhost', monitor_port=3459))
    with pytest.raises(ValueError):
        Requester(ConnectionConfig(monitor_dir=None, monitor_host=None, monitor_port=None))
    with pytest.raises(ValueError):
        Requester(ConnectionConfig(monitor_host='localhost'))
    with pytest.raises(ValueError):
        Requester(ConnectionConfig(monitor_port=1234))


def test_static_port_req_mon_pair(central_jobmon_static_port):
    con_3459 = ConnectionConfig(monitor_host='localhost', monitor_port=3459)
    req = Requester(con_3459)

    # Test basic connection
    resp = req.send_request({'action': 'alive'})
    assert resp[0] == 0


def test_batch_registration(central_jobmon_static_port):

    con_3459 = ConnectionConfig(monitor_host='localhost', monitor_port=3459)
    req = Requester(con_3459)

    # Test basic connection
    batch = req.send_request({'action': 'register_batch',
                             'kwargs': {'name': 'test_job_batch',
                                        'user': 'test_user'}})
    req.send_request({'action': 'register_job',
                      'kwargs': {'name': 'a test job',
                                 'batch_id': batch[1]}})
    req.send_request({'action': 'register_job',
                      'kwargs': {'name': 'a test job 2',
                                 'batch_id': batch[1]}})

    # Test via Job object
    jb = req.send_request({'action': 'batch',
                           'kwargs': {'name': 'Job_batch',
                                      'user': 'test_user'}})
    job = Job(monitor_connection=con_3459, batch_id=jb[1])
    jid = job.register_with_monitor()
    job_record = central_jobmon_static_port._action_get_job_information(jid)[1]
    assert job_record['batch_id'] == jb[1]


def test_job_registration_update(central_jobmon):
    req = Requester(ConnectionConfig(monitor_dir=central_jobmon.out_dir))

    # Test job creation and status updating
    req.send_request({'action': 'register_job'})
    jr = req.send_request({'action': 'register_job',
                           'kwargs': {'name': 'a test job'}})

    jr_id = jr[1]
    jir = req.send_request({'action': 'register_job_instance',
                            'kwargs': {'job_instance_id': 1234,
                                       'jid': jr_id}})
    ji_id = jir[1]
    up_resp = req.send_request({'action': 'update_job_instance_status',
                                'kwargs': {'job_instance_id': ji_id,
                                           'status_id': Status.FAILED}})
    assert up_resp == [0, ji_id, Status.FAILED]


def test_sge_job_registration(central_jobmon):
    req = Requester(ConnectionConfig(monitor_dir=central_jobmon.out_dir))

    # Test job creation and status updating
    req.send_request({'action': 'register_job'})
    jr2 = req.send_request({'action': 'register_job',
                            'kwargs': {'name': 'a test job'}})
    assert jr2[0] == ReturnCodes.OK


@pytest.mark.cluster
def test_sgejob_mon_pair(central_jobmon_cluster):
    os.environ["JOB_ID"] = "1234"
    os.environ["JOB_NAME"] = "job1"
    j1 = SGEJobInstance(ConnectionConfig(monitor_dir=central_jobmon_cluster.out_dir))
    os.environ["JOB_ID"] = "5678"
    os.environ["JOB_NAME"] = "job2"
    j2 = SGEJobInstance(ConnectionConfig(monitor_dir=central_jobmon_cluster.out_dir))
    j1.log_started()
    assert (
        [j.jid for j in
         central_jobmon_cluster.jobs_with_status(Status.RUNNING)] == [1])
    j2.log_started()
    assert (
        [j.jid for j in
         central_jobmon_cluster.jobs_with_status(Status.RUNNING)] == [1, 2])
    j1.log_completed()
    assert (
        [j.jid for j in
         central_jobmon_cluster.jobs_with_status(Status.RUNNING)] == [2])
    assert (
        [j.jid for j in
         central_jobmon_cluster.jobs_with_status(Status.COMPLETE)] == [1])


@pytest.mark.cluster
def test_monitor_job_by_status_query(central_jobmon_cluster):
    req = Requester(ConnectionConfig(monitor_dir=central_jobmon_cluster.out_dir))

    # Test job registration and status updating
    os.environ["JOB_ID"] = "1"
    os.environ["JOB_NAME"] = "job1"
    SGEJobInstance(ConnectionConfig(monitor_dir=central_jobmon_cluster.out_dir))
    os.environ["JOB_ID"] = "2"
    os.environ["JOB_NAME"] = "job2"
    SGEJobInstance(ConnectionConfig(monitor_dir=central_jobmon_cluster.out_dir))
    os.environ["JOB_ID"] = "3"
    os.environ["JOB_NAME"] = "job3"
    SGEJobInstance(ConnectionConfig(monitor_dir=central_jobmon_cluster.out_dir))

    assert (
        [j.name for j in
         central_jobmon_cluster.jobs_with_status(Status.SUBMITTED)] == [
            "job1", "job2", "job3"])

    os.environ["JOB_ID"] = "4"
    os.environ["JOB_NAME"] = "job4"
    SGEJobInstance(ConnectionConfig(monitor_dir=central_jobmon_cluster.out_dir))
    assert (
        [j.name for j in
         central_jobmon_cluster.jobs_with_status(Status.SUBMITTED)] == [
            "job1", "job2", "job3", "job4"])
    # Update a job's status and check that it gets committed to persistent
    # store
    req.send_request({'action': 'update_job_instance_status',
                      'kwargs': {'job_instance_id': 2,
                                 'status_id': Status.FAILED}})
    assert (
        [j.name for j in
         central_jobmon_cluster.jobs_with_status(Status.SUBMITTED)] == [
            "job1", "job3", "job4"])

    assert (
        [j.name for j in
         central_jobmon_cluster.jobs_with_status(Status.FAILED)] == [
            "job2"])


@pytest.mark.cluster
def test_get_job_information_query(central_jobmon_cluster):

    # Test job registration with sge-id's.  I like prime numbers.
    os.environ["JOB_ID"] = "17"
    os.environ["JOB_NAME"] = "job1"
    j17 = SGEJobInstance(ConnectionConfig(monitor_dir=central_jobmon_cluster.out_dir))

    job_info = central_jobmon_cluster._action_get_job_instance_information(17)
    assert job_info[0] == ReturnCodes.OK
    status = job_info[1]['current_status']
    assert status == Status.SUBMITTED

    # No job with ID 99
    job_info = central_jobmon_cluster._action_get_job_instance_information(99)
    assert job_info[0] == ReturnCodes.NO_RESULTS

    # Update a job's status and check that it gets committed to persistent
    # store
    j17.log_failed()
    job_info = central_jobmon_cluster._action_get_job_instance_information(17)
    assert job_info[0] == ReturnCodes.OK
    status = job_info[1]['current_status']
    assert status == Status.FAILED

import time
@pytest.mark.cluster
def test_pub(central_jobmon_cluster, central_jobmon_static_port):

    s = Subscriber(ConnectionConfig(monitor_dir=central_jobmon_cluster.out_dir, monitor_filename="publisher_info.json"))
    s.connect(topicfilter=PublisherTopics.JOB_STATE.value)
#, name="a-name",runfile="a-runfile",args='some-args'
    if sys.version_info < (3, 0):
        os.environ["JOB_ID"] = "140465072"
        os.environ["JOB_NAME"] = "QLOGIN"
        j1 = SGEJobInstance(ConnectionConfig(monitor_dir=central_jobmon_cluster.out_dir))
        j1.log_completed()
    else:
        central_jobmon_static_port._action_register_job_instance(140465072)
        central_jobmon_static_port._action_update_job_instance_status(140465072, 3)
    retries = 0
    update = None
    while update is None and retries < 10:
        update = s.receive_update()
        retries += 1
        time.sleep(1)
    assert update is not None, (update)


def test_pub_static(central_jobmon_static_port):

    # @TODO Change monitor_host to just host? similar for port
    s = Subscriber(ConnectionConfig(monitor_host='localhost', monitor_port=5678))
    s.connect(topicfilter=PublisherTopics.JOB_STATE.value)

    os.environ["JOB_ID"] = "1"
    os.environ["JOB_NAME"] = "job1"
    j1 = SGEJobInstance(ConnectionConfig(monitor_host='localhost', monitor_port=3459))
    j1.log_completed()

    # This is a python3 hack... for some reason having a requester and
    # subscriber in the same process causes the subscriber to not receive
    # messages...  Maybe related to the zmq.Context. See this post:
    # https://stackoverflow.com/questions/31396074/pyzmq-subscriber-doesnt-receive-messages-when-working-with-request-socket
    # central_jobmon_static_port._action_update_job_instance_status(1, 3)
    update = s.receive_update()
    assert update is not None
