import pytest
from threading import Thread
from time import sleep
from unittest import mock

from jobmon.server.integration.qpid.worker import MaxpssQ, maxpss_forever
from jobmon.client import shared_requester as req
from jobmon.client import client_config
from jobmon.client.swarm.executors.sge import SGEExecutor
from jobmon.models.job_status import JobStatus
from jobmon.client.swarm.workflow.bash_task import BashTask
from jobmon.client.swarm.workflow.task_dag import DagExecutionStatus


@pytest.mark.unittest
def test_MaxpssQ():
    """This is to test the Q stops increasing when the max size is reached."""
    if MaxpssQ._q is not None:
        print("Q already initiated. Skip the test.")
        return
    # Q: ()
    MaxpssQ(100)
    # get from empty queue
    assert MaxpssQ().get() is None
    # put into queue
    # Q: ((1,0))
    MaxpssQ().put(1)
    assert MaxpssQ().get_size() == 1
    # Q: ((1,0), (2, 1))
    MaxpssQ().put(2, 1)
    assert MaxpssQ().get_size() == 2
    # overflow
    for i in range(110):
        MaxpssQ().put(3)
    assert MaxpssQ().get_size() == 100
    # Test Queue Content
    # Q: ((2, 1))
    e1 = MaxpssQ().get()
    assert e1[0] == 1
    assert e1[1] == 0
    # Q: ()
    e2 = MaxpssQ().get()
    assert e2[0] == 2
    assert e2[1] == 1


@pytest.mark.unittest
def test_worker_with_mock_200():
    """This is to test the job with maxpss leaves the Q."""
    MaxpssQ().empty_q()
    MaxpssQ.keep_running = True
    assert MaxpssQ().get_size() == 0
    with mock.patch('jobmon.server.integration.qpid.worker._update_maxpss_in_db') as m_db, \
         mock.patch('jobmon.server.integration.qpid.worker._get_qpid_response') as m_restful:
        m_db.return_value = True
        m_restful.return_value = (200, 500)
        MaxpssQ().put(1)
        assert MaxpssQ().get_size() == 1
        t = Thread(target=maxpss_forever)
        t.start()
        t.join(10)
        for i in range(5):
            sleep(2)
            if MaxpssQ().get_size() == 0:
                break
        MaxpssQ.keep_running = False
        assert MaxpssQ().get_size() == 0


@pytest.mark.unittest
def test_worker_with_mock_404():
    """This is to test the job without maxpss will be put back to the Q with age increased."""
    MaxpssQ().empty_q()
    MaxpssQ.keep_running = True
    assert MaxpssQ().get_size() == 0
    with mock.patch('jobmon.server.integration.qpid.worker._get_qpid_response') as m_restful:
        m_restful.return_value = (404, None)
        MaxpssQ().put(1)
        assert MaxpssQ().get_size() == 1
        t = Thread(target=maxpss_forever)
        t.start()
        t.join(10)
        for i in range(5):
            sleep(2)
            if MaxpssQ().get_size() == 0:
                break
        MaxpssQ.keep_running = False
        assert MaxpssQ().get_size() == 1
        r = MaxpssQ().get()
        assert r[0] == 1
        assert r[1] > 0


@pytest.mark.unittest
def test_worker_with_mock_500():
    """This is to test the job will be put back to the Q with age increated when QPID is down."""
    MaxpssQ().empty_q()
    MaxpssQ.keep_running = True
    assert MaxpssQ().get_size() == 0
    with mock.patch('jobmon.server.integration.qpid.worker._get_qpid_response') as m_restful:
        m_restful.return_value = (500, None)
        MaxpssQ().put(1)
        assert MaxpssQ().get_size() == 1
        t = Thread(target=maxpss_forever)
        t.start()
        t.join(10)
        for i in range(5):
            sleep(2)
            if MaxpssQ().get_size() == 0:
                break
        MaxpssQ.keep_running = False
        assert MaxpssQ().get_size() == 1
        r = MaxpssQ().get()
        assert r[0] == 1
        assert r[1] > 0


def test_route_get_maxpss(db_cfg, dag_factory):
    """This is to test the restful API to get maxpss of a job instance in jobmon side"""
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    MaxpssQ().empty_q()
    MaxpssQ.keep_running = True
    # test non-existing ji
    code, _ = req.send_request(
        app_route='/job_instance/9999/maxpss',
        message={},
        request_type='get')
    assert code == 404
    # add a real task
    name = 'bash_task'
    cmd = 'date'
    task = BashTask(command=cmd, name=name, m_mem_free='1G', max_attempts=2,
                    num_cores=1, max_runtime_seconds=120)
    executor = SGEExecutor(project='proj_tools')
    real_dag = dag_factory(executor)
    real_dag.add_task(task)
    (rc, num_completed, num_previously_complete, num_failed) = (
        real_dag._execute())
    assert rc == DagExecutionStatus.SUCCEEDED
    assert num_completed == 1
    job_list_manager = real_dag.job_list_manager
    assert job_list_manager.status_from_task(task) == JobStatus.DONE
    with app.app_context():
        ex_id = DB.session.execute("select executor_id from job_instance").fetchone()['executor_id']
        code, _ = req.send_request(
            app_route=f'/job_instance/{ex_id}/maxpss/500',
            message={},
            request_type='post'
        )
        assert code == 200
        code, response = req.send_request(
            app_route='/job_instance/1/maxpss',
            message={},
            request_type='get')
        assert code == 200
        assert response['maxpss'] == '500'

