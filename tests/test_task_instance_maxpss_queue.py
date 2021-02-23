import pytest
from threading import Thread
from time import sleep
from unittest import mock

from jobmon.requester import Requester


"""
# comment out for now; modify later
def test_integration_with_mock(db_cfg, dag_factory):
    # this is to test the restful API to get maxpss of a job instance in jobmon side
    app = db_cfg["app"]
    MaxpssQ().empty_q()
    MaxpssQ.keep_running = True

    with mock.patch('jobmon.server.integration.qpid.qpid_integrator._get_qpid_response') as \
            m_restful, \
        mock.patch('jobmon.server.integration.qpid.qpid_integrator._get_current_app') as \
                m_app, \
        mock.patch('jobmon.server.integration.qpid.qpid_integrator._get_pulling_interval') as \
                m_interval:

        m_restful.return_value = (200, 500)
        m_app.return_value = app
        m_interval.return_value = 5

        # start the integration server in a thread
        t = Thread(target=maxpss_forever)
        t.start()

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
        assert job_list_manager.status_from_task(task) == TaskStatus.DONE

        t.join(30)
        for i in range(5):
            sleep(6)
            if MaxpssQ().get_size() == 0:
                break
        MaxpssQ.keep_running = False
        assert MaxpssQ().get_size() == 0
        code, response = req.send_request(
            app_route='/scheduler/task_instance/1/maxpss',
            message={},
            request_type='get')
        assert code == 200
        assert response['maxpss'] == '500'
"""


@pytest.fixture
def qpidcfg(monkeypatch, db_cfg):
    """This creates a new tmp_out_dir for every module"""
    from jobmon.server.qpid_integration.qpid_config import QPIDConfig
    db_conn = db_cfg['server_config']

    def get_config():
        return QPIDConfig(
            db_host=db_conn.db_host,
            db_port=db_conn.db_port,
            db_user=db_conn.db_user,
            db_pass=db_conn.db_pass,
            db_name=db_conn.db_name,
            qpid_polling_interval=600,
            qpid_max_update_per_second=10,
            qpid_cluster="fair",
            qpid_uri="https://jobapi.ihme.washington.edu"
        )

    monkeypatch.setattr(QPIDConfig, "from_defaults", get_config)


@pytest.mark.unittest
def test_MaxpssQ(qpidcfg):
    """This is to test the Q stops increasing when the max size is reached."""
    from jobmon.server.qpid_integration.qpid_integrator import MaxpssQ

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
def test_worker_with_mock_200(qpidcfg):
    """This is to test the job with maxpss leaves the Q."""
    from jobmon.server.qpid_integration.qpid_integrator import MaxpssQ, maxpss_forever

    MaxpssQ().empty_q()
    MaxpssQ.keep_running = True
    assert MaxpssQ().get_size() == 0
    with mock.patch('jobmon.server.qpid_integration.qpid_integrator._update_maxpss_in_db') as\
            m_db, \
         mock.patch('jobmon.server.qpid_integration.qpid_integrator._get_qpid_response') \
            as m_restful:
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
def test_worker_with_mock_404(qpidcfg):
    """This is to test the job without maxpss will be put back to the Q with age increased."""
    from jobmon.server.qpid_integration.qpid_integrator import MaxpssQ, maxpss_forever

    MaxpssQ().empty_q()
    MaxpssQ.keep_running = True
    assert MaxpssQ().get_size() == 0
    with mock.patch('jobmon.server.qpid_integration.qpid_integrator._get_qpid_response') as \
            m_restful:
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
def test_worker_with_mock_500(qpidcfg):
    """This is to test the job will be put back to the Q with age increated when QPID is
    down."""
    from jobmon.server.qpid_integration.qpid_integrator import MaxpssQ, maxpss_forever

    MaxpssQ().empty_q()
    MaxpssQ.keep_running = True
    assert MaxpssQ().get_size() == 0
    with mock.patch('jobmon.server.qpid_integration.qpid_integrator._get_qpid_response') as \
            m_restful:
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


def test_route_get_maxpss_error_path(qpidcfg, client_env):
    """This is to test the restful API to get maxpss of a job instance in jobmon side"""
    from jobmon.server.qpid_integration.qpid_integrator import MaxpssQ
    req = Requester(client_env)

    MaxpssQ().empty_q()
    # test non-existing ji
    code, _ = req.send_request(
        app_route='/scheduler/task_instance/9999/maxpss',
        message={},
        request_type='get'
    )
    assert code == 404


"""
# comment out for now; modify later
@pytest.mark.unittest
def test_get_completed_task_instance(db_cfg, dag_factory):
    app = db_cfg["app"]
    MaxpssQ().empty_q()
    t = time()

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
    assert job_list_manager.status_from_task(task) == TaskStatus.DONE

    with app.app_context():
        _get_completed_task_instance(t, SQLAlchemy(app).session)
        assert MaxpssQ().get_size() == 1
"""
