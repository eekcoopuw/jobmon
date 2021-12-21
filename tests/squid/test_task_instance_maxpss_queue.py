from threading import Thread
from time import sleep
from unittest import mock

from jobmon.requester import Requester

import pytest


@pytest.fixture
def squidcfg(monkeypatch, db_cfg):
    """This creates a new tmp_out_dir for every module"""
    from jobmon.server.squid_integration.squid_config import SQUIDConfig

    db_conn = db_cfg["server_config"]

    def get_config():
        return SQUIDConfig(
            db_host=db_conn.db_host,
            db_port=db_conn.db_port,
            db_user=db_conn.db_user,
            db_pass=db_conn.db_pass,
            db_name=db_conn.db_name,
            squid_polling_interval=600,
            squid_max_update_per_second=10,
            qpid_cluster="fair",
            qpid_uri="https://jobapi.ihme.washington.edu",
            squid_cluster="slurm",
        )

    monkeypatch.setattr(SQUIDConfig, "from_defaults", get_config)


@pytest.mark.unittest
def test_MaxrssQ(squidcfg):
    """This is to test the Q stops increasing when the max size is reached."""
    from jobmon.server.squid_integration.slurm_maxrss_queue import MaxrssQ
    from jobmon.server.squid_integration.squid_utils import QueuedTI

    # clean start
    MaxrssQ._q = None
    # set max
    MaxrssQ._maxsize = 100
    # get from empty queue
    assert MaxrssQ.get() is None
    # put into queue
    # Q: ((1,0))
    item1 = QueuedTI()
    item1.task_instance_id = 1
    MaxrssQ.put(item1)
    assert MaxrssQ.get_size() == 1
    # Q: ((1,0), (2, 1))
    item2 = QueuedTI
    item2.task_instance_id = 2
    MaxrssQ.put(item2, 1)
    assert MaxrssQ().get_size() == 2
    # overflow
    item3 = QueuedTI()
    item3.task_instance_id = 3
    for i in range(110):
        MaxrssQ().put(item3)
    assert MaxrssQ().get_size() == 100
    # Test Queue Content
    # Q: ((2, 1))
    e1 = MaxrssQ().get()
    assert e1[0].task_instance_id == 1
    assert e1[1] == 0
    # Q: ()
    e2 = MaxrssQ().get()
    assert e2[0].task_instance_id == 2
    assert e2[1] == 1


@pytest.mark.unittest
def test_worker_with_mock_200(squidcfg):
    """This is to test the job with maxpss leaves the Q."""
    from jobmon.server.squid_integration.slurm_maxrss_queue import MaxrssQ
    from jobmon.server.squid_integration.squid_utils import QueuedTI
    from jobmon.server.squid_integration.squid_integrator import _update_maxrss_in_db, _get_qpid_response, maxrss_forever

    MaxrssQ.empty_q()
    assert MaxrssQ.get_size() == 0
    with mock.patch(
        "jobmon.server.squid_integration.squid_integrator._update_maxrss_in_db"
    ) as m_db, mock.patch(
        "jobmon.server.squid_integration.squid_integrator._get_qpid_response"
    ) as m_restful:
        # mock
        m_db.return_value = True
        m_restful.return_value = (200, 500)

        # code logic to test
        item = QueuedTI()
        item.task_instance_id = 1
        item.cluster_type_name = "uge"
        MaxrssQ.put(item)
        assert MaxrssQ.get_size() == 1
        t = Thread(target=maxrss_forever)
        t.start()
        t.join(10)
        MaxrssQ.keep_running = False
        for i in range(5):
            sleep(2)
            if MaxrssQ.get_size() == 0:
                break
        assert MaxrssQ.get_size() == 0


@pytest.mark.unittest
def test_worker_with_mock_404(squidcfg):
    """This is to test the job without maxpss will be put back to the Q with age increased."""
    from jobmon.server.squid_integration.slurm_maxrss_queue import MaxrssQ
    from jobmon.server.squid_integration.squid_utils import QueuedTI
    from jobmon.server.squid_integration.squid_integrator import _update_maxrss_in_db, _get_qpid_response, \
        maxrss_forever

    MaxrssQ.empty_q()
    MaxrssQ.keep_running = True
    assert MaxrssQ.get_size() == 0
    with mock.patch(
        "jobmon.server.squid_integration.squid_integrator._get_qpid_response"
    ) as m_restful:
        # mock
        m_restful.return_value = (404, None)
        # code logic to test
        item = QueuedTI()
        item.task_instance_id = 1
        item.cluster_type_name = "uge"
        MaxrssQ.put(item)
        assert MaxrssQ.get_size() == 1
        t = Thread(target=maxrss_forever)
        t.start()
        t.join(10)
        for i in range(5):
            sleep(2)
            if MaxrssQ.get_size() == 0:
                break
        MaxrssQ.keep_running = False
        assert MaxrssQ.get_size() == 1
        r = MaxrssQ.get()
        assert r[0].task_instance_id == 1
        assert r[1] > 0


@pytest.mark.unittest
def test_worker_with_mock_500(squidcfg):
    """This is to test the job will be put back to the Q with age increased when QPID is
    down."""
    from jobmon.server.squid_integration.slurm_maxrss_queue import MaxrssQ
    from jobmon.server.squid_integration.squid_utils import QueuedTI
    from jobmon.server.squid_integration.squid_integrator import _update_maxrss_in_db, _get_qpid_response, \
        maxrss_forever

    MaxrssQ.empty_q()
    MaxrssQ.keep_running = True
    assert MaxrssQ.get_size() == 0
    with mock.patch(
            "jobmon.server.squid_integration.squid_integrator._get_qpid_response"
    ) as m_restful:
        # mock
        m_restful.return_value = (500, None)
        # code logic to test
        item = QueuedTI()
        item.task_instance_id = 1
        item.cluster_type_name = "uge"
        MaxrssQ.put(item)
        assert MaxrssQ.get_size() == 1
        t = Thread(target=maxrss_forever)
        t.start()
        t.join(10)
        for i in range(5):
            sleep(2)
            if MaxrssQ.get_size() == 0:
                break
        MaxrssQ.keep_running = False
        assert MaxrssQ.get_size() == 1
        r = MaxrssQ.get()
        assert r[0].task_instance_id == 1
        assert r[1] > 0
