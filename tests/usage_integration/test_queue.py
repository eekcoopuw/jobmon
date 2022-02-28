from threading import Thread
from time import sleep
from unittest import mock

from jobmon.requester import Requester

import pytest


@pytest.fixture
def squidcfg(monkeypatch, db_cfg):
    """This creates a new tmp_out_dir for every module"""
    from jobmon.server.usage_integration.config import UsageConfig

    db_conn = db_cfg["server_config"]

    def get_config():
        return UsageConfig(
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

    monkeypatch.setattr(UsageConfig, "from_defaults", get_config)


@pytest.mark.unittest
def test_MaxrssQ(squidcfg):
    """This is to test the Q stops increasing when the max size is reached."""
    from jobmon.server.usage_integration.usage_queue import UsageQ
    from jobmon.server.usage_integration.usage_utils import QueuedTI

    # clean start
    UsageQ.empty_q()
    # set max
    UsageQ._maxsize = 100
    # get from empty queue
    assert UsageQ.get() is None
    # put into queue
    # Q: ((1,0))
    item1 = QueuedTI(task_instance_id=1, distributor_id=1, cluster_type_name='slurm', cluster_id=5)
    UsageQ.put(item1)
    assert UsageQ.get_size() == 1
    # Q: ((1,0), (2, 1))
    item2 = QueuedTI(task_instance_id=2, distributor_id=2, cluster_type_name='slurm', cluster_id=5)
    UsageQ.put(item2, 1)
    assert UsageQ().get_size() == 2
    # overflow
    item3 = QueuedTI(task_instance_id=3, distributor_id=3, cluster_type_name='slurm', cluster_id=5)
    for i in range(110):
        UsageQ().put(item3, 2)
    assert UsageQ().get_size() == 100
    # Test Queue Content
    # Q: ((2, 1))
    e1 = UsageQ().get()
    assert e1.task_instance_id == 1
    assert e1.age == 0
    # Q: ()
    e2 = UsageQ().get()
    assert e2.task_instance_id == 2
    assert e2.age == 1


@pytest.mark.unittest
def test_worker_with_mock_200(squidcfg):
    """This is to test the job with maxpss leaves the Q."""
    from jobmon.server.usage_integration.usage_queue import UsageQ
    from jobmon.server.usage_integration.usage_utils import QueuedTI
    from jobmon.server.usage_integration.usage_integrator import (
        _get_qpid_response,
        q_forever,
    )

    UsageQ.empty_q()
    assert UsageQ.get_size() == 0
    with mock.patch(
        "jobmon.server.usage_integration.usage_integrator.UsageIntegrator.update_resources_in_db"
    ) as m_db, mock.patch(
        "jobmon.server.usage_integration.usage_integrator.UsageIntegrator.populate_queue"
    ) as m_restful:
        # mock
        m_db.return_value = None
        m_restful.return_value = None

        # code logic to test
        item = QueuedTI(task_instance_id=1, distributor_id=1, cluster_type_name='UGE', cluster_id=4)
        UsageQ.put(item)
        assert UsageQ.get_size() == 1
        t = Thread(target=q_forever)
        t.start()
        t.join(10)
        UsageQ.keep_running = False
        for i in range(5):
            sleep(2)
            if UsageQ.get_size() == 0:
                break
        assert UsageQ.get_size() == 0


@pytest.mark.unittest
def test_worker_with_mock_404(squidcfg):
    """This is to test the job without maxpss will be put back to the Q with age increased."""
    from jobmon.server.usage_integration.usage_queue import UsageQ
    from jobmon.server.usage_integration.usage_utils import QueuedTI
    from jobmon.server.usage_integration.usage_integrator import (
        _get_qpid_response,
        q_forever,
    )

    UsageQ.empty_q()
    UsageQ.keep_running = True
    assert UsageQ.get_size() == 0
    with mock.patch(
        "jobmon.server.usage_integration.usage_integrator.UsageIntegrator.populate_queue"
    ) as m_restful, mock.patch(
        "jobmon.server.usage_integration.usage_integrator._get_qpid_response"
    ) as m_qpid:
        # mock
        m_restful.return_value = None
        m_qpid.return_value = 404, None

        # code logic to test
        item = QueuedTI(task_instance_id=1, distributor_id=1, cluster_type_name='UGE', cluster_id=4)
        UsageQ.put(item)
        assert UsageQ.get_size() == 1
        t = Thread(target=q_forever)
        t.start()
        t.join(10)
        for i in range(5):
            sleep(2)
            if UsageQ.get_size() == 0:
                break
        UsageQ.keep_running = False
        assert UsageQ.get_size() == 1
        r = UsageQ.get()
        assert r.task_instance_id == 1
        assert r.age > 0


@pytest.mark.unittest
def test_worker_with_mock_500(squidcfg):
    """This is to test the job will be put back to the Q with age increased when QPID is
    down."""
    from jobmon.server.usage_integration.usage_queue import UsageQ
    from jobmon.server.usage_integration.usage_utils import QueuedTI
    from jobmon.server.usage_integration.usage_integrator import (
        _get_qpid_response,
        q_forever,
    )

    UsageQ.empty_q()
    UsageQ.keep_running = True
    assert UsageQ.get_size() == 0
    with mock.patch(
        "jobmon.server.usage_integration.usage_integrator.UsageIntegrator.populate_queue"
    ) as m_restful, mock.patch(
        "jobmon.server.usage_integration.usage_integrator._get_qpid_response"
    ) as m_qpid:
        # mock
        m_restful.return_value = None
        m_qpid.return_value = 500, None
        # code logic to test
        item = QueuedTI(task_instance_id=1, distributor_id=1, cluster_type_name='UGE', cluster_id=4)
        UsageQ.put(item)
        assert UsageQ.get_size() == 1
        t = Thread(target=q_forever)
        t.start()
        t.join(10)
        for i in range(5):
            sleep(2)
            if UsageQ.get_size() == 0:
                break
        UsageQ.keep_running = False
        assert UsageQ.get_size() == 1
        r = UsageQ.get()
        assert r.task_instance_id == 1
        assert r.age > 0
