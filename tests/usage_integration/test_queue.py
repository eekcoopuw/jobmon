from threading import Thread
from time import sleep
from typing import List
from unittest import mock

from jobmon.requester import Requester
from jobmon.server.usage_integration.usage_integrator import q_forever
from jobmon.server.usage_integration.usage_queue import UsageQ
from jobmon.server.usage_integration.usage_utils import QueuedTI

import pytest
from sqlalchemy.sql import text

@pytest.mark.usage_integrator
def test_MaxrssQ():
    """This is to test the Q stops increasing when the max size is reached.

    Note: Do not run usage_integrator tests with multiprocessing."""

    # clean start
    UsageQ.empty_q()
    # set max
    UsageQ._maxsize = 100
    # get from empty queue
    assert UsageQ.get() is None
    # put into queue
    # Q: ((1,0))
    item1 = QueuedTI(
        task_instance_id=1, distributor_id='1', cluster_type_name="slurm", cluster_id=5
    )
    UsageQ.put(item1)
    assert UsageQ.get_size() == 1
    # Q: ((1,0), (2, 1))
    item2 = QueuedTI(
        task_instance_id=2, distributor_id='2', cluster_type_name="slurm", cluster_id=5
    )
    UsageQ.put(item2, 1)
    assert UsageQ().get_size() == 2
    # overflow
    item3 = QueuedTI(
        task_instance_id=3, distributor_id='3', cluster_type_name="slurm", cluster_id=5
    )
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


@pytest.mark.usage_integrator
def test_worker_with_mock_200(usage_integrator_config):
    """This is to test the job with maxpss leaves the Q.

    Note: Do not run usage_integrator tests with multiprocessing."""

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
        item = QueuedTI(
            task_instance_id=1, distributor_id='1', cluster_type_name="slurm", cluster_id=4
        )
        UsageQ.put(item)
        assert UsageQ.get_size() == 1
        t = Thread(target=q_forever, kwargs={'integrator_config': usage_integrator_config})
        t.start()
        t.join(10)
        UsageQ.keep_running = False
        for i in range(5):
            sleep(2)
            if UsageQ.get_size() == 0:
                break
        assert UsageQ.get_size() == 0


@pytest.mark.usage_integrator
def test_slurm_update(usage_integrator):
    """This is to test the SLURM updates behave accordingly.

    Test 1: job will be put back to the Q with age increased if resources are
    not found.

    Test 2: the update statement can be performed as expected.
    """

    UsageQ.empty_q()
    assert UsageQ.get_size() == 0

    def mock_no_resources(task_instances: List[QueuedTI], *args, **kwargs):
        """Return a dict of Nones to mock a "task instance not found" issue.

        This path I believe is almost guaranteed to never happen, since submitted tasks
        are added to the accounting database almost instantly. However, might as well test.
        """
        return {ti: None for ti in task_instances}

    with mock.patch(
        "jobmon.server.usage_integration.usage_integrator._get_slurm_resource_via_slurm_sdb",
        new=mock_no_resources
    ):
        # code logic to test
        item = QueuedTI(
            task_instance_id=1_000_000, distributor_id='1', cluster_type_name="slurm", cluster_id=5
        )
        # Call the update tasks method. Check that age is incremented and the task is added to
        # the queue.
        usage_integrator.update_slurm_resources([item])
        assert item.age == 1
        assert UsageQ.get_size() == 1

    # Check that task instance can be updated accordingly
    UsageQ.empty_q()

    def mock_resources(task_instances: List[QueuedTI], *args, **kwargs):
        """Return a hardcoded dict to mock SQUID return values"""
        return {ti: {'maxrss': 100, 'wallclock': 100} for ti in task_instances}


    with mock.patch(
        "jobmon.server.usage_integration.usage_integrator._get_slurm_resource_via_slurm_sdb",
        new=mock_resources
    ):
        try:
            # Call the update tasks method. Check that resources are updated accordingly.
            usage_integrator.update_slurm_resources([item])

            # Query maxrss and wallclock values
            resource_query = (
                "SELECT ti.maxrss, ti.wallclock "
                "FROM task_instance ti "
                "WHERE id = :tid"
            )

            res = usage_integrator.session.execute(text(resource_query), {'tid': item.task_instance_id}).one()
            assert res.maxrss == '100'
            assert res.wallclock == '100'
        finally:
            # Ensure that fictitious task instance is deleted to avoid cluttering the database
            delete_query = (
                "DELETE FROM task_instance "
                "WHERE id = :tid"
            )
            usage_integrator.session.execute(text(delete_query), {'tid': item.task_instance_id})
            usage_integrator.session.commit()
