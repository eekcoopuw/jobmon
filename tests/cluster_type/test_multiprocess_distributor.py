import os
import time
import random

import pytest

from jobmon import __version__
from jobmon.requester import Requester
from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun
from jobmon.client.distributor.distributor_array import DistributorArray
from jobmon.client.distributor.distributor_service import DistributorService
from jobmon.client.distributor.distributor_workflow_run import DistributorWorkflowRun
from jobmon.cluster_type.dummy import DummyDistributor
from jobmon.cluster_type.sequential.seq_distributor import SequentialDistributor
from jobmon.cluster_type.multiprocess.multiproc_distributor import (
    MultiprocessDistributor,
)
from jobmon.constants import TaskInstanceStatus
from jobmon.worker_node.worker_node_task_instance import WorkerNodeTaskInstance


def test_multiprocess_distributor(
    tool, db_cfg, client_env, task_template, array_template
):
    from jobmon.serializers import SerializeClusterType
    from jobmon.cluster_type.multiprocess.multiproc_distributor import (
        MultiprocessDistributor,
        MultiprocessWorkerNode,
    )

    # set up a MultiprocessDistributor with 5 consumers.
    dist = MultiprocessDistributor(5)

    dist.start()

    # submit 2 commands to non-array operation
    dist.submit_to_batch_distributor("echo 1", "echo_1", {"queue": "null.q"})
    dist.submit_to_batch_distributor("echo 2", "echo_2", {"queue": "null.q"})
    assert len(dist.consumers) == 5

    # we expect that dist.task_queue will be consumed by the consumers
    # fairly soon and become empty;
    # at that point, dict._running_or_submitted should have 2 items at most
    # (as _update_internal_states may drain it),
    # with the array_step_id being None, as this is a non-array operation.
    while not dist.task_queue.empty():
        time.sleep(1)
    assert len(dist._running_or_submitted) <= 2
    keys = dist._running_or_submitted.keys()
    for x in keys:
        assert x[1] is None

    dist.stop(dist.get_submitted_or_running([]))

    # reset up a MultiprocessDistributor with 5 consumers.
    dist = MultiprocessDistributor(5)

    dist.start()

    # submit 2 to array operation with array_length = 3
    dist.submit_array_to_batch_distributor("echo 1", "echo_1", {"queue": "null.q"}, 3)
    dist.submit_array_to_batch_distributor("echo 2", "echo_2", {"queue": "null.q"}, 3)

    # we expect that dist.task_queue will be consumed by the consumers
    # fairly soon and become empty;
    # at that point, dict._running_or_submitted should have 2 * 3 items at most
    # (as _update_internal_states may drain it),
    # with the array_step_id being some int(>0) (array_step_id), as this is an array operation.
    while not dist.task_queue.empty():
        time.sleep(1)
    assert len(dist._running_or_submitted) <= 2 * 3
    keys = dist._running_or_submitted.keys()
    for x in keys:
        assert x[1] > 0 and x[1] <= 3

    # dist.get_submitted_or_running() counts on distributor_ids only,
    # so it should only have 2 at most
    assert len(dist.get_submitted_or_running([])) <= 2

    dist.stop(dist.get_submitted_or_running([]))
