import subprocess
from unittest.mock import patch, PropertyMock

# from jobmon.client.distributor.strategies.base import ExecutorParameters
# from jobmon.client.distributor.strategies.sge import TaskInstanceSGEInfo
# from jobmon.client.distributor.strategies.sge.sge_executor import SGEExecutor
from jobmon.cluster_type.sequential.seq_distributor import SequentialDistributor
# from jobmon.client.distributor.worker_node.worker_node_task_instance import \
#     WorkerNodeTaskInstance
from jobmon.constants import TaskInstanceStatus
from jobmon.exceptions import ReturnCodes

import pytest

error_raised = False

@pytest.mark.unittest
def test_seq_kill_self_state():
    """
    mock the error status
    """
    expected_words = "job was in kill self state"
    executor = SequentialDistributor()
    executor._exit_info = {1: 199}
    r_value, r_msg = executor.get_remote_exit_info(1)
    assert r_value == TaskInstanceStatus.UNKNOWN_ERROR
    assert expected_words in r_msg

