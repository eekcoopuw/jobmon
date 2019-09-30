import os
import pytest
from jobmon.client import client_config


path_to_file = os.path.dirname(__file__)


def cleanup_jlm(workflow):
    """If a dag is not completed properly and all threads are disconnected,
    a new job list manager will access old job instance factory/reconciler
    threads instead of creating new ones. So we need to make sure threads get
    cleand up at the end"""

    if workflow.task_dag.job_list_manager:
        workflow.task_dag.job_list_manager.disconnect()


@pytest.fixture
def fast_heartbeat():
    old_heartbeat = client_config.heartbeat_interval
    client_config.heartbeat_interval = 10
    yield
    client_config.heartbeat_interval = old_heartbeat


def mock_slack(msg, channel):
    print("{} to be posted to channel: {}".format(msg, channel))
