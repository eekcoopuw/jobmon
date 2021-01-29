import logging
from threading import Thread

import pytest

from jobmon.requester import Requester


logger = logging.getLogger(__name__)

# The testing threads that sending requests simultaneously
TOTAL_THREADS = 2
# The number of nodes to create in each threads
TOTAL_NODES = 1000


# the function to create node using /nodes
def create_node(requester_url, starter=0):
    requester = Requester(requester_url, logger)

    nodes = []
    for i in range(0, TOTAL_NODES):
        node = {'task_template_version_id': 2, 'node_args_hash': i+starter, 'node_args': {}}
        nodes.append(node)
    rc, r = requester.send_request(
        app_route='/nodes',
        message={'nodes': nodes},
        request_type='post')
    assert rc == 200
    assert len(r['nodes']) == TOTAL_NODES


@pytest.mark.performance_tests
def test_single_thread(db_cfg, client_env):
    create_node(client_env)


@pytest.mark.performance_tests
def test_multi_thread(db_cfg, client_env):
    threads = []
    for i in range(0, TOTAL_THREADS):
        t = Thread(target=create_node, args=[client_env, 5000000])
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
