from http import HTTPStatus as StatusCodes

from jobmon.client import shared_requester
from jobmon.client.swarm.workflow.node import Node


def test_node(real_jsm_jqs):
    """tests Node.bind() and /node GET and POST routes"""

    node_1 = Node(task_template_version_id=1,
                  node_args={1: 3, 2: 2006, 4: 'aggregate'})
    node_1_id = node_1.bind()

    assert node_1_id is not None

    node_2 = Node(task_template_version_id=1,
                  node_args={1: 3, 2: 2006, 4: 'aggregate'})
    node_2_id = node_2.bind()

    assert node_1_id == node_2_id


def test_node_get_route(real_jsm_jqs):
    node = Node(task_template_version_id=1,
                  node_args={1: 3, 2: 2006, 4: 'aggregate'})
    return_code, response = shared_requester.send_request(
        app_route='/node',
        message={
            'task_template_version_id': node.task_template_version_id,
            'node_args_hash': node.node_args_hash
        },
        request_type='get'
    )
    assert return_code == StatusCodes.OK


def test_node_post_route(real_jsm_jqs):
    node = Node(task_template_version_id=1,
                node_args={1: 3, 2: 2006, 4: 'aggregate'})
    return_code, response = shared_requester.send_request(
        app_route='/node',
        message={
            'task_template_version_id': node.task_template_version_id,
            'node_args_hash': node.node_args_hash,
            'node_args': node.node_args
        },
        request_type='post'
    )
    assert return_code == StatusCodes.OK
