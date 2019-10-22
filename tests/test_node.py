from http import HTTPStatus as StatusCodes

from jobmon.client.swarm.workflow.node import Node


def test_node():
    """tests Node.bind() and /node GET and POST routes"""
    
    node_1 = Node(task_template_version_id=1,
                  node_args={1: 3, 2: 2006, 4: 'aggregate'})
    node_1_id = node_1.bind()

    assert node_1_id is not None

    node_2 = Node(task_template_version_id=1,
                  node_args={1: 3, 2: 2006, 4: 'aggregate'})
    node_2_id = node_2.bind()

    assert node_1_id == node_2_id
