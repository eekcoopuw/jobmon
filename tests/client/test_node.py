def test_node(client_env, db_cfg):
    """tests ClientNode.bind() - checks that a node created for the first time
    creates a new db entry, and if it gets bound again a new entry
    won't be created"""
    from jobmon.client.node import Node

    node_1 = Node(task_template_version_id=1, node_args={1: 3, 2: 2006, 4: "aggregate"})
    node_1_id = node_1.bind()
    assert node_1_id is not None

    # ensure we can add this twice and avoid integrity issues
    node_1_id_redo = node_1._insert_node_and_node_args()
    assert node_1_id_redo == node_1_id

    node_2 = Node(task_template_version_id=1, node_args={1: 3, 2: 2006, 4: "aggregate"})

    node_2_id = node_2.bind()

    assert node_1_id == node_2_id
