def test_node(tool, db_cfg):
    """tests ClientNode.bind() - checks that a node created for the first time
    creates a new db entry, and if it gets bound again a new entry
    won't be created"""
    from jobmon.client.node import Node

    tt = tool.get_task_template(
        "node_test",
        command_template="{node1} {node2} {node3}",
        node_args=["node1", "node2", "node3"],
    )

    node_1 = Node(
        task_template_version=tt.active_task_template_version,
        node_args={"node1": 3, "node2": 2006, "node3": "aggregate"},
    )
    node_1_id = node_1.bind()
    assert node_1_id is not None

    # ensure we can add this twice and avoid integrity issues
    node_1_id_redo = node_1._insert_node_and_node_args()
    assert node_1_id_redo == node_1_id

    node_2 = Node(
        task_template_version=tt.active_task_template_version,
        node_args={"node1": 3, "node2": 2006, "node3": "aggregate"},
    )
    node_2_id = node_2.bind()

    assert node_1_id == node_2_id
