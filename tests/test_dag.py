from jobmon.client.swarm.workflow.clientnode import ClientNode
from jobmon.client.swarm.workflow.clientdag import ClientDag

def test_dag(env_var):
    """tests ClientDag.bind() - checks that a dag created for the first time
    creates a new db entry, and if it gets bound again a new entry
    won't be created"""

    # create nodes for populate dag
    node_1 = ClientNode(task_template_version_id=1,
                        node_args={1: 1, 2: 2006, 4: 'female'})
    node_1.bind()
    node_2 = ClientNode(task_template_version_id=1,
                        node_args={1: 2, 2: 2006, 4: 'male'})
    node_2.bind()
    node_3 = ClientNode(task_template_version_id=1,
                        node_args={1: 3, 2: 2006, 4: 'both_sex'},
                        upstream_nodes=[node_1, node_2])
    node_3.bind()

    dag_1 = ClientDag()
    # add nodes to dag
    [dag_1.add_node(node) for node in [node_1, node_2, node_3]]
    dag_1_id = dag_1.bind()

    assert dag_1_id is not None

    # build a dag identical to dag_1
    dag_2 = ClientDag()
    # add nodes to dag
    [dag_2.add_node(node) for node in [node_1, node_2, node_3]]

    dag_2_id = dag_2.bind()
    # since they have the same nodes and edges, the dags should be the same
    assert dag_2_id == dag_1_id
