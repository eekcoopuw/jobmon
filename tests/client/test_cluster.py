from jobmon.client.task_resources import TaskResources


def test_plugin_loading(client_env):
    from jobmon.client.cluster import Cluster
    from jobmon.cluster_type import sequential

    cluster = Cluster(cluster_name="sequential")
    cluster.bind()
    assert cluster.plugin == sequential


def test_get_queue(client_env):
    from jobmon.client.cluster import Cluster
    from jobmon.cluster_type.sequential.seq_client import SequentialQueue

    cluster = Cluster(cluster_name="sequential")
    cluster.bind()

    sequential_queue = cluster.get_queue(queue_name="sequential")
    assert type(sequential_queue) == SequentialQueue
    assert sequential_queue == cluster.get_queue(queue_name="sequential")
