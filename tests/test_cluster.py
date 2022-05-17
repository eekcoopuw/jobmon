def test_plugin_loading(client_env):
    from jobmon.cluster import Cluster
    from jobmon.builtins import sequential

    cluster = Cluster(cluster_name="sequential")
    cluster.bind()
    assert cluster._cluster_type.plugin == sequential


def test_get_queue(client_env):
    from jobmon.cluster import Cluster
    from jobmon.builtins.sequential.seq_queue import SequentialQueue

    cluster = Cluster(cluster_name="sequential")
    cluster.bind()

    sequential_queue = cluster.get_queue(queue_name="null.q")
    assert type(sequential_queue) == SequentialQueue
    assert sequential_queue == cluster.get_queue(queue_name="null.q")
