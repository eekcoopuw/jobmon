

def test_cluster(client_env):
    from jobmon.client.cluster import Cluster
    from jobmon.cluster_type import sequential

    cluster = Cluster(cluster_type_name="sequential")
    cluster.bind()
    assert cluster.plugin == sequential
    # assert cluster.plugin.create_resource({"sequential"})
