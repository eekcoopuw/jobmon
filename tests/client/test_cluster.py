import pytest


def test_plugin_loading(client_env):
    from jobmon.cluster import Cluster
    from jobmon.cluster_type import sequential

    cluster = Cluster(cluster_name="sequential")
    cluster.bind()
    assert cluster.plugin == sequential


def test_get_queue(client_env):
    from jobmon.cluster import Cluster
    from jobmon.cluster_type.sequential.seq_queue import SequentialQueue

    cluster = Cluster(cluster_name="sequential")
    cluster.bind()

    sequential_queue = cluster.get_queue(queue_name="null.q")
    assert type(sequential_queue) == SequentialQueue
    assert sequential_queue == cluster.get_queue(queue_name="null.q")


def test_validate(db_cfg, client_env):
    """Test that adjust and validate work as expected."""
    from jobmon.client.task_resources import TaskResources
    from jobmon.constants import TaskResourcesType
    from jobmon.client.cluster import Cluster

    cluster = Cluster.get_cluster("multiprocess")

    # Create a valid resource. In test_utils.db_schema, note that min/max cores for the multiprocess
    # cluster null.q is 1/20
    happy_resource: TaskResources = cluster.create_valid_task_resources(
        resource_params={"cores": 10, "queue": "null.q", "runtime": "01:02:33"},
        task_resources_type_id=TaskResourcesType.VALIDATED,
        fail=False,
    )

    assert happy_resource.concrete_resources.resources["cores"] == 10
    assert happy_resource.queue.queue_name == "null.q"
    assert happy_resource.concrete_resources.resources["runtime"] == "3753s"

    # Create invalid resource
    # Try a fail call first
    with pytest.raises(ValueError):
        cluster.create_valid_task_resources(
            resource_params={"cores": 100, "queue": "null.q"},
            task_resources_type_id=TaskResourcesType.VALIDATED,
            fail=True,
        )

    # Same call but check that the resources are coerced
    unhappy_resource: TaskResources = cluster.create_valid_task_resources(
        resource_params={"cores": 100, "queue": "null.q"},
        task_resources_type_id=TaskResourcesType.VALIDATED,
        fail=False,
    )
    assert unhappy_resource.concrete_resources.resources["cores"] == 20
