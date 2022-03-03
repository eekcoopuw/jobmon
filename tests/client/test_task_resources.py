from jobmon.client.task_resources import TaskResources
from jobmon.cluster_type.base import ConcreteResource


def test_task_resources_hash(client_env):
    class MockConcreteResource(ConcreteResource):
        """Mock the concrete resource object."""

        def __init__(self, resource_dict: dict):
            self._resources = resource_dict

        @property
        def queue(self):
            class MockQueue:
                queue_name: str = "mock-queue"

            return MockQueue()

        @property
        def resources(self):
            return self._resources

        @classmethod
        def validate_and_create_concrete_resource(cls, *args, **kwargs):
            pass

        @classmethod
        def adjust_and_create_concrete_resource(cls, *args, **kwargs):
            pass

    # keys purposefully out of order
    resources_dict = {"d": "string datatype", "b": 123, "a": ["list", "data", "type"]}

    # same values, different representation order. Should not affect object equality.
    resource_dict_sorted = {key: resources_dict[key] for key in sorted(resources_dict)}

    # resources 1 and 2 should be equal, 3 should be different
    resource_1 = MockConcreteResource(resources_dict)
    resource_2 = MockConcreteResource(resource_dict_sorted)
    resource_3 = MockConcreteResource(dict(resources_dict, b=100))

    tr1 = TaskResources(task_resources_type_id="O", concrete_resources=resource_1)
    tr2 = TaskResources(task_resources_type_id="O", concrete_resources=resource_2)
    tr1_clone = TaskResources("O", resource_1)
    tr3 = TaskResources("O", resource_3)

    assert tr1 == tr1_clone
    assert tr1 == tr2
    assert tr1 != tr3

    assert len({tr1, tr2, tr3}) == 2

    # Equality instance check - should be false if other is not a ConcreteResource object
    class FakeResource:
        def __hash__(self):
            return hash(resource_1)

    assert not resource_1 == FakeResource()


def test_cluster_resource_cache(db_cfg, client_env):
    from jobmon.cluster import Cluster

    cluster = Cluster.get_cluster("sequential")

    resources_1 = {"runtime": 80, "queue": "null.q"}
    resources_2 = {"runtime": 90, "queue": "null.q"}

    tr1 = cluster.create_valid_task_resources(
        resource_params=resources_1, task_resources_type_id="O"
    )
    tr1_copy = cluster.create_valid_task_resources(
        resource_params=resources_1, task_resources_type_id="O"
    )

    assert tr1 is tr1_copy

    tr2 = cluster.create_valid_task_resources(resources_2, "O")
    assert tr1 is not tr2

    assert len(cluster.task_resources) == 2

    # Modify an attribute on tr1 - tr1_copy should also receive the change.
    # Mimics behavior in bind,
    # where task resources bound to multiple tasks should all be updated in 1 bind
    tr1.task_resources_id = 1
    assert tr1_copy.task_resources_id == 1


def test_task_resource_bind(db_cfg, client_env, tool, task_template):

    resources = {"queue": "null.q"}
    task_template.set_default_compute_resources_from_dict(
        cluster_name="sequential", compute_resources=resources
    )

    t1 = task_template.create_task(cluster_name="sequential", arg="echo 1")
    t2 = task_template.create_task(cluster_name="sequential", arg="echo 2")
    t3 = task_template.create_task(arg="echo 3")

    wf = tool.create_workflow()
    wf.add_tasks([t1, t2, t3])

    wf.bind()
    wf._create_workflow_run()

    app, db = db_cfg["app"], db_cfg["DB"]

    with app.app_context():

        q = f"""
        SELECT DISTINCT tr.id
        FROM task t
        JOIN task_resources tr ON tr.id = t.task_resources_id
        WHERE t.id IN {tuple(set([t.task_id for t in [t1, t2, t3]]))}
        """
        res = db.session.execute(q).fetchall()
        db.session.commit()
        assert len(res) == 1

    tr1, tr2, tr3 = [t.task_resources for t in wf.tasks.values()]
    assert tr1 is tr2
    assert tr1 is tr3
    assert tr1.id == res[0].id
