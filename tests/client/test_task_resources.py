from jobmon.client.task_resources import TaskResources


def test_task_resources_hash(client_env):

    class MockQueue:
        queue_name: str = "mock-queue"

    # keys purposefully out of order
    resources_dict = {"d": "string datatype", "b": 123, "a": ["list", "data", "type"]}

    # same values, different representation order. Should not affect object equality.
    resource_dict_sorted = {key: resources_dict[key] for key in sorted(resources_dict)}

    # resources 1 and 2 should be equal, 3 should be different
    resource_1 = resources_dict
    resource_2 = resource_dict_sorted
    resource_3 = dict(resources_dict, b=100)

    tr1 = TaskResources(requested_resources=resource_1, queue=MockQueue())
    tr2 = TaskResources(requested_resources=resource_2, queue=MockQueue())
    tr1_clone = TaskResources(resource_1, queue=MockQueue())

    tr3 = TaskResources(resource_3, MockQueue())

    assert tr1 == tr1_clone
    assert tr1 == tr2
    assert tr1 != tr3

    assert len({tr1, tr2, tr3}) == 2

    # Equality instance check - should be false if other is not a ConcreteResource object
    class FakeResource:
        def __hash__(self):
            return hash(resource_1)

    assert not resource_1 == FakeResource()


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

    tr1, tr2, tr3 = [t.original_task_resources for t in wf.tasks.values()]
    assert tr1 is tr2
    assert tr1 is tr3
    assert tr1.id == res[0].id
