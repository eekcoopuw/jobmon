from jobmon.client.task_resources import TaskResources


def test_create_task_resources(client_env):

    tr = TaskResources(queue_id=3, task_resources_type_id="O", resource_scales="(3,4)",
                        requested_resources="long list of resources", requester=None)
    tr.bind(111)
    assert tr.task_id==111

