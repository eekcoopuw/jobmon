from jobmon.client.tool import Tool

import pytest
import uuid


def test_get_tasks_dependencynotexist(db_cfg, client_env):
    tool = Tool()
    tool.set_default_compute_resources_from_dict(
        cluster_name="sequential", compute_resources={"queue": "null.q"}
    )
    task_template = tool.get_task_template(
        template_name="cli_template_1",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
    )

    t1 = task_template.create_task(arg="echo 1")
    t2 = task_template.create_task(arg="echo 11")
    t3 = task_template.create_task(arg="echo 111")
    t2.add_upstream(t1)
    t3.add_upstream(t2)

    wf = tool.create_workflow(str(uuid.uuid4()))
    wf.add_tasks([t1, t2, t3])
    wf.run()
    app_route = f"/task_dependencies/{t1.task_id}"
    return_code, msg = wf.requester.send_request(
        app_route=app_route,
        message={},
        request_type='get'
    )
    assert msg == {'down': [{'id': t2.task_id, 'status': 'D'}], 'up': []}
    app_route = f"/task_dependencies/{t2.task_id}"
    return_code, msg = wf.requester.send_request(
        app_route=app_route,
        message={},
        request_type='get'
    )
    assert msg == {'down': [{'id': t3.task_id, 'status': 'D'}], 'up': [{'id': t1.task_id, 'status': 'D'}]}
    app_route = f"/task_dependencies/{t3.task_id}"
    return_code, msg = wf.requester.send_request(
        app_route=app_route,
        message={},
        request_type='get'
    )
    assert msg == {'down': [], 'up': [{'id': t2.task_id, 'status': 'D'}]}


def test_get_task_template_version(db_cfg, client_env):
    t = Tool()
    wf = t.create_workflow(name="i_am_a_fake_wf")
    tt1 = t.get_task_template(
        template_name="tt1",
        command_template="sleep {arg}",
        node_args=["arg"],
        default_compute_resources={"queue": "null.q"},
        default_cluster_name="sequential")
    tt2 = t.get_task_template(
        template_name="tt2",
        command_template="echo {arg}",
        node_args=["arg"],
        default_compute_resources={"queue": "null.q"},
        default_cluster_name="sequential")

    task_1 = tt1.create_task(arg=1)
    task_2 = tt1.create_task(arg=2)
    task_3 = tt2.create_task(arg=3)
    wf.add_tasks([task_1, task_2, task_3])
    wf.run()
    tt1.load_task_template_versions()
    tt2.load_task_template_versions()

    # Test getting task template for task
    app_route = "/get_task_template_version"
    return_code, msg = wf.requester.send_request(
        app_route=app_route,
        message={"task_id": task_1.task_id},
        request_type='get'
    )
    # msg = {'task_template_version_ids': [{'id': 1, 'name': 'bash_task'}]}
    assert len(msg) == 1
    assert 'task_template_version_ids' in msg.keys()
    assert len(msg['task_template_version_ids']) == 1
    assert 'id' in msg['task_template_version_ids'][0].keys()
    assert msg['task_template_version_ids'][0]['name'] == 'tt1'

    # Test getting task template for workflow
    app_route = "/get_task_template_version"
    return_code, msg = wf.requester.send_request(
        app_route=app_route,
        message={"workflow_id": wf.workflow_id},
        request_type='get'
    )
    # msg = {'task_template_version_ids': [{'id': 1, 'name': 'tt1'}, {'id': 2, 'name': 'tt2'}]}
    assert len(msg) == 1
    assert 'task_template_version_ids' in msg.keys()
    assert len(msg['task_template_version_ids']) == 2
    for i in msg['task_template_version_ids']:
        if i["id"] == tt1._active_task_template_version.id:
            assert i["name"] == "tt1"
        else:
            assert i["name"] == "tt2"


def test_get_requested_cores(db_cfg, client_env):
    t = Tool()
    wf = t.create_workflow(name="i_am_a_fake_wf")
    tt1 = t.get_task_template(
        template_name="tt_core",
        command_template="echo {arg}",
        node_args=["arg"])
    t1 = tt1.create_task(arg=1, cluster_name="sequential",
                         compute_resources={"queue": "null.q", "num_cores": 2})
    t2 = tt1.create_task(arg=2, cluster_name="sequential",
                         compute_resources={"queue": "null.q", "num_cores": 4})
    wf.add_tasks([t1, t2])
    wf.run()

    # Get task template for workflow
    app_route = "/get_task_template_version"
    return_code, msg = wf.requester.send_request(
        app_route=app_route,
        message={"workflow_id": wf.workflow_id},
        request_type='get'
    )
    ttvis = msg['task_template_version_ids'][0]['id']
    # Test getting requested cores
    app_route = "/get_requested_cores"
    return_code, msg = wf.requester.send_request(
        app_route=app_route,
        message={"task_template_version_ids": f"({ttvis})"},
        request_type='get'
    )
    # msg = {'core_info': [{'avg': 2, 'id': 1, 'max': 3, 'min': 1}]}
    assert len(msg['core_info']) == 1
    assert msg['core_info'][0]['id'] == ttvis
    assert msg['core_info'][0]['min'] == 2
    assert msg['core_info'][0]['max'] == 4
    assert msg['core_info'][0]['avg'] == 3


def test_most_popular_queue(db_cfg, client_env):
    import uuid
    from jobmon.client.execution.strategies.base import ExecutorParameters
    from jobmon.client.tool import Tool

    t = Tool.create_tool(name=str(uuid.uuid4()))
    wf = t.create_workflow(name="i_am_a_fake_wf")
    wf.set_executor(executor_class="SequentialExecutor")
    tt1 = t.get_task_template(
          template_name="tt1",
          command_template="sleep {arg}",
          node_args=["arg"])
    tt2 = t.get_task_template(
          template_name="tt2",
          command_template="echo {arg}",
          node_args=["arg"])
    ep1 = ExecutorParameters(executor_class="SequentialExecutor",
                             queue="all.q")
    ep2 = ExecutorParameters(executor_class="SequentialExecutor",
                             queue="long.q")
    task_1 = tt1.create_task(executor_parameters=ep1, arg=1)
    task_2 = tt1.create_task(executor_parameters=ep1, arg=2)
    task_3 = tt2.create_task(executor_parameters=ep1, arg=3)
    task_4 = tt2.create_task(executor_parameters=ep2, arg=1)
    task_5 = tt2.create_task(executor_parameters=ep2, arg=2)
    wf.add_tasks([task_1, task_2, task_3, task_4, task_5])
    wf.run()
    app_route = "/cli/get_most_popular_queue"
    return_code, msg = wf.requester.send_request(
        app_route=app_route,
        message={"task_template_version_ids": f"({tt1.task_template_version.id}, {tt2.task_template_version.id})"},
        request_type='get'
    )
    # msg = {'queue_info': [{'id': 1, 'queue': 'all.q'}, {'id': 2, 'queue': 'long.q'}]}
    assert len(msg["queue_info"]) == 2
    for i in msg["queue_info"]:
        if i["id"] == tt1.task_template_version.id:
            assert i["queue"] == "all.q"
        else:
            assert i["queue"] == "long.q"
