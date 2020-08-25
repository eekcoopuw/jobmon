import pytest

from jobmon.client import shared_requester as requester


TOTAL_TASKS = 100000


def test_one_by_one(db_cfg, client_env):
    for i in range(0, TOTAL_TASKS):
        tasks = []
        task = {}
        task["workflow_id"] = 1
        task["node_id"] = 1
        task["task_args_hash"] = i
        task["name"] = f"name{i}"
        task["command"] = "whatever"
        task["max_attempts"] = 1
        task["task_args"] = {}
        task["task_attributes"] = None
        tasks.append(task)
        rc, _ = requester.send_request(
            app_route=f'/task',
            message={'tasks': tasks},
            request_type='post')
        assert rc == 200


def test_bulk(db_cfg, client_env):
    tasks = []
    for i in range(0, TOTAL_TASKS):
        task = {}
        task["workflow_id"] = 2
        task["node_id"] = 2
        task["task_args_hash"] = i
        task["task_args"] = {}
        task["name"] = f"name{i}"
        task["command"] = "whatever"
        task["max_attempts"] = 1
        task["task_attributes"] = None
        tasks.append(task)
    rc, _ = requester.send_request(
        app_route=f'/task',
        message={'tasks': tasks},
        request_type='post')
    assert rc == 200
