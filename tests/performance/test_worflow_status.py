import pytest

import time


from jobmon.server.web.models import load_model

load_model()

@pytest.mark.performance_tests
def test_get_workflow_status(db_engine, tool):
    t = tool

    tt1 = t.get_task_template(
        template_name="tt_core", command_template="echo {arg}", node_args=["arg"]
    )
    wfs = []
    for i in range(1, 1000):
        task = tt1.create_task(
            arg=i,
            cluster_name="sequential",
            compute_resources={"queue": "null.q", "num_cores": 4},
        )
        wf = t.create_workflow(name=f"i_am_a_fake_wf_{i}")
        wf.add_tasks([task])
        wf.bind()
        wf._bind_tasks()
        wfs.append(wf.workflow_id)

    n1 = time.time()
    app_route = f"/workflow_status"
    r1, m1 = wf.requester.send_request(
        app_route=app_route, message={"workflow_id": wfs, "limit": 5}, request_type="get"
    )
    n2 = time.time()
    new_route = n2 - n1
    assert r1 == 200
    assert new_route < 0.25