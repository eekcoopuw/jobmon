import pytest

import time


@pytest.mark.performance_tests
def test_get_workflow_status(tool):
    t = tool

    tt1 = t.get_task_template(
        template_name="tt_core", command_template="echo {arg}", node_args=["arg"]
    )
    wfs = []
    for i in range(1, 5000):
        task = tt1.create_task(
            arg=i,
            cluster_name="sequential",
            compute_resources={"queue": "null.q", "num_cores": 4},
        )
        wf = t.create_workflow(name=f"i_am_a_fake_wf_{i}")
        wf.add_tasks([task])
        wf.bind()


    n1 = time.time()
    wf._bind_tasks()
    n2 = time.time()
    new = n2 - n1

    wfs = []
    for i in range(1, 5000):
        task = tt1.create_task(
            arg=i,
            cluster_name="sequential",
            compute_resources={"queue": "null.q", "num_cores": 4},
        )
        wf = t.create_workflow(name=f"i_am_a_fake_wf_{i}")
        wf.add_tasks([task])
        wf.bind()

    o1 = time.time()
    wf._bind_tasks_old()
    o2 = time.time()
    old = o2 - o1

    import pdb
    pdb.set_trace()
