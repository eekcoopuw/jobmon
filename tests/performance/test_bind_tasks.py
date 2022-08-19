import pytest

import time


@pytest.mark.performance_tests
def test_get_workflow_status(tool):
    t = tool

    tt1 = t.get_task_template(
        template_name="tt_core", command_template="{n} {a1} {a2} {a3} {a4} {a5}",
        node_args=["n"],
        task_args=["a1", "a2", "a3", "a4", "a5"]
    )
    wf = t.create_workflow(name=f"i_am_a_fake_wf_1")
    tasks = []
    for i in range(1, 5000):
        task = tt1.create_task(
            n=f"a{i}",
            a1=f"a{i}",
            a2=f"a{i}",
            a3=f"a{i}",
            a4=f"a{i}",
            a5=f"a{i}",
            task_attributes={"aa": "a", "bb": "b", "cc": "c"},
            cluster_name="sequential",
            compute_resources={"queue": "null.q", "num_cores": 1},
        )
        tasks.append(task)
    wf.add_tasks(tasks)
    wf.bind()


    n1 = time.time()
    wf._bind_tasks()
    n2 = time.time()
    new = n2 - n1

    wf = t.create_workflow(name=f"i_am_a_fake_wf_2")
    for i in range(1, 5000):
        task = tt1.create_task(
            n=f"b{i}",
            a1=f"b{i}",
            a2=f"b{i}",
            a3=f"b{i}",
            a4=f"b{i}",
            a5=f"b{i}",
            task_attributes={"aa": "a", "bb": "b", "cc": "c"},
            cluster_name="sequential",
            compute_resources={"queue": "null.q", "num_cores": 1},
        )
        tasks.append(task)
    wf.add_tasks(tasks)
    wf.bind()

    o1 = time.time()
    wf._bind_tasks_old()
    o2 = time.time()
    old = o2 - o1

    import pdb
    pdb.set_trace()
    print("abc")
