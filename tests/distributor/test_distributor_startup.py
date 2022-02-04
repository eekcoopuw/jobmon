import sys
from subprocess import PIPE, Popen, TimeoutExpired


def test_startup(tool, task_template, client_env):
    t1 = task_template.create_task(arg="echo 1", cluster_name="sequential")
    workflow = tool.create_workflow(name="test_instantiate_queued_jobs")

    workflow.add_tasks([t1])
    workflow.bind()
    wfr = workflow._create_workflow_run()

    cmd = [
        sys.executable, "-m",  # safest way to find the entrypoint
        "jobmon.client.distributor.cli", "start",
        "--cluster_name", "sequential",
        "--workflow_run_id", str(wfr.workflow_run_id)
    ]
    distributor_proc = Popen(cmd, stdout=PIPE, stderr=PIPE, universal_newlines=True)

    time_remaining = 180
    wait_interval = 10
    while time_remaining > 0:

        try:
            breakpoint()
            out, err = distributor_proc.communicate(timeout=wait_interval)
        except TimeoutExpired:
            time_remaining -= wait_interval
        else:
            if "ALIVE" in out:
                time_remaining = 0
            else:
                time_remaining -= wait_interval
