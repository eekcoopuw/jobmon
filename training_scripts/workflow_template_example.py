def workflow_template_example():
    import os
    import sys
    import getpass
    import uuid

    from jobmon.client.tool import Tool

    """
    Instructions:

      The steps in this example are:
      1. Create a tool
      2. Create  workflow using the tool from step 1
      3. Create task templates using the tool from step 1
      4. Create tasks using the template from step 3
      5. Add created tasks to the workflow
      6. Run the workflow

    To actually run the provided example:
      Make sure Jobmon is installed in your activated conda environment, and that you're on
      the Slurm cluster in a srun session. From the root of the repo, run:
         $ python training_scripts/workflow_template_example.py
    """

    user = getpass.getuser()
    wf_uuid = uuid.uuid4()
    script_path = os.path.abspath(os.path.dirname(__file__))

    # Create a tool
    tool = Tool(name="example_tool")

    # Create a workflow, and set the executor
    workflow = tool.create_workflow(
        name=f"template_workflow_{wf_uuid}",
    )

    # Create task templates
    echo_template = tool.get_task_template(
        default_compute_resources={
            "queue": "all.q",
            "cores": 1,
            "memory": "1G",
            "runtime": "1m",
            "stdout": f"/ihme/scratch/users/{user}/{wf_uuid}",
            "stderr": f"/ihme/scratch/users/{user}/{wf_uuid}",
            "project": "proj_scicomp"
        },
        template_name="quickstart_echo_template",
        default_cluster_name="slurm",
        command_template="echo {output}",
        node_args=["output"],
    )

    python_template = tool.get_task_template(
        default_compute_resources={
            "queue": "all.q",
            "cores": 2,
            "memory": "2G",
            "runtime": "10m",
            "stdout": f"/ihme/scratch/users/{user}/{wf_uuid}",
            "stderr": f"/ihme/scratch/users/{user}/{wf_uuid}",
            "project": "proj_scicomp"
        },
        template_name="quickstart_python_template",
        default_cluster_name="slurm",
        command_template="{python} {script_path} --args1 {val1} --args2 {val2}",
        node_args=["val1", "val2"],
        op_args=["python", "script_path"],
    )

    # Create tasks
    task1 = echo_template.create_task(name="task1", output="task1")

    task2 = echo_template.create_task(
        name="task2", upstream_tasks=[task1], output="task2"
    )

    task3 = python_template.create_task(
        name="task3",
        upstream_tasks=[task2],
        python=sys.executable,
        script_path=os.path.join(script_path, "test_scripts/test.py"),
        val1="val1",
        val2="val2",
    )

    # add task to workflow
    workflow.add_tasks([task1, task2, task3])

    # run workflow
    workflow.run()


if __name__ == "__main__":
    workflow_template_example()
