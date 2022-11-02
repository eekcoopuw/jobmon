**********
Quickstart
**********

Jobmon is a job-control system used for automating scientific workflows and running them on
distributed computing systems. It manages complex job and resource dependencies and manages
computing environment instability, ensuring dependably and assisting in troubleshooting when
needed. It is developed and maintained by IHME's Scientific Computing team.

Jobmon’s vision is to make it as easy as possible for everyone to run any kind of code
on any compute platform, reliably, and efficiently.
Jobmon should allow people to sleep easily on the
weekend because they do not have to manually monitor their applications.

.. include:: quickstart-ihme.rst

Getting Started
###############
The Jobmon controller script (i.e. the code defining the workflow) must be
written in Python or R. The modeling code can be in Python, R, Stata, C++, or in fact any
language.

The controller script interacts with Jobmon by creating a :ref:`jobmon-workflow-label` and
then iteratively adding :ref:`jobmon-task-label` to it. Each Workflow is uniquely defined by its
:ref:`jobmon-wf-arg-label` and its set of Tasks.

Jobmon allows you to resume workflows (see :ref:`jobmon-resume-label`). A Workflow can only
be resumed if the WorkflowArgs and all Tasks added to it are
exact matches to the previous Workflow.

Create a Workflow
#################

A Workflow is essentially a set of Tasks, their configuration details, and the
dependencies between them.
For example, a series of jobs that models one disease could be a Workflow.


A task is a single executable object in the workflow; a command that will be run.

A dependency from Task A to Task B means that B will not execute until A
has successfully completed. We say that Task A is _upstream_ of Task B.
Conversely, Task B is _downstream_ of Task A. If A always fails (up to its retry
limit) then B will never be started, and the Workflow as a whole will fail.

In general a task can have many upstreams. A Task will not start until all of its
upstreams have successfully completed, potentially after multiple attempts.

The Tasks and their dependencies form a
`directed-acyclic graph (DAG) <https://en.wikipedia.org/wiki/Directed_acyclic_graph>`_.
where the tasks are the nodes, and the edges are the dependencies.

For more about the objects go to :doc:`Core Concepts <core_concepts>`.

Constructing a Workflow and adding a few Tasks is simple:

.. code-tabs::

    .. code-tab:: python
      :title: Python

        import getpass
        import os
        import sys
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
        script_path = '/mnt/team/scicomp/pub/docs/training_scripts/test.py'

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
                "stdout": f"/ihme/scratch/users/{user}",
                "stderr": f"/ihme/scratch/users/{user}",
                "project": "proj_scicomp",
                "constraints": "archive"  # To request a J-drive access node
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
                "stdout": f"/ihme/scratch/users/{user}",
                "stderr": f"/ihme/scratch/users/{user}",
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
            script_path=script_path,
            val1="val1",
            val2="val2",
        )

        # add task to workflow
        workflow.add_tasks([task1, task2, task3])

        # run workflow
        workflow.run()

    .. code-tab:: R
      :title: R

      library(jobmonr)

      # Create a workflow
      username <- Sys.getenv("USER")
      script_path <- '/mnt/team/scicomp/pub/docs/training_scripts/test.py'


      # Create a tool
      my_tool <- tool(name='r_example_tool')

      # Set the tool compute resources
      jobmonr::set_default_tool_resources(
        tool=my_tool,
        default_cluster_name='slurm',
        resources=list(
          'cores'=1,
          'queue'='all.q',
          'runtime'="2m",
          'memory'='1G'
        )
      )

      # Bind a workflow to the tool
      wf <- workflow(my_tool,
                    workflow_args=paste0('template_workflow_', Sys.Date()),
                    name='template_workflow')

      # Create an echoing task template
      echo_tt <- task_template(tool=my_tool,
                              template_name='echo_template',
                              command_template='echo {echo_str}',
                              node_args=list('echo_str'))


      # Create template to run our script
      script_tt <- task_template(tool=my_tool,
                                template_name='test_templ',
                                command_template=paste(
                                  Sys.getenv("RETICULATE_PYTHON"),
                                  '{script_path}',
                                  '--args1 {val1}',
                                  '--args2 {val2}',
                                  sep=" "),
                                task_args=list('val1', 'val2'),
                                op_args=list('script_path'))


      # Optional: default resources can be updated at the task or task template level
      jobmonr::set_default_template_resources(
        task_template=script_tt,
        default_cluster_name='slurm',
        resources=list(
          'queue'='long.q',
          'constraints'='archive',
        )
      )

      # Create two echoing tasks
      task1 <- task(task_template=echo_tt,
                    name='echo_1',
                    echo_str="task1")

      task2 <- task(task_template=echo_tt,
                    name='echo_2',
                    upstream_tasks=list(task1), # Depends on the previous task,
                    echo_str="task2")

      # Add the test script task
      test_task <- task(task_template=script_tt,
                        name='test_task',
                        upstream_tasks=list(task2),
                        val1="val1",
                        val2="val2",
                        script_path=script_path
      )

      # Add tasks to the workflow
      wf <- add_tasks(wf, list(task1, task2, test_task))

      # Run it
      wfr <- run(
        workflow=wf,
        resume=FALSE,
        seconds_until_timeout=7200)

.. note::
    Unique Workflows: If you know that your Workflow is to be used for a
    one-off project only, you may choose to use an anonymous Workflow, meaning
    you leave workflow_args blank. In this case, WorkflowArgs will default to
    a UUID which, as it is randomly generated, will be harder to remember and
    thus is not recommended for use cases outside of the one-off project. A workflow's
    uniqueness is based on its command, upstreams and downstreams, and workflow_args.

Compute Resources
#################

Compute Resources are used to allocate resources to your tasks.
You can specify memory, cores, runtime, queue, stdout, stderr, and project.

For IHME's Slurm cluster the defaults for all queues are:
* One core
* 1G memory, and
* Ten minutes runtime.

These values might change in the future.

You can specify that you want to run your jobs on an "archive" node
(i.e., a node with access to /snfs1
a.k.a "the J-drive"). Add the following key value pair to
their compute resources: ``"constraints": "archive"``.


.. note::
    By default Workflows are set to time out if all of your tasks haven't
    completed after 10 hours (36,000 seconds). If your Workflow times out
    before your tasks have finished running, those tasks will continue
    running, but you will need to restart your Workflow again. You can change
    the Workflow timeout period if your tasks combined run longer than 10 hours.


.. include:: help-ihme.rst