**********
Quickstart
**********

Jobmon is a job-control system used for automating scientific workflows and running them on
distributed computing systems. It manages complex job and resource dependencies and manages
computing environment instability, ensuring dependably and assisting in troubleshooting when
needed. It is developed and maintained by IHME's Scientific Computing team.

Jobmon’s vision is to make it as easy as possible for everyone at IHME to run any kind of code
on any compute platform, reliably, and efficiently.

Install
#######

plugins
*******
Jobmon has the capability to run jobs on both the Slurm and UGE clusters. At present, it also
has limited capabilities for executing Tasks locally on a single machine using either
sequential execution or multiprocessing.

To use either of the clusters with Jobmon users need to install their Jobmon plugin. If a user
wants to use Slurm with Jobmon, they would need to have the core Jobmon software and the
Jobmon Slurm plugin installed. If a user wants to use UGE with Jobmon, they would need to have
core Jobmon and the Jobmon UGE plugin installed.

Users can either install Jobmon core and the plugins individually using "pip" or they can
install Jobmon core, the UGE plugin, and Slurm plugin all together with a single conda command.

conda install
*************
To install core Jobmon and both plugins using conda::

    conda install ihme_jobmon -k --channel https://artifactory.ihme.washington.edu/artifactory/api/conda/conda-scicomp --channel conda-forge

pip install
***********
To install just core jobmon (no cluster plugins) via pip::

    pip install jobmon

To install the preconfigured UGE and Slurm plugins::

    pip install jobmon_installer_ihme

To install both at once via pip::

    pip install jobmon[ihme]

Then issue the following command to configure the web service and port:

    .. webservicedir::

.. note::
    If you get the error **"Could not find a version that satisfies the requirement jobmon (from version: )"** then create (or append) the following to your ``~/.pip/pip.conf``::

        [global]
        extra-index-url = https://artifactory.ihme.washington.edu/artifactory/api/pypi/pypi-shared/simple
        trusted-host = artifactory.ihme.washington.edu


Jobmon Learning
###############
For a deeper dive in to Jobmon, check out some of our courses:
    1. `About Jobmon <https://hub.ihme.washington.edu/pages/viewpage.action?pageId=74531156>`_.
    2. `Learn Jobmon <https://hub.ihme.washington.edu/pages/viewpage.action?pageId=78062050>`_.
    3. `Jobmon Retry <https://hub.ihme.washington.edu/pages/viewpage.action?pageId=78062056>`_.
    4. `Jobmon Resume <https://hub.ihme.washington.edu/pages/viewpage.action?pageId=78062059>`_.

These courses are occasionally offered in-person. Check IHME Learn to see if there are any
upcoming trainings.

Getting Started
###############
The Jobmon controller script (i.e. the code defining the workflow) has to be
written in Python or R. The modeling code can be in Python, R, Stata, C++, or in fact any
language.

Users will primarily interact with Jobmon by creating a :ref:`jobmon-workflow-label` and
iteratively adding :ref:`jobmon-task-label` to it. Each Workflow is uniquely defined by its
:ref:`jobmon-wf-arg-label` and the set of Tasks attached to it.

Jobmon allows you to resume workflows (:ref:`jobmon-resume-label`). A Workflow can only
be resumed if the WorkflowArgs and all Tasks added to it are shown to be
exact matches to the previous Workflow.

Create a Workflow
#################

A Workflow is a framework by which a user may define the relationship between
Tasks and define the relationship between multiple runs of the same set of Tasks.

A task is a single executable object in the workflow, a command that will be run.

A Workflow represents a set of Tasks which may depend on one another such
that if each relationship were drawn (Task A) -> (Task B) meaning that Task B
depends on Task A, it would form a `directed-acyclic graph (DAG) <https://en.wikipedia.org/wiki/Directed_acyclic_graph>`_.

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
                "stdout": f"/ihme/scratch/users/{user}",
                "stderr": f"/ihme/scratch/users/{user}",
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
            script_path=os.path.join(script_path, "test_scripts/test.py"),
            val1="val1",
            val2="val2",
        )

        # add task to workflow
        workflow.add_tasks([task1, task2, task3])

        # run workflow
        workflow.run()

    .. code-tab:: R
      :title: R

      Sys.setenv("RETICULATE_PYTHON"='/mnt/team/scicomp/envs/jobmon/bin/python')  # Set the Python interpreter path
      library(jobmonr)

      # Create a workflow
      username <- Sys.getenv("USER")
      script_path <- '/mnt/team/scicomp/training/test_scripts/test.py'  # Update with your repository installation

      # Templates are not supported in the R client, since there are no Jobmon 1.* R clients.
      # Create a tool

      my_tool <- tool(name='r_example_tool')

      # Bind a workflow to the tool
      wf <- workflow(tool,
        workflow_args=paste0('template_workflow_', Sys.Date()),
        name='template_workflow')

      # Create an echoing task template
      echo_tt <- task_template(tool=my_tool,
        template_name='echo_templ',
        command_template='echo {}',
        task_args=list('echo_str'))


      # Create template to run our script
      script_tt <- task_template(tool=my_tool,
        template_name='test_templ',
        command_template=paste0(Sys.getenv("RETICULATE_PYTHON"), ' ', script_path, ' --args1 {val1} --args2 {val2}'),
        task_args=list('val1', 'val2'))

      # Set the echo task template compute resources
      echo_tt_resources <- jobmonr::set_default_template_resources(
          task_template=echo_tt,
          default_cluster_name='buster',
          resources=list(
            'cores'=1,
            'queue'='all.q',
            'runtime'="2m",
            'memory'='1G'
          )
        )

      # Set the script task template compute resources
      script_tt_resources <- jobmonr::set_default_template_resources(
          task_template=script_tt,
          default_cluster_name='buster',
          resources=list(
            'cores'=1,
            'queue'='all.q',
            'runtime'="2m",
            'memory'='1G'
          )
        )

      # Create two sleepy tasks
      task1 <- task(task_template=echo_tt,
        executor_parameters=copy(params),  # Copied to prevent parallel resource scaling
        name='echo_1',
        echo_str="task1")

      task2 <- task(task_template=echo_tt,
        executor_parameters=copy(params),
        name='echo_2',
        upstream_tasks=list(task1), # Depends on the previous task,
        echo_str="task2")

      # Add the test script task
      test_task <- task(task_template=tt,
        executor_parameters=copy(params),
        name='test_task',
        upstream_tasks=list(task2),
        val1="val1",
        val2="val2"
        )

      # Add tasks to the workflow
      wf <- add_tasks(wf, list(task1, task2, task3))

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
    uniqueness is based on it's command, upstreams and downstreams, and workflow_args.

Compute Resources: Compute resources are used to allocate resources for your tasks. Users are
able to specify requested memory, cores, runtime, queue, stdout, stderr, and project. Compute
resources can be set at the Task, TaskTemplate, Workflow and Tool level. If compute resources
are set on multiple objects, Jobmon has a hierarchy of which resources will take precedence,
the hierarchy is Task -> TaskTemplate -> Workflow -> Tool. To set compute resources on Tasks, use
"compute_resources". To set compute resources on TaskTemplate, Workflow, and Tool, use
"default_compute_resources".

By default compute resources on the Slurm cluster: cores will be 1, memory will be 1G, and
runtime will be 10 minutes.

Cluster name: You can specify the cluster you want to use on the Task, TaskTemplate, Workflow
and Tool level. To set cluster name on Tasks, use "cluster_name". To set cluster_name on
TaskTemplate, Workflow, and Tool, use "default_cluster_name". If cluster name
is set on multiple Jobmon objects, Jobmon has a hierarchy of which cluster will take precedence,
the hierarchy is Task -> TaskTemplate -> Workflow -> Tool.

.. note::
    By default Workflows are set to time out if all of your tasks haven't
    completed after 10 hours (or 36000 seconds). If your Workflow times out
    before your tasks have finished running, those tasks will continue
    running, but you will need to restart your Workflow again. You can change
    this if your tasks combined run longer than 10 hours.

.. note::
    Errors with a return code of 199 indicate an issue occurring within Jobmon
    itself. Errors with a return code of 137, 247, or -9 indicate resource errors.

Getting Additional Help
#######################
The Scientific Computing team is always available to answer your questions or to consult on
Jobmon.

To contact the team via Slack:
    - #jobmon-users to ask questions or raise concerns about Jobmon.

To set up a consultation:
    - Create a Help Desk ticket asking for a consultation:
      `SciComp Help Desk <https://help.ihme.washington.edu/servicedesk/customer/portal/16>`_.
    - A Scientific Computing team member will reach out to you to schedule a consultation
      meeting.

To raise a Scientific Computing help desk request:
    - `SciComp Help Desk <https://help.ihme.washington.edu/servicedesk/customer/portal/16>`_.

When requesting help try to provide the team with as much information as you have about your
problem. *Please include your Workflow id, the Jobmon version that you're using, and any
TaskInstance error logs that you have.*
