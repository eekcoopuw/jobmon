
Quickstart
##########

Jobmon is a job-control system used for automating scientific workflows and running them on
distributed computing systems. It manages complex job and resource dependencies and manages
computing environment instability to execute dependably and assist in troubleshooting when
needed. It is developed and maintained by IHME's Scientific Computing team.

Jobmon’s vision is to make it as easy as possible for everyone at IHME to run any kind of code
on any compute platform, reliably, and efficiently.

Install
*******
To get started::

    pip install jobmon

.. note::
    If you get the error **"Could not find a version that satisfies the requirement jobmon (from version: )"** then create (or append) the following to your ``~/.pip/pip.conf``::

        [global]
        extra-index-url = https://artifactory.ihme.washington.edu/artifactory/api/pypi/pypi-shared/simple
        trusted-host = artifactory.ihme.washington.edu

.. note::

    Jobmon is intended to be used on the SGE cluster. At present, it has
    limited capabilities for executing Tasks locally on a single machine using
    either sequential execution or multiprocessing. These local task-management
    capabilities will be improved going forward, but SGE support will always be
    the primary goal for the project.

Jobmon Learning
***************
For a deeper dive in to Jobmon, check out some of our courses:
    1. `About Jobmon <https://hub.ihme.washington.edu/pages/viewpage.action?pageId=74531156>`_.
    2. `Learn Jobmon <https://hub.ihme.washington.edu/pages/viewpage.action?pageId=78062050>`_.
    3. `Jobmon Retry <https://hub.ihme.washington.edu/pages/viewpage.action?pageId=78062056>`_.
    4. `Jobmon Resume <https://hub.ihme.washington.edu/pages/viewpage.action?pageId=78062059>`_.

These courses are occasionally offered in-person. Check IHME Learn to see if there are any
upcoming trainings.

Getting Started
***************
The Jobmon controller script (i.e. the code defining the workflow) has to be
written in Python or R. The modeling code can be in Python, R, Stata, C++, or in fact any
language.

Users will primarily interact with Jobmon by creating a :term:`Workflow` and iteratively
adding :term:`Task` to it. Each Workflow is uniquely defined by its
:term:`WorkflowArgs` and the set of Tasks attached to it. A Workflow can only
be resumed if the WorkflowArgs and all Tasks added to it are shown to be
exact matches to the previous Workflow.

Create a Workflow
*****************

A Workflow is a framework by which a user may define the relationship between
Tasks and define the relationship between multiple runs of the same set of Tasks.

A task is a single executable object in the workflow, a command that will be run.

A Workflow represents a set of Tasks which may depend on one another such
that if each relationship were drawn (Task A) -> (Task B) meaning that Task B
depends on Task A, it would form a `directed-acyclic graph (DAG) <https://en.wikipedia.org/wiki/Directed_acyclic_graph>`_.
A Workflow is further uniquely identified by a set of WorkflowArgs which are
required if the Workflow is to be resumable.

For more about the objects go to the :doc:`Workflow and Task Reference <api/jobmon.client>`
or :doc:`Executor Parameter Reference <api/jobmon.client.execution>`

Constructing a Workflow and adding a few Tasks is simple:

.. code-tabs::

    .. code-tab:: python
      :title: Python

      import os
      import getpass
      import uuid

      from jobmon.client.api import Tool, ExecutorParameters

      def workflow_template_example():
      """
      Instructions:

        The steps in this example are:
        1. Create a tool
        2. Create  workflow using the tool from step 1
        3. Create executor parameters to use with the tasks
        4. Create task templates using the tool from step 1
        5. Create tasks using the template from step 3
        6. Add created tasks to the workflow
        7. Run the workflow

      To actually run the provided example:
        with Jobmon installed in your conda environment from the root of the repo, run:
           $ python training_scripts/workflow_template_example.py
      """

      user = getpass.getuser()
      wf_uuid = uuid.uuid4()
      script_path = os.path.abspath(os.path.dirname(__file__))

      # Create a tool
      tool = Tool.create_tool(name="example tool")

      # Create a workflow, and set the executor
      workflow = tool.create_workflow(
        name = f"template_workflow_{wf_uuid}",
        description = "template_workflow")
      workflow.set_executor(
        executor_class='SGEExecutor',
        stderr = f"/ihme/scratch/users/{user}/{wf_uuid}",
        stdout = f"/ihme/scratch/users/{user}/{wf_uuid}",
        project = "proj_scicomp"  # specify your team's project
      )

      # Create task templates
      echo_template = tool.get_task_template(
        template_name='echo_template',
        command_template='echo {output}',
        task_args=['output'])

      python_template = tool.get_task_template(
        template_name='python_template',
        command_template='{python} {script_path} --args1 {val1} --args2 {val2}',
        task_args=['val1', 'val2'],
        op_args=['python', 'script_path'])

      # Create an executorparameters object for each task template
      echo_parameters = ExecutorParameters(
        num_cores=1,
        queue='all.q',
        max_runtime_seconds=10,
        m_mem_free='128M')

      python_parameters = ExecutorParameters(
        num_cores=2,
        queue='all.q',
        max_runtime_seconds=1000,
        m_mem_free='2G')


      # Create tasks
      task1 = echo_template.create_task(
        executor_parameters=echo_parameters,
        name='task1',
        output='task1'
      )

      task2 = echo_template.create_task(
        executor_parameters=echo_parameters,
        name='task2',
        upstream_tasks = [task1],
        output='task2'
      )

      task3 = python_template.create_task(
        executor_parameters=python_parameters,
        name='task3',
        upstream_tasks=[task2],
        python=sys.executable,
        script_path=os.path.join(script_path, 'test_scripts/test.py'),
        val1='val1',
        val2='val2'
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

      # Set the executor
      wf <- set_executor(wf, executor_class='SGEExecutor')

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


      # Define executor parameters for our tasks
      params <- executor_parameters(num_cores=1,
        m_mem_free="1G",
        queue='all.q',
        max_runtime_seconds=100)

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

Default Executor Parameters: ExecutorParameters are used to allocate resources for your tasks.
ExecutorParameters are specific to their given Executor. Jobmon current has the following
executors: SGE, Sequential, and Multiprocess.

Tasks, such as BashTask, PythonTask, etc. take many qsub-type arguments, that you can use to
specify ExecutorParameters. For the SGE executor you are able to specify number of
cores (num_cores), memory (m_mem_free), and runtime (max_runtime_seconds). By default, num_cores
used will be 1, mem_free will be 1G, and max attempts will be 3. Stderr, stdout, project,
and working_dir (if desired) are set at the Workflow level (see below).

Example of adding ExecutorParameters to a Task:

.. code-tabs::

    .. code-tab:: python
      :title: Python

        from jobmon.client.api import ExecutorParameters
        from jobmon.client.templates.bash_task import BashTask

        #Create ExecutorParameter
        executor_parameters_example = ExecutorParameters(
            m_mem_free = "1G",
            num_cores = 1,
            queue = "all.q",
            max_runtime_seconds = 60,
            executor_class="SGEExecutor"
        )

        #Create task and assign the ExecutorParameter to it
        task1 = BashTask(
            command = "echo task1",
            executor_parameters = executor_parameters_example
        )

    .. code-tab:: R
      :title: R

        library(jobmonr)
        executor_parameters_example <- executor_parameters(
            m_mem_free="1G",
            num_cores=1,
            queue='all.q',
            max_runtime_seconds=60,
            executor_class="SGEExecutor")


Additional Arguments: If you need to launch a Python, R, or Stata job, but
usually do so with a shellscript that sets environment variables before
running the full program, you can pass these environment variables to your
Jobmon Task, in the form of a dictionary. These will then be formatted and
prepended to the command, so that all environment variables will be set on
each node where the code executes. These additional arguments are called
context_args.

For example if you wanted to specify a host to run on, you would add context_args to a
task's ExecutorParameters

.. code-tabs::

    .. code-tab:: python
      :title: Python

        #Create ExecutorParameter
        executor_parameters_example = ExecutorParameters(
            m_mem_free = "1G",
            num_cores = 1,
            queue = "all.q",
            max_runtime_seconds = 60,
            executor_class="SGEExecutor",
            context_args={"sge_add_args": "-l hostname=<hostname>"}
        )

    .. code-tab:: R
        :title: R

        # Create Executor Parameter
        executor_parameters_example <- executor_parameters(
            m_mem_free = "1G",
            num_cores = 1,
            queue = "all.q",
            max_runtime_seconds = 60,
            executor_class="SGEExecutor",
            context_args=list("sge_add_args"="-l hostname=<hostname>")
        )

.. note::
    By default Workflows are set to time out if all of your tasks haven't
    completed after 10 hours (or 36000 seconds). If your Workflow times out
    before your tasks have finished running, those tasks will continue
    running, but you will need to restart your Workflow again. You can change
    this if your tasks combined run longer than 10 hours.

.. note::
    Errors with a return code of 199 indicate an issue occurring within Jobmon
    itself. Errors with a return code of 137 or 247 indicate resource errors.

Nodes, TaskTemplates, and Tools
===============================
Nodes are the object representing a Task within a DAG. It simply keeps track of where a
Task is and what attributes make the task unique within the DAG. Tasks
will often be created from a TaskTemplate and they will vary somewhat e.g. by location, this
variation is what makes a Node unique.

TaskTemplates are the underlying structure of a given Task. A user defines a command template that
individual Tasks will fill in with varying arguments. A Task's uniqueness is defined by it's
NodeArgs and TaskArgs. A Task Template can be used in different Workflows and is
associated with a given Tool. TaskTemplates can also be versioned, meaning you can iterate
upon them. A user would create a new version of their TaskTemplate if the command changes or
if the underlying methods change in a way that the user wants to recognize as different from
before.

A tool is the project (e.g. STG-PR, CODCorrect) that you want to associate your Workflow and
Task Templates with. A Tool keeps track of where Workflows and Tasks are within the IHME
pipeline.

For example::

    import os
    import sys
    import getpass
    from jobmon.client.api import Tool, ExecutorParameters

    # This example can also be found at "/jobmon/training_scripts/tool_template_example.py"
    def tool_template_example():
        """
        Instructions:
            In this example the Workflow consists of three phases. These phases are: Transform,
            Aggregate, and Summarize

            The steps in this example are:
            1. Create a Tool and Workfow
            2. Create a TaskTemplate
            3. Define ExecutorParameters for the Tasks
            4. Create a Task by specifying a the TaskTemplate that is created in step two
            5. Add Tasks to the Workflow
            6. Run the Workflow

        To Run:
            With Jobmon installed in your conda environment from the root of the repo, run:
               $ python training_scripts/tool_template_example.py
        """

        # Define some dummy variables for testing
        locations = list(range(10)) # dummy data
        sexes = list(range(2))       # dummy data
        location_hierarchy_id = 0   # dummy data
        user = getpass.getuser()
        script_path = os.path.abspath(os.path.dirname(__file__))

        # Create a Tool, Workflow and set the Executor
        jobmon_tool = Tool.create_tool(name="jobmon_testing_tool")
        """
        Only call this when you explicitly want to create a new version of your Tool
        (i.e. when you have done an overhaul of your Workflow or you want to indicate
        widespread changes within the tool). We do not recommend creating a new version for
        every run because it will be difficult to see which runs are related.

        jobmon_tool = Tool(name="jobmon_testing_tool")
        jobmon_tool.create_new_tool_version()
        """
        workflow = jobmon_tool.create_workflow(name="jobmon_workflow")
        workflow.set_executor(
            executor_class="SGEExecutor",
            project="proj_scicomp"  # specify your team's project
        )

        # Create Template
        """
        There is only one summarize job. It will take the whole hierarchy of locations
        and write a file for each of the location. Therefore, the number of nodes created
        in the dag will not be dictated by the location hierarchy id. The script will
        need the location hierarchy id to create the correct output, therefore location
        hierarchy is not a NodeArg, it is a TaskArg.
        """
        template_transform = jobmon_tool.get_task_template(
            template_name = "transform",
            command_template = "{python} {script} --location_id {location_id} --sex_id {sex_id} --output_file_path {output_file_path}",
            node_args = ["location_id", "sex_id"],
            task_args = ["output_file_path"],
            op_args = ["python", "script"]
        )
        template_aggregate = jobmon_tool.get_task_template(
            template_name = "aggregate",
            command_template = "{python} {script} --location_id {location_id} --output_file_path {output_file_path}",
            node_args = ["location_id"],
            task_args = ["output_file_path"],
            op_args = ["python", "script"]
        )
        template_summarize = jobmon_tool.get_task_template(
            template_name = "summarize",
            command_template = "{python} {script} --location_hierarchy_id {location_hierarchy_id} --output_file_path {output_file_path}",
            node_args = [],
            task_args = ["location_hierarchy_id", "output_file_path"],
            op_args = ["python", "script"]
        )

        # Set ExecutorParameters
        executor_parameters_transform = ExecutorParameters(
            m_mem_free = "1G",
            num_cores = 1,
            queue = "all.q",
            max_runtime_seconds = 60
        )
        executor_parameters_aggregate = ExecutorParameters(
            m_mem_free = "2G",
            num_cores = 2,
            queue = "long.q",
            max_runtime_seconds = 120
        )
        executor_parameters_summarize = ExecutorParameters(
            m_mem_free = "3G",
            num_cores = 3,
            queue = "all.q",
            max_runtime_seconds = 180
        )

        # Create Task
        task_all_list = []
        # Tasks for the transform phase
        task_transform_by_location = {}
        for location_id in locations:
            task_location_list = []
            for sex_id in sexes:
                task = template_transform.create_task(
                    executor_parameters = executor_parameters_transform,
                    name = f"transform_{location_id}_{sex_id}",
                    upstream_tasks = [],
                    max_attempts = 3,
                    python = sys.executable,
                    script = os.path.join(script_path, 'test_scripts/transform.py'),
                    location_id = location_id,
                    sex_id = sex_id,
                    output_file_path = f"/ihme/scratch/users/{user}/{workflow.name}/transform",
                    task_attributes = {"release_id": 3}
                )
                # Append Task to Workflow and the list
                task_all_list.append(task)
                task_location_list.append(task)
            # Create dictionary by location
            task_transform_by_location[location_id] = task_location_list

        # Tasks for the aggregate phase
        task_aggregate_list = []
        for location_id in locations:
            upstreams_tasks = task_transform_by_location[location_id]
            task = template_aggregate.create_task(
                executor_parameters = executor_parameters_aggregate,
                name = f"aggregate_{location_id}",
                upstream_tasks = upstreams_tasks,
                max_attempts = 3,
                python = sys.executable,
                script = os.path.join(script_path, 'test_scripts/aggregate.py'),
                location_id = location_id,
                output_file_path = f"/ihme/scratch/users/{user}/{workflow.name}/aggregate",
                task_attributes = {"location_set_version_id": 35}
            )
            task_all_list.append(task)
            task_aggregate_list.append(task)

        # Tasks for the summarize phase
        task = template_summarize.create_task(
            executor_parameters = executor_parameters_summarize,
            name = f"summarize_{location_hierarchy_id}",
            upstream_tasks = task_aggregate_list,
            max_attempts = 1,
            python = sys.executable,
            script = os.path.join(script_path, 'test_scripts/summarize.py'),
            location_hierarchy_id = location_hierarchy_id,
            output_file_path = f"/ihme/scratch/users/{user}/{workflow.name}/summarize"
        )
        task_all_list.append(task)

        # Add tasks to the workflow
        workflow.add_tasks(task_all_list)

        # Run the workflow
        workflow.run()

Logging
===============================
To attach Jobmon's simple formatted logger use the following code.

For example::

    from jobmon.client.client_logging import ClientLogging

    ClientLogging().attach()

Jobmon Commands
=======================================
The Jobmon status
commands allow you to check that status of your Workflows and Tasks from the
command line.

To use the status commands:
    1. Open a new terminal window
    2. SSH in to the cluster
    3. qlogin
    4. Activate the same conda environment that your Tasks are running in

There are currently three supported commands:

**workflow_status**
    Entering ``jobmon workflow_status`` in to the command line will show you
    a table of how many tasks are in each state within that workflow. You
    can specify the workflow by user using the -u flag. For example:
    ``jobmon workflow_status -u {user}``. You can also specify the workflow
    using the -w flag. For example: ``jobmon workflow_status -w 9876``.
    You can also use the -w flag to specify multiple workflows at the same
    time. For example, if you have one workflow named 9876 and one
    workflow named 1234 you would enter ``jobmon workflow_status -w 9876 1234``.

**workflow_tasks**
    Entering ``jobmon workflow_tasks`` in to the command line will show you
    the status of specific tasks in a given workflow. You can specify which
    workflow with the -w flag. For example: ``jobmon workflow_tasks -w 9876``.
    You can also add a -s flag to only query tasks that are in a certain
    state. For example: ``jobmon workflow_tasks -w 9876 -s PENDING`` will query all
    tasks within workflow 9876 that have the pending status. You may also query by multiple
    statuses. For example: ``jobmon workflow_tasks -w 9876 -s PENDING RUNNING``

**task_status**
    Entering ``jobmon task_status`` in to the command line will show you the
    state of each task instance for a certain task. You may specify the task
    by adding a -t flag. For example: ``jobmon task_status -t 1234``. You may also filter by
    multiple task ids and statuses. The -s flag will allow you to filter upon a specific status.
    For example, if you wanted to query all task instances in the Done state for task 1234 and
    task 7652 you would do the following ``jobmon task_status -t 1234 7652 -s done``

**JSON flag**
    A new flag has been added to the Jobmon CLI to allow users to return their workflow and
    task statuses in JSON format. To use this feature add a ``-n`` flag to any of the Jobmon
    CLI commands. For example: ``jobmon task_status -t 1234 7652 -s done -n``

Possible states: PENDING, RUNNING, DONE, FATAL

Jobmon Self-Service Commands
****************************

Jobmon 2.1.0 will introduce a suite of additional commands to not only visualize task statuses from the database, but to allow the users to modify the states of their workflows. These self-service commands can be invoked from the command line in the same way as the status commands.

There are two supported:

**concurrency_limit**
    Entering ``jobmon concurrency_limit`` will allow the user to change the maximum running task instances allowed in their workflow. When a workflow is instantiated, the user can specify a maximum limit to the number of concurrent tasks in case a very wide workflow threatens to resource-throttle the cluster. While running, the user can use this command to change the maximum allowed concurrency as needed if cluster busyness starts to wax or wane.

    As an example, if we plan on running 100,000 tasks at once and don't specify a default, Jobmon will ensure only 10,000 tasks at once will be queued and run. If the cluster is particularly free, the user can use ``jobmon concurrency_limit -w <workflow_id> -n 100000`` to run all 100,000 tasks simultaneously without interrupting the current workflow execution. If cluster usage starts to pick back up and we need to make space for others, we can use ``jobmon concurrency_limit -w <workflow_id> -n 100`` to ensure that only 100 tasks at once will be queued and that we can make space for others.

**update_task_status**

    Entering ``jobmon update_task_status`` allows the user to set the status of tasks in their workflow. This is helpful for either rerunning portions of a workflow that have already completed, or allowing a workflow to progress past a blocking error. The usage is ``jobmon update_task_status -t [task_ids] -w [workflow_id] -s [status]``

    There are 2 allowed statuses - "D" and "F". Specifying status "D" will mark only the listed task_ids as "D", and leave the rest of the DAG unchanged. When the workflow is resumed, the DAG executes as if the listed task_ids have finished successfully.

    If status "F" is specified, the listed task IDs will be set to "F" as well as all downstream dependents of those tasks. When the workflow is resumed, the specified tasks will be rerun and subsequently their downstream tasks as well. If the workflow has successfully completed, and is marked with status "D", the workflow status will be amended to status "E" in order to allow a resume.

    .. note::
        1. All status changes are propagated to the database.
        2. Only inactive workflows can have task statuses updated
        3. The updating user must have at least 1 workflow run associated with the requested workflow.
        4. The requested tasks must all belong to the specified workflow ID


A Workflow that retries Tasks if they fail
******************************************

By default a Task will be retried up to three times if it fails. This helps to
reduce the chance that random events on the cluster or landing on a bad node
will cause your entire Task and Workflow to fail.

In order to configure the number of times a Task can be retried, configure the
max_attempts parameter in the Task that you create. If you are still debugging
your code, please set the number of retries to zero so that it does not retry
code with a bug multiple times. When the code is debugged, and you are ready
to run in production, set the retries to a non-zero value.

The following example shows a configuration in which the user wants their Task
to be retried 4 times and it will fail up until the fourth time.::

    import getpass
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow as Workflow
    from jobmon.client.templates.python_task import PythonTask
    from jobmon.client.api import ExecutorParameters
    from jobmon.client.execution.strategies.sge import sge_utils

    user = getpass.getuser()

    wf = Workflow(
        workflow_args="workflow_with_many_retries",
        project="proj_scicomp")

    params = ExecutorParameters(
        num_cores=1,
        m_mem_free="1G",
        max_runtime_seconds=100,  # set max runtime to be shorter than task runtime
        queue="all.q",
        executor_class="SGEExecutor",
        resource_scales={'m_mem_free': 0.5, 'max_runtime_seconds': 0.5})

    name = "retry_task"
    output_file_name = f"/ihme/scratch/users/{user}/retry_output"
    retry_task = PythonTask(
        script=sge_utils.true_path("tests/remote_sleep_and_write.py"),
        args=["--sleep_secs", "4",
              "--output_file_path", output_file_name,
              "--fail_count", 3,
              "--name", name],
        name=name, max_attempts=4, executor_parameters = params)

    wf.add_task(retry_task)

    # 3 TaskInstances will fail before ultimately succeeding
    wf.run()

Dynamically Configure Resources for a Given Task
************************************************
It is possible to dynamically configure the resources needed to run a
given task. For example, if an upstream Task may better inform the resources
that a downstream Task needs, the resources will not be checked and bound until
the downstream is about to run and all of it's upstream dependencies
have completed. To do this, the user can provide a function that will be called
at runtime and return an ExecutorParameter object with the resources needed.


For example ::

    from jobmon.client.api import ExecutorParameters
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow as Workflow
    from jobmon.client.templates.bash_task import BashTask

    def assign_resources(*args, **kwargs):
        """ Callable to be evaluated when the task is ready to be scheduled
        to run"""
        fp = '/ihme/scratch/users/svcscicompci/tests/jobmon/resources.txt'
        with open(fp, "r") as file:
            resources = file.read()
            resource_dict = ast.literal_eval(resources)
        m_mem_free = resource_dict['m_mem_free']
        max_runtime_seconds = int(resource_dict['max_runtime_seconds'])
        num_cores = int(resource_dict['num_cores'])
        queue = resource_dict['queue']

        exec_params = ExecutorParameters(m_mem_free=m_mem_free,
                                         max_runtime_seconds=max_runtime_seconds,
                                         num_cores=num_cores, queue=queue)
        return exec_params

    # task with static resources that assigns the resources for the 2nd task
    # when it runs
    task1 = PythonTask(name='task_to_assign_resources',
                       script="/assign_resources.py", max_attempts = 1,
                       max_runtime_seconds=200, num_cores=1,
                       queue='all.q', m_mem_free='1G')

    task2 = BashTask(name='dynamic_resource_task', command='sleep 1',
                    max_attempts=2, executor_parameters=assign_resources)
    task2.add_upstream(task1) # make task2 dependent on task 1

    wf = Workflow(workflow_args='dynamic_resource_wf')
    wf.add_task(task1)
    wf.run()


A Workflow that adjusts the resources of a Task
===============================================

Sometimes a user may not be able to accurately predict the runtime or memory usage
of a task. Jobmon will detect when the task fails due to resource constraints and
retry that task with with more resources. The default resource scaling factor is 50%
for m_mem_free and max_runtime_sec unless otherwise specified. For example if your
max_runtime for a task was set to 100 seconds and fails, Jobmon will automatically
retry the Task with a max runtime set to 150 seconds.

For example::

    from jobmon.client.templates.unknown_workflow import UnknownWorkflow as Workflow
    from jobmon.client.templates.bash_task import BashTask
    from jobmon.client.api import ExecutorParameters

    my_wf = Workflow(
        workflow_args="resource_starved_workflow",
        project="proj_scicomp")


    # specify SGE specific parameters
    sleepy_params = ExecutorParameters(
        num_cores=1,
        m_mem_free="1G",
        max_runtime_seconds=100,  # set max runtime to be shorter than task runtime
        queue="all.q",
        executor_class="SGEExecutor",
        resource_scales={'m_mem_free': 0.6, 'max_runtime_seconds': 0.6})
    sleepy_task = BashTask(
        # set sleep to be longer than max runtime, forcing a retry
        "sleep 120",
        # job should succeed on second try. The runtime will 160 seconds on the retry
        max_attempts=2,
        executor_parameters=sleepy_params)
    my_wf.add_task(sleepy_task)

    # The Task will time out and get killed by the cluster. After a few minutes Jobmon
    # will notice that it has disappeared and ask SGE for exit status. SGE will
    # show a resource kill. Jobmon will scale the memory and runtime by 60% and retry the
    # job at which point it will succeed.
    my_wf.run()



Resume an Entire Workflow
*************************

A Workflow allows for sophisticated tracking of how many times a DAG gets
executed, who ran them and when.
With a Workflow you can:

#. Re-use a set of Tasks
#. Stop a set of Tasks mid-run and resume it (either intentionally or unfortunately, as
   a result of an adverse cluster event)
#. Re-attempt a set of Tasks that may have ERROR'd out in the middle (assuming you
   identified and fixed the source of the error)
#. Set stderr, stdout, working_dir, and project qsub arguments from the top level

When a workflow is resumed, Jobmon examines  it from the beginning and skips over
any tasks that are already Done. It will restart jobs that were in Error (maybe you fixed
that bug!) or are Registered. As always it only starts a job when all its upstreams are Done.
In other words, it starts from first failure, creating a new workflow run for an existing workflow.

To resume a Workflow, make sure that your previous workflow
run process is dead (kill it using the pid from the workflow run table)::

    import getpass
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow as Workflow

    # Re-instantiate your Workflow with the same WorkflowArgs but add the resume flag
    user = getpass.getuser()
    workflow = Workflow(
        name = "template_workflow",
        description = "template_workflow",
        executor_class = "SGEExecutor",
        stderr = f"/ihme/scratch/users/{user}/{wf_uuid}",
        stdout = f"/ihme/scratch/users/{user}/{wf_uuid}",
        project = "proj_scicomp",
        resume = True
    )

    # Re-add the same Tasks to it...
    task1 = BashTask(
        command = "echo task1",
        executor_class = "SGEExecutor"
    )

    task2 = BashTask(
        command = "echo task2",
        executor_class = "SGEExecutor",
        upstream_tasks = [task1]
    )

    task3 = PythonTask(
        script = os.path.join(script_path, 'test_scripts/test.py'),
        args = ["--args1", "val1", "--args2", "val2"],
        executor_class = "SGEExecutor",
        upstream_tasks = [task2]
    )

    workflow.add_tasks([task1, task2, task3])

    # Re-run the workflow
    workflow.run()

That's it. It is the same setup, just change the resume flag so that it is
true (otherwise you will get an error that you are creating a workflow that
already exists)

For further configuration there are two types of resumes:
    1.Cold Resume: all Tasks are stopped and you are ok with resetting all
    running Tasks and killing any running TaskInstances before restarting
    (the default option).

    2. Hot Resume: any Tasks that are currently running will not be reset, and
    any TaskInstance that are currently running on the cluster will not be killed

Behind the scenes, the Workflow will launch your Tasks as soon as each is
ready to run (i.e. as soon as the Task's upstream dependencies are DONE). It
will automatically restart Tasks that die due to cluster instability or other
intermittent issues. If for some reason, your Workflow itself dies (or you need
to kill it yourself), resuming the script at a later time will automatically pickup
where you left off (i.e. use the '--resume' flag). A resumed run will not
re-run any Tasks that completed successfully in prior runs.

Note carefully the distinction between "restart" and "resume."
Jobmon itself will restart individual Tasks, whereas a human operator can resume the
entire Workflow.

For more examples, take a look at the `resume tests <https://stash.ihme.washington.edu/projects/SCIC/repos/jobmon/browse/tests/workflow/test_workflow_resume.py>`_.

.. note::

    Remember, a Workflow is defined by its WorkflowArgs and its Tasks. If you
    want to resume a previously stopped run, make sure you haven't changed the
    values of WorkflowArgs or added/removed any Tasks to it. If either of these change,
    you will end up creating a brand new Workflow.

.. note::

    Resuming a previously stopped Workflow will create a new
    :term:`WorkflowRun`. This is generally an internal detail that you won't
    need to worry about, but the concept may be helpful in debugging failures.
    (SEE DEBUGGING TODO).

.. todo for the jobmon developers::

    (DEBUGGING) Figure out whether/how we want users to interact with
    WorkflowRuns. I tend to think they're only useful for debugging purposes...
    but that leads to the question of what utilities we want to expose to help
    users to debug in general.

As soon as you change any of the values of your WorkflowArgs or modify its Tasks,
you'll cause a new Workflow entry to be created in the Jobmon
database. When calling run() on this new Workflow, any progress through the
Tasks that may have been made in previous Workflows will be ignored.

.. todo for the jobmon developers::

    Figure out how we want to give users visibility into the Workflows
    they've created over time.


Making a Workflow Fail On First Failure
***************************************

On occasion, a user might want to see how far a workflow can get before it fails,
or want to immediately see where problem spots are. To do this, the user can just
instantiate the workflow with fail_fast set to True. Then add tasks to the workflow
as normal, and the workflow will fail on the first failure.

For example::

    wf = Workflow(workflow_args='testing', fail_fast=True)
    t1 = BashTask("not a command 1")
    t2 = BashTask("sleep 10", upstream_tasks=[t1])
    wf.add_tasks([t1, t2])
    wf.run()


Jobmon Database
***************

If the command line status commands do not provide the information you need,
you can look in the jobmon database.
By default, your Workflow talks to our centrally-hosted Jobmon server
(scicomp-maria-db-p02.db.ihme.washington.edu). You can access the
Jobmon database from your favorite DB browser (e.g. Sequel Pro) using the credentials::

    host: scicomp-maria-db-p02.db.ihme.washington.edu
    port: 3306
    user: read_only
    pass: docker
    database: docker

If you are accessing a version of Jobmon prior to 2.0.0 the database host is
jobmon-docker-cont-p02.hosts.ihme.washington.edu.

.. note::
    Following the 1.1.0 series of Jobmon a persistent database was created. This means any
    time the client side of Jobmon is updated it will continue to use the same database.
    The database credentials will only change when database changes are implemented
    (e.g. Jobmon 2.0.0)

.. todo for the jobmon developers::

    Create READ-ONLY credentials


Running Queries in Jobmon
*************************


You can query the Jobmon database to see the status of a whole Workflow, or any set of tasks.
Open a SQL browser (e.g. Sequel Pro) and connect to the database defined above.

Tables:

arg
    A list of args that the node_args and task_args use
arg_type
    The different types of args (NODE_ARG, TASK_ARG, OP_ARG)
command_template_arg_type_mapping
    A table that associates a TaskTemplate version with arg types.
dag
    Has every entry of dags created, as identified by it's id and hash.
edge
    A table that shows the relationship between two nodes.
executor_parameter_set
    The executor-specific parameters of a given Task, e.g max_runtime_seconds, m_mem_free, num_cores etc.
executor_parameteter_set_type
    The type of parameters (original requested, validated, adjusted).
node
    The object representing a Task within a DAG. Table includes TaskTemplate version and the hash of the node args.
node_arg
    Args that identify a unique node in the DAG.
task
    A single executable object in the workflow. The table includes the name of the task, the command it submitted, and it's executor parameters.
task_arg
    A list of args that make a command unique across different workflows, includes task_id, arg_id and the associated value.
task_attribute
    Additional attributes of the task that can be tracked. For example, release ID or location
    set version ID. Task attributes are not passed to the job but may be useful for profiling
    or resource prediction work in the Jobmon database. Pass in task attributes as a list or
    dictionary to create_task().
task_attribute_type
    Types of task attributes that can be tracked.
task_instance
    This is an actual run of a task. Like calling a function in Python. One Task can have
    multiple task instances if they are retried.
task_instance_error_log
    Any errors that are produced by a task instance are logged in this table.
task_instance_status
    Meta-data table that defines the ten states of Task Instance:

    +-----+---------------------------------+---------------------------------------------------------------------------------+
    |     | Status                          | Description                                                                     |
    +=====+=================================+=================================================================================+
    |  B  |  SUBMITTED_TO_BATCH_EXECUTIONER | Task instance submitted normally.                                               |
    +-----+---------------------------------+---------------------------------------------------------------------------------+
    |  D  |  DONE                           | Task instance finishes normally.                                                |
    +-----+---------------------------------+---------------------------------------------------------------------------------+
    |  E  |  ERROR                          | Task instance has hit an application error.                                     |
    +-----+---------------------------------+---------------------------------------------------------------------------------+
    |  I  |  INSTANTIATED                   | Task instance is created.                                                       |
    +-----+---------------------------------+---------------------------------------------------------------------------------+
    |  R  |  RUNNING                        | Task instance starts running normally.                                          |
    +-----+---------------------------------+---------------------------------------------------------------------------------+
    |  U  |  UNKNOWN                        | Task instance stops reporting that it's alive and Jobmon can't figure out why.  |
    +-----+---------------------------------+---------------------------------------------------------------------------------+
    |  W  |  NO_EXECUTOR_ID                 | Task instance submission has hit a bug and did not receive an executor_id.      |
    +-----+---------------------------------+---------------------------------------------------------------------------------+
    |  Z  |  RESOURCE_ERROR                 | Task instance died because of an insufficient resource request.                 |
    +-----+---------------------------------+---------------------------------------------------------------------------------+
    |  K  |  KILL_SELF                      | Task instance has been ordered to kill itself if it is still alive.             |
    +-----+---------------------------------+---------------------------------------------------------------------------------+

task_status
    Meta-data table that defines the eight states of Task:

    +-----+---------------------------+----------------------------------------------------------------------------------------+
    |     | Status                    | Description                                                                            |
    +=====+===========================+========================================================================================+
    |  A  |  ADJUSTING_RESOURCES      | Task has errored with a resource error, the resources will be adjusted before retrying.|
    +-----+---------------------------+----------------------------------------------------------------------------------------+
    |  D  |  DONE                     | Task ran to completion.                                                                |
    +-----+---------------------------+----------------------------------------------------------------------------------------+
    |  E  |  ERROR_RECOVERABLE        | Task has errored out but has more attempts so it will be retried.                      |
    +-----+---------------------------+----------------------------------------------------------------------------------------+
    |  F  |  ERROR_FATAL              | Task has errored out and has used all of the attempts. It cannot be retried.           |
    +-----+---------------------------+----------------------------------------------------------------------------------------+
    |  G  |  REGISTERED               | Task has been bound to the database.                                                   |
    +-----+---------------------------+----------------------------------------------------------------------------------------+
    |  I  |  INSTANTIATED             | Task has had a Task Instance created that will be submitted to the Executor.           |
    +-----+---------------------------+----------------------------------------------------------------------------------------+
    |  Q  |  QUEUED_FOR_INSTANTIATION | Task's dependencies have been met, task can be run when the scheduler is ready.        |
    +-----+---------------------------+----------------------------------------------------------------------------------------+
    |  R  |  RUNNING                  | Task is running on the specified Executor.                                             |
    +-----+---------------------------+----------------------------------------------------------------------------------------+

task_template
    This table has every TaskTemplate, paired with it's tool_version_id.
task_template_version
    A table listing the different versions a TaskTemplate can have.
tool
    A table that shows the list of Tools that can be associated with your Workflow and TaskTemplates.
tool_version
    A table listing the different versions a Tool has.
workflow
    This table has every Workflow created, along with it’s associated dag_id, and workflow_args
workflow_attribute
    Additional attributes that are being tracked for a given Workflow.
workflow_attribute_type
    The types of attributes that can be tracked for Workflows.
workflow_run
    This table has every run of a workflow, paired with it's workflow, as identified by
    workflow_id.
workflow_run_status
    Meta-data table that defines the ten states of Workflow Run:

    +-----+--------------+--------------------------------------------------------------------------------------------------------+
    |     | Status       | Description                                                                                            |
    +=====+==============+========================================================================================================+
    |  G  |  REGISTERED  | WorkflowRun has been validated.                                                                        |
    +-----+--------------+--------------------------------------------------------------------------------------------------------+
    |  B  |  BOUND       | WorkflowRun has been bound to the database.                                                            |
    +-----+--------------+--------------------------------------------------------------------------------------------------------+
    |  R  |  RUNNING     | WorkflowRun is currently running.                                                                      |
    +-----+--------------+--------------------------------------------------------------------------------------------------------+
    |  D  |  DONE        | WorkflowRun has run to completion.                                                                     |
    +-----+--------------+--------------------------------------------------------------------------------------------------------+
    |  A  |  ABORTED     | WorkflowRun encountered problems while binding so it stopped.                                          |
    +-----+--------------+--------------------------------------------------------------------------------------------------------+
    |  S  |  STOPPED     | WorkflowRun has been stopped, probably due to keyboard interrupt from user.                            |
    +-----+--------------+--------------------------------------------------------------------------------------------------------+
    |  E  |  ERROR       | WorkflowRun has not completed successfully, may have lost contact with services.                       |
    +-----+--------------+--------------------------------------------------------------------------------------------------------+
    |  C  |  COLD RESUME | WorkflowRun was set to resume once all tasks were stopped.                                             |
    +-----+--------------+--------------------------------------------------------------------------------------------------------+
    |  H  |  HOT RESUME  | WorkflowRun was set to resume while tasks are still running, they will continue running.               |
    +-----+--------------+--------------------------------------------------------------------------------------------------------+
    |  T  |  TERMINATED  | WorkflowRun was in resume, new WorkflowRun created to pick up remainingtTasks, so this one terminated. |
    +-----+--------------+--------------------------------------------------------------------------------------------------------+

workflow_status
    Meta-data table that defines eight states of Workflow:

    +-----+--------------+--------------------------------------------------------------------------+
    |     | Status       | Description                                                              |
    +=====+==============+==========================================================================+
    |  G  |  REGISTERED  | Workflow created and validated.                                          |
    +-----+--------------+--------------------------------------------------------------------------+
    |  B  |  BOUND       | Workflow bound to the database.                                          |
    +-----+--------------+--------------------------------------------------------------------------+
    |  A  |  ABORTED     | Workflow encountered an error before a WorkflowRun was created.          |
    +-----+--------------+--------------------------------------------------------------------------+
    |  C  |  CREATED     | Workflow created a WorkflowRun.                                          |
    +-----+--------------+--------------------------------------------------------------------------+
    |  D  |  DONE        | Workflow finished successfully.                                          |
    +-----+--------------+--------------------------------------------------------------------------+
    |  F  |  FAILED      | Workflow unsuccessful in one or more WorkflowRuns, none finished as Done.|
    +-----+--------------+--------------------------------------------------------------------------+
    |  R  |  RUNNING     | Workflow has a WorkflowRun that is running.                              |
    +-----+--------------+--------------------------------------------------------------------------+
    |  S  |  SUSPENDED   | Workflow paused if marked for resume, can be set to running again.       |
    +-----+--------------+--------------------------------------------------------------------------+

You will need to know your workflow_id or dag_id. Hopefully your application
logged it, otherwise it will be obvious by name as one of the recent entries
in the dag table.

Useful Jobmon SQL Queries
**************************
If you wanted the current status of all Tasks in workflow 191:
    | SELECT status, count(*)
    | FROM task
    | WHERE workflow_id=191
    | GROUP BY status

To find your Workflow if you know the Workflow name:
    | SELECT *
    | FROM workflow
    | WHERE name="<your workflow name>"

To find all of your Workflows by your username:
    | SELECT *
    | FROM workflow
    | JOIN workflow_run ON workflow.id = workflow_run.workflow_id
    | WHERE workflow_run.user = "<your username>"

To get all of the error logs associated with a given Workflow:
    | SELECT *
    | FROM task t1, task_instance t2, task_instance_error_log t3
    | WHERE t1.id = t2.task_id
    | AND t2.id = t3.task_instance_id
    | AND t1.workflow_id = <workflow id>

To get the error logs for a given WorkflowRun:
    | SELECT *
    | FROM task_instance t1, task_instance_error_log t2
    | WHERE t1.id = t2.task_instance_id
    | AND t1.workflow_run_id = <workflow_run_id>


Getting Additional Help
************************
The Scientific Computing team is always available to answer your questions or to consult on
Jobmon.

To contact the team via Slack:
    - #jobmon-users to ask questions about Jobmon.
    - #jobmonalerts is an automated messaging channel. Jobmon will notify the channel when a
      workflow failed.

To set up a consultation:
    - Send a message in the #jobmon-users slack channel saying that you would like a
      consultation.
    - A Scientific Computing team member will reach out to you to schedule a consultation
      meeting.

To raise a Scientific Computing help desk request:
    - `SciComp Help Desk <https://help.ihme.washington.edu/servicedesk/customer/portal/16>`_.

When requesting help try to provide the team with as much information as you have about your
problem. *Please include your Workflow id, the Jobmon version that you're using, and any
TaskInstance error logs that you have.*
