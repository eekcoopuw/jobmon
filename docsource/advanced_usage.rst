**************
Advanced Usage
**************

Retries
#######

Ordinary
********
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



Resource
********
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




.. _jobmon-resume-label:

Resumes
#######

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
        project = "proj_scicomp"
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
    workflow.run(resume=True)

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

As soon as you change any of the values of your WorkflowArgs or modify its Tasks,
you'll cause a new Workflow entry to be created in the Jobmon
database. When calling run() on this new Workflow, any progress through the
Tasks that may have been made in previous Workflows will be ignored.


Hot Resume
**********
TODO: SPLIT RESUME IN TO THESE COMPONENTS

Cold Resume
***********
TODO: SPLIT RESUME IN TO THESE COMPONENTS

Fail Fast
#########
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


Dynamic Task Resources
######################
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

Advanced Task Dependencies
##########################
For this example, we'll use a slightly simplified version of the Burdenator which has five
"phases": most-detailed, pct-change, loc-agg, cleanup, and upload. To reduce runtime,
we want to link up each job only to the previous jobs that it requires, not to every job
in that phase. The parallelization strategies for each phase are a little different,
complicating the dependency scheme.

1. Most-detailed jobs are parallelized by location, year;
2. Loc-agg jobs are parallelized by measure, year, rei, and sex;
3. Cleanup jobs are parallelized by location, measure, year
4. Pct-change jobs are parallelized by location_id, measure, start_year, end_year; For most-detailed locations, this can run immediately after the most-detailed phase. But for aggregate locations, this has to be run after both loc-agg and cleanup
5. Upload jobs are parallelized by measure

To begin, we create an empty dictionary for each phase and when we build each task, we add the
task to its dictionary. Then the task in the following phase can find its upstream task using
the upstream dictionary. The only dictionary not needed is one for the upload jobs, since no
downstream tasks depend on these jobs.

.. code::

    # python 3
    import sys
    from jobmon.client.tool import Tool
    from jobmon.client.task_template import TaskTemplate
    from jobmon.client.execution.strategies.base import ExecutorParameters

    from my_app.utils import split_locs_by_loc_set

    class NatorJobSwarm(object):
        def __init__(self, year_ids, start_years, end_years, location_set_id,
                     measure_ids, rei_ids, sex_ids, version):
            self.year_ids = year_ids
            self.start_year_ids = start_years
            self.end_year_ids = end_years
            self.most_detailed_location_ids, self.aggregate_location_ids, \
                self.all_location_ids = split_locs_by_loc_set(location_set_id)
            self.measure_ids = measure_ids
            self.rei_ids = rei_ids
            self.sex_ids = sex_ids
            self.version = version

            self.tool = Tool(name="Burdenator")
            self.most_detailed_jobs_by_command = {}
            self.pct_change_jobs_by_command = {}
            self.loc_agg_jobs_by_command = {}
            self.cleanup_jobs_by_command = {}

            self.python = sys.executable

        def create_workflow(self):
            """ Instantiate the workflow """

            self.workflow = self.tool.create_workflow(
                workflow_args = f'burdenator_v{self.version}',
                name = f'burdenator run {self.version}'
            )

        def create_task_templates(self):
            """ Create the task template metadata objects """

            self.most_detailed_tt = self.tool.get_task_template(
                template_name = "run_burdenator_most_detailed",
                command_template = "{python} {script} --location_id {location_id} --year {year}",
                node_args = ["location_id", "year"],
                op_args = ["python", "script"])

            self.loc_agg_tt = self.tool.get_task_template(
                template_name = "location_aggregation",
                command_template = "{python} {script} --measure {measure} --year {year} --sex {sex} --rei {rei}",
                node_args = ["measure", "year", "sex", "rei"],
                op_args = ["python", "script"])

            self.cleanup_jobs_tt = self.tool.get_task_template(
                template_name = "cleanup_jobs",
                command_template = "{python} {script} --measure {measure} --loc {loc} --year {year}",
                node_args = ["measure", "loc", "year"],
                op_args = ["python", "script"])

            self.pct_change_tt = self.tool.get_task_template(
                template_name = "pct_change",
                command_template = ("{python} {script} --measure {measure} --loc {loc} --start_year {start_year}"
                                    " --end_year {end_year}"),
                node_args = ["measure", "loc", "start_year", "end_year"],
                op_args = ["python", "script"])

            self.upload_tt = self.tool.get_task_template(
                template_name = "upload_jobs",
                command_template = "{python} {script} --measure {measure}"
                node_args = ["measure"],
                op_args = ["python", "script"])


        def create_most_detailed_jobs(self):
            """First set of tasks, thus no upstream tasks"""

            executor_parameters = ExecutorParameters(
                num_cores=40,
                m_mem_free="20G",
                max_attempts=5,
                max_runtime_seconds=360
            )

            for loc in self.most_detailed_location_ids:
                for year in self.year_ids:
                    task = self.most_detailed_tt.create_task(
                                      executor_parameters=executor_parameters,
                                      name='most_detailed_{}_{}'.format(loc, year),
                                      python=self.python,
                                      script='run_burdenator_most_detailed',
                                      loc=loc,
                                      year=year)
                    self.workflow.add_task(task)
                    self.most_detailed_jobs_by_command[task.name] = task

        def create_loc_agg_jobs(self):
            """Depends on most detailed jobs"""

            executor_parameters = ExecutorParameters(
                num_cores=20,
                m_mem_free="40G",
                max_attempts=11,
                max_runtime_seconds=540
            )

            for year in self.year_ids:
                for sex in self.sex_ids:
                    for measure in self.measure_ids:
                        for rei in self.rei_ids:
                            task = self.loc_agg_tt.create_task(
                                executor_parameters=executor_parameters,
                                name='loc_agg_{}_{}_{}_{}'.format(measure, year, sex, rei),
                                python=self.python,
                                script='run_loc_agg',
                                measure=measure,
                                year=year,
                                sex=sex,
                                rei=rei)

                            for loc in self.most_detailed_location_ids:
                                task.add_upstream(
                                    self.most_detailed_jobs_by_command['most_detailed_{}_{}'
                                                                       .format(loc, year)])
                            self.workflow.add_task(task)
                            self.loc_agg_jobs_by_command[task.name] = task

        def create_cleanup_jobs(self):
            """Depends on aggregate locations coming out of loc agg jobs"""

            executor_parameters = ExecutorParameters(
                num_cores=25,
                m_mem_free="50G",
                max_attempts=11,
                max_runtime_seconds=360
            )

            for measure in self.measure_ids:
                for loc in self.aggregate_location_ids:
                    for year in self.year_ids:
                        task = self.cleanup_jobs_tt.create_task(
                                          executor_parameters=executor_parameters,
                                          name='cleanup_{}_{}_{}'.format(measure, loc, year),
                                          python=self.python,
                                          script='run_cleanup',
                                          measure=measure,
                                          loc=loc,
                                          year=year)

                        for sex in self.sex_ids:
                            for rei in self.rei_ids:
                                task.add_upstream(
                                    self.loc_agg_jobs_by_command['loc_agg_{}_{}_{}_{}'
                                                                 .format(measure, year,
                                                                         sex, rei)])
                        self.workflow.add_task
                        self.cleanup_jobs_by_command[task.name] = task

        def create_pct_change_jobs(self):
            """For aggregate locations, depends on cleanup jobs.
            But for most_detailed locations, depends only on most_detailed jobs"""

            executor_parameters = ExecutorParameters(
                num_cores=45,
                m_mem_free="90G",
                max_attempts=11,
                max_runtime_seconds=540
            )

            for measure in self.measure_ids:
                for start_year, end_year in zip(self.start_year_ids, self.end_year_ids):
                    for loc in self.location_ids:
                        if loc in self.aggregate_location_ids:
                            is_aggregate = True
                        else:
                            is_aggregate = False
                        task = self.pct_change_tt.create_task(
                                          executor_parameters=executor_parameters,
                                          name=('pct_change_{}_{}_{}_{}'
                                                .format(measure, loc, start_year, end_year),
                                          python=self.python,
                                          script='run_pct_change',
                                          measure=measure,
                                          loc=loc,
                                          start_year=start_year,
                                          end_year=end_year)

                        for year in [start_year, end_year]:
                            if is_aggregate:
                                task.add_upstream(
                                    self.cleanup_jobs_by_command['cleanup_{}_{}_{}'
                                                                 .format(measure, loc, year)]
                            else:
                                task.add_upstream(
                                    self.most_detailed_jobs_by_command['most_detailed_{}_{}'
                                                                       .format(loc, year)])
                        self.workflow.add_task(task)
                        self.pct_change_jobs_by_command[task.name] = task

        def create_upload_jobs(self):
            """Depends on pct-change jobs"""

            executor_parameters = ExecutorParameters(
                num_cores=20,
                m_mem_free="40G",
                max_attempts=3,
                max_runtime_seconds=720
            )

            for measure in self.measure_ids:
                task = self.upload_tt.create_task(
                                  executor_parameters=executor_parameters,
                                  name='upload_{}'.format(measure)
                                  script='run_pct_change',
                                  measure=measure)

                for location_id in self.all_location_ids:
                    for start_year, end_year in zip(self.start_year_ids, self.end_year_ids):
                        task.add_upstream(
                            self.pct_change_jobs_by_command['pct_change_{}_{}_{}_{}'
                                                            .format(measure, location,
                                                                    start_year, end_year])
                self.workflow.add_task(task)

        def run():
            success = self.workflow.run()
            if success:
                print("You win at life")
            else:
                print("Failure")


Jobmon Self-Service Commands
############################
Jobmon has a suite of commands to not only visualize task statuses from the database, but to
allow the users to modify the states of their workflows. These self-service commands can be
invoked from the command line in the same way as the status commands, see :ref:`status-commands-label`.

concurrency_limit
*****************
Entering ``jobmon concurrency_limit`` will allow the user to change the maximum running task
instances allowed in their workflow. When a workflow is instantiated, the user can specify a
maximum limit to the number of concurrent tasks in case a very wide workflow threatens to
resource-throttle the cluster. While running, the user can use this command to change the
maximum allowed concurrency as needed if cluster busyness starts to wax or wane.

workflow_reset
**************
TODO: FILL OUT THIS SECTION
https://stash.ihme.washington.edu/projects/SCIC/repos/jobmon/pull-requests/356/overview

update_task_status
******************
    Entering ``jobmon update_task_status`` allows the user to set the status of tasks in their
    workflow. This is helpful for either rerunning portions of a workflow that have already
    completed, or allowing a workflow to progress past a blocking error. The usage is
    ``jobmon update_task_status -t [task_ids] -w [workflow_id] -s [status]``

    There are 2 allowed statuses: "D" - DONE and "G" - REGISTERED.

    Specifying status "D" will mark only the listed task_ids as "D", and leave the rest of the
    DAG unchanged. When the workflow is resumed, the DAG executes as if the listed task_ids
    have finished successfully.

    If status "G" is specified, the listed task IDs will be set to "G" as well as all
    downstream dependents of those tasks. TaskInstances will be set to "K". When the workflow
    is resumed, the specified tasks will be rerun and subsequently their downstream tasks as
    well. If the workflow has successfully completed, and is marked with status "D", the
    workflow status will be amended to status "E" in order to allow a resume.

    .. note::
        1. All status changes are propagated to the database.
        2. Only inactive workflows can have task statuses updated
        3. The updating user must have at least 1 workflow run associated with the requested workflow.
        4. The requested tasks must all belong to the specified workflow ID

TaskTemplate Resource Prediction to YAML
****************************************
    Entering ``jobmon task_template_resources`` will allow users to generate a task template
    compute resources YAML file that can be used in Jobmon 3.0 and later.

    As an example, ``jobmon task_template_resources -w 1 -p f ~/temp/resource.yaml`` generates
    a YAML file for all task templates used in workflow 1 and saves it to ~/temp/resource.yaml.
    It will also print the generated compute resources to standard out.

    An example output:

    .. code-block:: yaml

       your_task_template_1:
            slurm:
              cores: 1
              memory: "400B"
              runtime: 10
              queue: "all.q"
            buster:
              num_cores: 1
              m_mem_free: "400B"
              max_runtime_seconds: 10
              queue: "all.q"
        your_task_template_2:
            slurm:
              cores: 1
              memory: "600B"
              runtime: 20
              queue: "long.q"
            buster:
              num_cores: 1
              m_mem_free: "600B"
              max_runtime_seconds: 20
              queue: "long.q"

Resource Usage
##############
Task Resource Usage
*******************
    There is a method on the Task object that will return the resource usage for a Task. This
    method must be called after ``workflow.run()``. To use it simply call the method on your
    predefined Task object, ``task.resource_usage()``. This method will return a dictionary
    that includes: the memory usage (in bytes), the name of the node the task was run on, the
    number of attempts, and the runtime. This method will only return resource usage data for
    Tasks that had a successful TaskInstance (in DONE state).

TaskTemplate Resource Usage
***************************
    Jobmon can aggregate the resource usage at the TaskTemplate level. Jobmon will return a
    dictionary that includes: number of Tasks used to calculate the usage, the minimum,
    maximum, and mean memory used (in bytes), and the minimum, maximum and mean runtime. It
    only includes Tasks in the calculation that are associated with a specified
    TaskTemplateVersion.

    You can access this in two ways: via a method on TaskTemplate or the Jobmon command line
    interface.

    To access it via the TaskTemplate object, simply call the method on your predefined
    TaskTemplate, ``task_template.resource_usage()``. This method has two *optional*
    arguments: workflows (a list of workflow IDs) and node_args (a dictionary of node
    arguments). This allows users to have more exact resource usage data. For example, a
    user can call ``resources = task_template.resource_usage(workflows=[123, 456],
    node_args={"location_id":[101, 102], "sex":[1]})`` This command will find all of the
    Tasks associated with that version of the TaskTemplate, that are associated with either
    workflow 123 or 456, that also has a location_id that is either 102 or 102, and has a
    sex ID of 1. Jobmon will then calculate the resource usage values based on those queried
    Tasks.

    To use this functionality via the CLI, call ``jobmon task_template_resources -t
    <task_template_version_id>`` The CLI has two optional flags: -w to specify workflow IDs
    and -a to query by specific node_args. For example, ``jobmon task_template_resources -t
    12 -w 101 102 -a '{"location_id":[101,102], "sex":[1]}'``.

Logging
#######
To attach Jobmon's simple formatted logger use the following code.

For example::

    from jobmon.client.client_logging import ClientLogging

    ClientLogging().attach()


