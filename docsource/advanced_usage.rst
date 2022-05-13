**************
Advanced Usage
**************

Arrays
######
As of the 3.1.0 release, all jobs launched on the Slurm cluster will be launched as arrays.
The effect is that Jobmon uses one sbatch command to launch all the jobs in one TaskTemplate,
rather than one sbatch command to launch a single job. This allows Jobmon to launch jobs
faster. In a comparison load test, 3.0.5 took 1045.10 seconds (17.418 minutes) to submit
10,000 tasks to the cluster, while 3.1.0 took 32.10 seconds to submit the same 10,000 tasks
to the cluster.

Usage
*****
.. code-tabs::

    .. code-tab:: python
      :title: Python

        example_task_template = tool.get_task_template(
            template_name="my_example_task_template",
            command_template="python model_script.py --loc_id {location_id}",
            node_args=["location_id"],
            default_cluster_name="slurm",
            default_compute_resources={"queue": "all.q"},
        )

        example_tasks = example_task_template.create_tasks(
            location_id=[1, 2, 3],
        )

        workflow = tool.create_workflow()
        workflow.add_tasks(example_tasks)
        workflow.run()

    .. code-tab:: R
      :title: R

        library(jobmonr)

        example_tool <- jobmonr::tool("example_project")

        example_task_template <- jobmonr::task_template(
            template_name="my_example_task_template",
            command_template="python model_script.py --loc_id {location_id}",
            node_args=c("location_id")
        )

        workflow <- jobmonr::workflow(example_tool)

        example_tasks <- jobmonr::array_tasks(task_template=example_task_template, location_id=1:3)

        jobmonr::add_tasks(workflow, example_tasks)

        status <- jobmonr::run(workflow)

Array Inference
***************
As of Jobmon 3.1.0 all tasks are launched using Slurm arrays (even tasks that were created using
create_task() instead of create_array()). Tasks that share the same task_template and
compute_resources will be grouped into arrays during workflow.run(). Therefore, will launch
more quickly without users needing to make any code changes.

This means that if a user has tasks that execute in different phases within a workflow the
tasks should belong to different task_templates.

Slurm Job Arrays
****************
For more info about job arrays on a Slurm cluster, see here: https://slurm.schedmd.com/job_array.html

Retries
#######

Ordinary
********
By default a Task will be retried up to three times if it fails. This helps to
reduce the chance that random events on the cluster or landing on a bad node
will cause a user's entire Task and Workflow to fail. If a TaskInstance fails, then Jobmon will
run an exact copy as long as the max number of attempts hasn't be reached. The new TaskInstance
will be created with the same resources and configurations as the first TaskInstance.

In order to configure the number of times a Task can be retried, configure the
max_attempts parameter in the Task that you create. If you are still debugging
your code, please set the number of retries to zero so that it does not retry
code with a bug multiple times. When the code is debugged, and you are ready
to run in production, set the retries to a non-zero value.

The following example shows a configuration in which the user wants their Task
to be retried 4 times and it will fail up until the fourth time.::

    import getpass
    from jobmon.client.tool import Tool

    user = getpass.getuser()

    tool = Tool(name=ordinary_resume_tutorial)

    wf = tool.create_workflow(name=ordinary_resume_wf, workflow_args="wf_with_many_retries")

    retry_tt = tool.get_task_template(
        template_name="retry_tutorial_template",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[]
    )
    output_file_name = f"/ihme/scratch/users/{user}/retry_output"
    this_file = os.path.dirname(__file__)
    remote_sleep_and_write = os.path.abspath(
        os.path.expanduser(f"{this_file}/../tests/_scripts/remote_sleep_and_write.py")
    )
    retry_task = retry_tt.create_task(
        name="retry_task",
        max_attempts=4,
        compute_resources={
            'cores': 1,
            'runtime': '100s',
            'memory': '1Gb',
            'queue': 'all.q',
            'project': 'proj_scicomp',
        },
        cluster_name="slurm",
        arg=f"python {remote_sleep_and_write} --sleep_secs 4 --output_file_path {output_file_name} --name "retry_task" --fail-count 3"

    )

    wf.add_task(retry_task)

    # 3 TaskInstances will fail before ultimately succeeding
    workflow_run_status = wf.run()



Resource
********
Sometimes a user may not be able to accurately predict the runtime or memory usage
of a task. Jobmon will detect when the task fails due to resource constraints and
retry that task with with more resources. The default resource
scaling factor is 50% for memory and runtime unless otherwise specified. For example if your
runtime for a task was set to 100 seconds and fails, Jobmon will automatically
retry the Task with a max runtime set to 150 seconds. Users can specify how they percentage
they would like runtime and memory to scale by.

For example::

    import getpass
    from jobmon.client.tool import Tool

    # The Task will time out and get killed by the cluster. After a few minutes Jobmon
    # will notice that it has disappeared and ask Slurm for an exit status. Slurm will
    # show a resource kill. Jobmon will scale the memory and runtime by the default 50% and
    # retry the job at which point it will succeed.

    user = getpass.getuser()

    tool = Tool(name=resource_resume_tutorial)

    wf = tool.create_workflow(name=resource_resume_wf, workflow_args="wf_with_resource_retries")

    retry_tt = tool.get_task_template(
        template_name="resource_retry_tutorial_template",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[]
    )

    retry_task = retry_tt.create_task(
                        arg="sleep 110"
                        name="retry_task",
                        # job should succeed on second try. The runtime will 135 seconds on the retry
                        max_attempts=2,
                        compute_resources={
                            'cores': 1,
                            'runtime': '90s',
                            'memory': '1Gb',
                            'queue': 'all.q',
                            'project': 'proj_scicomp'},
                        cluster_name="slurm"
                    )

    wf.add_task(retry_task)

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
run process is dead (kill it using the pid from the workflow run table). Users for the
most part will keep the same Jobmon code, only one line of code needs to change to resume. A
user simply needs to add a resume parameter to the run() function to resume their Workflow.::

    workflow.run(resume=True)

That's it. If you don't set "resume=True", Jobmon will raise an error saying that the user is
trying to create a Workflow that already exists.

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

For further configuration there are two types of resumes:
Cold Resume
***********
All Tasks are stopped and you are ok with resetting all running Tasks and killing any running
TaskInstances before restarting (the default option).

Hot Resume
**********
Any Tasks that are currently running will not be reset, and
any TaskInstance that are currently running on the cluster will not be killed

Fail Fast
#########
On occasion, a user might want to see how far a workflow can get before it fails,
or want to immediately see where problem spots are. To do this, the user can just
instantiate the workflow with fail_fast set to True. Then add tasks to the workflow
as normal, and the workflow will fail on the first failure. The Workflow will **not** fail fast
if a Task fails because of a resource error (e.g. over runtime or over memory).

For example::

    workflow = tool.create_workflow(name="test_fail_fast", workflow_args="testing")
    task = task_template.create_task(name="fail_fast_task",
                                     compute_resources={runtime: "100s"},
                                     arg="sleep 1")
    workflow.add_tasks([task])

    # This line makes the workflow fail fast
    wfr_status = workflow.run(fail_fast=True)


Fallback Queues
###############
Users are able to specify fallback queues in Jobmon. Scenario: a user has a Task that fails due
to a resource error, Jobmon then scales that Tasks resources, but the newly scaled resources
exceed the resources of the queue the Task is on. In this scenario the user could have
specified a fallback queue(s), if this was specified Jobmon would run the Task with scaled
resources to the next specified queue. If a user does not specify a fallback queue, the
resources will only scale to the maximum values of their originally specified queue.

To set fallback queues, simply pass a list of queues to the  create_task() method. For example::

    # In this example Jobmon will run the Task on all.q. Hypothetically, if it scaled the resources
    # past the all.q limits, it would then try to run the Task on long.q. If that also failed,
    # it would then try to run the Task on i.q.

    workflow = tool.create_workflow(name="test_fallback_queue", workflow_args="fallback")
    fallback_task = fallback_tt.create_task(
                        arg="sleep 110"
                        name="fallback_task",
                        compute_resources={
                            'cores': 1,
                            'runtime': '90s',
                            'memory': '1Gb',
                            'queue': 'all.q',
                            'project': 'proj_scicomp'},
                        cluster_name="slurm",
                        fallback_queues=["long.q", "i.q"]
                    )
    workflow.add_tasks([task])

    # This line makes the workflow fail fast
    wfr_status = workflow.run(fail_fast=True)

Dynamic Task Resources
######################
It is possible to dynamically configure the resources needed to run a
given task. For example, if an upstream Task may better inform the resources
that a downstream Task needs, the resources will not be checked and bound until
the downstream is about to run and all of it's upstream dependencies
have completed. To do this, the user can provide a function that will be called
at runtime and return a ComputeResources object with the resources needed.

For example ::

    import sys
    from jobmon.client.tool import Tool

    def assign_resources(*args, **kwargs):
        """ Callable to be evaluated when the task is ready to be scheduled
        to run"""
        fp = '/ihme/scratch/users/svcscicompci/tests/jobmon/resources.txt'
        with open(fp, "r") as file:
            resources = file.read()
            resource_dict = ast.literal_eval(resources)
        memory = resource_dict['memory']
        runtime = int(resource_dict['runtime'])
        cores = int(resource_dict['cores'])
        queue = resource_dict['queue']

        compute_resources = {"memory": memory, "runtime": runtime, "cores": cores,
                            "queue": queue}
        return compute_resources

    tool = Tool(name="dynamic_tool")

    dynamic_tt = tool.get_task_template(
                template_name="random_template",
                command_template="{python} {script}",
                node_args=[],
                task_args=[],
                op_args=["python", "script"],
                default_cluster_name='slurm')

    # task with static resources that assigns the resources for the 2nd task
    # when it runs
    workflow = tool.create_workflow(name="dynamic_tasks", workflow_args="dynamic")
    task1 = dynamic_tt.create_task(
                        name="task_to_assign_resources",
                        python=sys.executable,
                        script="/assign_resources.py"
                        compute_resources={
                            'cores': 1,
                            'runtime': '200s',
                            'memory': '1Gb',
                            'queue': 'all.q',
                            'project': 'proj_scicomp'},
                        max_attempts=1
                        cluster_name="slurm"
                    )
    # tt is a simple task template that makes arg the command
    task2 = tt.create_task(
                name="dynamic_resource_task",
                arg="echo hello",
                max_attempts=2,
                compute_resouces=assign_resources
            )
    task2.add_upstream(task1) # make task2 dependent on task 1

    wf.add_task(task1, task2)
    wfr_status = wf.run()

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

            for loc in self.most_detailed_location_ids:
                for year in self.year_ids:
                    task = self.most_detailed_tt.create_task(
                                      compute_resources={"cores": 40, "memory": "20Gb", "runtime": "360s"},
                                      cluster_name="slurm",
                                      max_attempts=5,
                                      name='most_detailed_{}_{}'.format(loc, year),
                                      python=self.python,
                                      script='run_burdenator_most_detailed',
                                      loc=loc,
                                      year=year)
                    self.workflow.add_task(task)
                    self.most_detailed_jobs_by_command[task.name] = task

        def create_loc_agg_jobs(self):
            """Depends on most detailed jobs"""

            for year in self.year_ids:
                for sex in self.sex_ids:
                    for measure in self.measure_ids:
                        for rei in self.rei_ids:
                            task = self.loc_agg_tt.create_task(
                                compute_resources={"cores": 20, "memory": "40Gb", "runtime": "540s"},
                                cluster_name="slurm,
                                max_attempts=11,
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

            for measure in self.measure_ids:
                for loc in self.aggregate_location_ids:
                    for year in self.year_ids:
                        task = self.cleanup_jobs_tt.create_task(
                                          compute_resources={"cores": 25, "memory": "50Gb", "runtime": "360s"},
                                          cluster_name="slurm",
                                          max_attempts=11,
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

            for measure in self.measure_ids:
                for start_year, end_year in zip(self.start_year_ids, self.end_year_ids):
                    for loc in self.location_ids:
                        if loc in self.aggregate_location_ids:
                            is_aggregate = True
                        else:
                            is_aggregate = False
                        task = self.pct_change_tt.create_task(
                                          compute_resources={"cores": 45, "memory": "90Gb", "runtime": "540s"},
                                          cluster_name="slurm",
                                          max_attempts=11,
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

            for measure in self.measure_ids:
                task = self.upload_tt.create_task(
                                  compute_resources={"cores": 20, "memory": "40Gb", "runtime": "720s"},
                                  cluster_name="slurm",
                                  max_attempts=3,
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


Concurrency Limiting
####################
Users can set the maximum number of tasks per workflow that are running at one time.
The value can be set statically (in the Jobmon code), or dynamically via the Jobmon CLI.
One of the main use cases for concurrency limit is if an user needs to "throttle down" a
workflow to make space on the cluster without killing their workflow. By default, Jobmon sets
the limit to 10,000 tasks.

To statically set concurrency limit, simply set the ``max_concurrently_running`` flag on the
``create_workflow()`` method.

.. code-block:: python

  tool = Tool(name="example_tool")
  workflow = tool.create_workflow(
      name=f"template_workflow",
      max_concurrently_running=2000
  )

To dynamically set the concurrency limit, see :ref:`concurrency-limit-label`.

Jobmon Self-Service Commands
############################
Jobmon has a suite of commands to not only visualize task statuses from the database, but to
allow the users to modify the states of their workflows. These self-service commands can be
invoked from the command line in the same way as the status commands, see :ref:`status-commands-label`.

.. _concurrency-limit-label:

concurrency_limit
*****************
Entering ``jobmon concurrency_limit`` will allow the user to change the maximum running task
instances allowed in their workflow. When a workflow is instantiated, the user can specify a
maximum limit to the number of concurrent tasks in case a very wide workflow threatens to
resource-throttle the cluster. While running, the user can use this command to change the
maximum allowed concurrency as needed if cluster busyness starts to wax or wane.

workflow_reset
**************
Entering ``jobmon workflow_reset`` will reset a Workflow to G state (REGISTERED). When a
Workflow is reset, all of the Tasks associated with the Workflow will also be transitioned to
G state. The usage of this command is ``jobmon workflow_reset -w [workflow_id]``.

To use this command the last WorkflowRun of the specified Workflow must be in E (ERROR) state.
The last WorkflowRun must also have been started by the same user that is attempting to reset
the Workflow.

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

Error Logs
##########
    There is a method on the Workflow object called ``get_errors`` that will return all of the
    task instance error logs associated with a Workflow. To use it simply call the method on
    your predefined Workflow object: ``workflow.get_errors()``. This method will return a
    dictionary; the key will be the ID of the task and the key will be the error message.
    By default this method will return the last 1,000 error messages. Users can specify the
    limit by utilizing the parameter ``limit``. For example if a user wanted to only see the
    errors for the ten most recent tasks they would call ``workflow.get_errors(limit=10)``.

    .. note::
        To see the error log for a specific task users can call the ``task_status`` CLI
        command. For more information see :ref:`task_status-commands-label`.

Logging
#######
To attach Jobmon's simple formatted logger use the following code.

For example::

    from jobmon.client.client_logging import ClientLogging

    ClientLogging().attach()


