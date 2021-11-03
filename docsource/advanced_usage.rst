**************
Advanced Usage
**************

Retries
#######

Ordinary
********

Resource
********

Resumes
#######

Hot Resume
**********

Cold Resume
***********

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
            ihme_slurm:
              cores: 1
              memory: "400B"
              runtime: 10
              queue: "all.q"
            ihme_uge:
              num_cores: 1
              m_mem_free: "400B"
              max_runtime_seconds: 10
              queue: "all.q"
        your_task_template_2:
            ihme_slurm:
              cores: 1
              memory: "600B"
              runtime: 20
              queue: "long.q"
            ihme_uge:
              num_cores: 1
              m_mem_free: "600B"
              max_runtime_seconds: 20
              queue: "long.q"
