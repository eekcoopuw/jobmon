Workflows
#########

The primary means for users to interact with jobmon is by creating a
:term:`TaskDag` and attaching it to a :term:`Workflow`. A TaskDag is so-named
because it represents a set of Tasks which may depend on one another such that
if each relationship were drawn (Task A) -> (Task B depends on Task A), it
would form a
`directed-acyclic graph (DAG) <https://en.wikipedia.org/wiki/Directed_acyclic_graph>`_.
Constructing a TaskDag and adding a few tasks is simple (aspirational)::

    from jobmon.workflow.task_dag_factory import TaskDagFactory
    from jobmon.workflow import BashTask

    my_dag = JobDagFactory.create_task_dag(name="MyTaskTag")

    task_a = BashTask("touch ~/mytestfile")
    task_b = BashTask("ls ~")
    task_b.add_upstream(task_a)

    my_dag.add_tasks([task_a, task_b])


You may want your tasks to take arguments. That might look something like::

    task_c = BashTask("echo $1", args=("hello jobmon",))

Sometimes, you might want to get really fancy and re-use your TaskDag
definition multiple times, with different arguments passed to tasks each time.
This is where the concept of a Workflow comes into play. Workflows allow you to
parameterize your DAGs to reduce re-coding while maintaining flexibility
and resiliency in your workflows. For example, maybe some of the tasks in my
TaskDag take a model_version as an argument. We could add TaskDag to a Workflow
and declare some model_versions as :term:`WorkflowArgs` (interface TBD)::

    from jobmon.workflow import Workflow, WorkflowArgs

    wfa = WorkflowArgs()
    wfa.add_argument("model_ver1", 12345)
    wfa.add_argument("model_ver2", 67890)
    wfa.add_argument("model_ver3", sys.argv[1])

    wf = Workflow(task_dag=my_dag, workflow_args=wfa)

    task_d = BashTask("add_two_mvs $1 $2", args=(wfa.model_ver1,
                                                 wfa.model_ver2))
    task_e = BashTask("add_two_mvs $1 $2", args=(wfa.model_ver2,
                                                 wfa.model_ver3))
    task_e.add_upstream(task_d)
    my_dag.add_tasks([task_d, task_e])

Once you've setup your Workflow, running it is as simple as::

    wf.run()

Behind the scenes, the Workflow will launch your Tasks as soon as they are
ready to run (i.e. as soon as all their upstream dependencies are DONE). It
will restart Tasks that die due to cluster instability or other intermittent
issues. If for some reason, your Workflow itself dies (or you need to pause
it), re-running the script at a later time will automatically pickup where
you left off (assuming you've passed the same values for WorkflowArgs and
haven't modified the TaskDag). It will not re-run those jobs that completed
successfully.

.. note::

    Resuming a previously stopped Workflow will create a new
    :term:`WorkflowRun`. These are generally internal details that you won't
    need to worry about, but the concept may be helpful in debugging failures
    (SEE DEBUGGING TODO).

.. todo::

    Figure out whether/how we want users to interact with WorkflowRuns. I tend
    to think they're only useful for debugging purposes... but that leads to
    the question of what utilities we want to expose to help users to debug in
    general.

As soon as you change any of the values of your Workflow's WorkflowArgs, or
modify its TaskDag, you'll cause a new Workflow entry to be created in the
jobmon database. When calling run() on this new Workflow, any progress through
the TaskDag that may have been made in previous Workflows will be ignored.

.. todo::

    Figure out how we want to give users visibility into the Workflows
    they've created over time.



(Emu Design Notes) Workflows, TaskDags, and WorkflowRuns
********************************************************

Users should create a Workflow. Upon creation, it takes a WorkflowArgs and
TaskDag. In the simple case, WorkflowArgs could be None, though this would
limit the amount of intelligence that could be provided around pause/resume
(i.e. WorkflowRun generation).  Tasks can be added to the TaskDag and their
upstream/downstream connections to other Tasks can be specified. After
specifying the shape of the Dag, the user should call Workflow.execute(). At
this point, the TaskDag + WorkflowArgs are frozen (i.e. hashed + locked) so
that they can be used to search for previous WorkflowRuns in the case of a
pause/resume cycle.

If the WorkflowArgs + TaskDag that define a Workflow already point to an
existing Workflow that is incomplete when the user calls "execute()," the user
must decide (via interactive prompt) (TBD... we could potentially do this
intelligently) whether to:

- Resume the Workflow by creating a new WorkflowRun
- To create a brand new Workflow (in this case, we'll have to mark the previous
  Workflow as inactive or something... otherwise a future resume would have
  multiple Workflows to choose from)

When a WorkflowRun is created (which Geoff's Dag processing algorithm will
crawl and make appropriate calls to JobListManager), the "counters" any Jobs
associated with the Workflow that are in a FAILED state will be reset to
INSTANTIATED and their "attempts" counter will be reset to 0. After this
resetting process, any Jobs not in COMPLETE state will be associated to the new
WorkflowRun.

If the WorkflowArgs + TaskDag that define a Workflow already point to an
existing Workflow that is complete when the user calls "execute()," the user
must force creation of a Workflow (via interactive prompt). Otherwise, they
would be expected to be passing new WorkflowArgs or to modify 1 or more Tasks
in the TaskDag.
