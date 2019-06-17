Design Notes
############


(Emu) Workflows
***************

Users should create a Workflow. Upon creation, it takes a WorkflowArg. In the simple case, WorkflowArgs could be None, though this would limit the amount of intelligence
that could be provided around pause/resume (i.e. WorkflowRun generation).
Tasks can be added to the Workflow and their upstream/downstream connections to other Tasks can be specified. After specifying the shape of the Workflow, the user should call Workflow.execute(). At this point, the Task dependencies + WorkflowArgs are frozen (i.e. hashed + locked) so that they can be used to search for previous WorkflowRuns in the case of a pause/resume cycle.

If the WorkflowArgs + Tasks and dependencies that define a Workflow already point to an
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

If the WorkflowArgs + Tasks that define a Workflow already point to an
existing Workflow that is complete when the user calls "execute()," the user
must force creation of a Workflow (via interactive prompt). Otherwise, they
would be expected to be passing new WorkflowArgs or to modify 1 or more Tasks.

Resource Requesting and Retries
*******************************

With the move to the fair (buster) cluster, resource limits are enforced,
and jobs may die due to cluster enforcement if they have underrequested
resources. In order to help jobs complete without user intervention every time,
jobmon now has resource adjustment. If it detects that a job has died due to
resource enforcing, the resources will be increased and the job will be retried
if it has not exceeded the maximum attempts.

A record of the resources requested can be found in the ExecutorParameters
table where each job will have the original parameters requested and the
validated resources as well as rows added each time a resource error occurs
and the resources need to be increased. If this happens, the user should
reconfigure their job to use the resources that ultimately succeeded so that
they do not waste cluster resources in the future.

A step-by-step breakdown of how jobmon deals with a job instance failing due
to resource enforcement is as follows:

1. job instance exits with a resource killed error code (in state 'Z')
2. if the job has no more retries it will move into failed state, if not it
   will move into error recoverable
3. the reconciler will take jobs that are in error recoverable, and marked
   with a resource error and adjust them by the adjustment factor (right now it
   adjusts all resources by the same factor, but in the future we hope to do
   resource specific scaling)
4. a new job instance will be created, and it will now refer to the new
   adjusted resource values