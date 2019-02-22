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
