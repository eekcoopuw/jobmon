Glossary of terms
#################

For users
*********

.. glossary::

    Workflow
        *(aka Batch, aka Swarm)*
        Has a TaskDag and a set of WorkflowArgs.  Optionally, a subset of
        WorkflowArgs may be passed to TaskDag and inform the shape of the Dag
        itself. A Workflow can only be re-loaded if the TaskDag and
        WorkflowArgs are shown to be exact matches to a previous Workflow (have
        to work out how to hash+compare).

        .. todo::
            Work out how to hash+compare TaskDag+WorkflowArgs so we can
            differentiate one Workflow from another and determine when a new
            Workflow is required vs. simply a new WorkflowRun

        .. todo::
            Consider capturing the pip freeze or other environmental markers as
            part of the Workflow hash?)

    WorkflowArgs
        A set of arguments that are used to deteremine the "uniqueness" of the
        Workflow and whether it can be resumed. Must be hashable.  For example,
        CodCorrect or Como version might be passed as Args to the Workflow.
        Coupled with a populated TaskDag (which must be hashable upon
        execution....  somehow) they define a Workflow.

    WorkflowRun
        A Workflow may be started/paused/ and resumed multiple times.  Each
        start or resume represents a new WorkflowRun. In order for a Workflow
        can be deemed to be complete (successfully), it must have 1 or more
        WorkflowRuns. A WorkflowJob may belong to one or more WorkflowRuns, but
        once the Job reaches a COMPLETE state, it may longer be added to a
        subsequent WorkflowRun.

    TaskDag
        A set of Tasks. Upon calling "execute()," it is frozen (i.e.  becomes
        immutable for the lifetime of the Workflow). It must be hashable in
        this frozen state.

    Task
        *(aka WorkflowJob, aka "Job that may have up/downstream dependencies")*
        Can be created using the TaskDag method create_task().  Represents a
        unit of work with potential upstream/downstream dependencies.

    Job
        *(an executable unit of work)*
        When a Workflow is first created, all its Jobs are populated in the
        database. A Job is tightly linked to a set of JobInstances. The only
        difference between a Job and a Task is that a Task is a Job that has
        dependencies or dependents.


For developers
**************

You'll want to study to the :doc:`API Reference <modules>`.
