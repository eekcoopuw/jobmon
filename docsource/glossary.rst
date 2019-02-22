Glossary of terms
#################

For users
*********

.. glossary::

    Workflow
        *(aka Batch, aka Swarm)*
        Has a set of tasks or a TaskDag and a set of WorkflowArgs.
        A Workflow can only be re-loaded if the WorkflowArgs and the Tasks
        that are attached to it are shown to be exact matches to a previous Workflow.

        .. todo::
            Consider capturing the pip freeze or other environmental markers as
            part of the Workflow hash?)

        .. todo::
            Explore a mechanism by which a subset of WorkflowArgs may be passed
            to TaskDag and inform the shape of the Dag itself. This would
            enable more extensive code-reuse, but would require more
            intelligence in the Tasks themselves.

    WorkflowArgs
        A set of arguments that are used to deteremine the "uniqueness" of the
        Workflow and whether it can be resumed. Must be hashable. For example,
        CodCorrect or Como version might be passed as Args to the Workflow.
        Coupled with a populated TaskDag, WorkflowArgs define a Workflow.

    WorkflowRun
        WorkflowRun enables tracking for multiple runs of a single Workflow. A
        Workflow may be started/paused/ and resumed multiple times. Each start
        or resume represents a new WorkflowRun.

        In order for a Workflow can be deemed to be DONE (successfully), it must
        have 1 or more WorkflowRuns. In the current implementation, a Workflow Job
        may belong to one or more WorkflowRuns, but once the Job reaches a DONE
        state, it will no longer be added to a subsequent WorkflowRun. However,
        this is not enforced via any database constraints.

    TaskDag
        A set of Tasks. Upon the workflow calling "_execute()" on it, it is frozen (i.e.  becomes immutable for the lifetime of the Workflow). It must be hashable in
        this frozen state.

    Task
        *(aka WorkflowJob, aka "Job that may have up/downstream dependencies")*
        Can be created using the TaskDag method create_task(). Represents a
        unit of work with potential upstream/downstream dependencies.

    Job
        *(an executable unit of work)*
        When a Workflow is first created, all its Jobs are populated in the
        database. A Job is tightly linked to a set of JobInstances. The only
        difference between a Job and a Task is that a Task is a Job that has
        dependencies or dependents.

    Tag
        A identifier attribute of a Task that allows tasks to be grouped together.
        Currently, this identifier is only used for purposes of visualization: all
        tasks with the same tag will be colored the same.


For developers
**************

You'll want to study to the :doc:`API Reference <modules>`.
