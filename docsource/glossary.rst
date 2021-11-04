Glossary of terms
#################

For users
*********

.. glossary::

    Tool
        The project (like CODem, DisMod, etc.) to associate your Workflow and Task Templates
        with. You may overhaul your Workflows and Tasks/TaskTemplates over time, but the
        concept of the project will remain to categorize them within the broader IHME pipeline.

    Workflow
        *(aka Batch, aka Swarm)*
        The object that encompasses all of your Tasks that will be executed. It builds on the
        DAG to include the setup logic to bind all of the objects to the database and capture
        metadata about the Workflow. Much like Task is the intent to run a command, and Task
        Instance is the actual execution, Workflow is the intent to run a DAG of Tasks, and a
        Workflow Run is the actual execution of the DAG traversal. Therefore a Workflow can be
        resumed if it failed on a previous Workflow Run, but the Tasks that it will execute
        must remain the same. This is codified in the WorkflowArgs parameter that the user
        can define to indicate what makes this set of Tasks unique such that no other
        Workflow will be the same.

    WorkflowStatus
        === =========================== =========================================================================
        ID  Label                       Description
        === =========================== =========================================================================
        G   REGISTERED                  Workflow created and validated
        B   BOUND                       Workflow bound to the database
        A   ABORTED                     Workflow encountered an error before a WorkflowRun was created
        C   CREATED                     Workflow created a WorkflowRun
        R   RUNNING                     Workflow has a WorkflowRun that is running
        S   SUSPENDED                   Workflow paused if marked for resume, can be set to running again
        F   FAILED                      Workflow unsuccessful in one or more WorkflowRuns, none finished as Done
        D   DONE                        Workflow finished successfully
        === =========================== =========================================================================

    WorkflowArgs
        A set of arguments that are used to determine the "uniqueness" of the
        Workflow and whether it can be resumed. Must be hashable. For example,
        CodCorrect or Como version might be passed as Args to the Workflow.
        Coupled with a populated TaskDag, WorkflowArgs define a Workflow.

    WorkflowRun
        WorkflowRun enables tracking for multiple runs of a single Workflow. A
        Workflow may be started/paused/ and resumed multiple times. Each start
        or resume represents a new WorkflowRun.

        In order for a Workflow to be deemed DONE (successful), it must have 1 or more
        WorkflowRuns. A Task belongs to a workflow, so it may be run multiple times (i.e.
        multiple Task Instances will be created), but once the Task reaches a DONE
        state, it will no longer be added to a subsequent WorkflowRun, and therefore the
        Workflow Run will not create any Task Instances for that Task. (If a user wants it to
        be rerun, then it must be reset to a Registered, or other non-Done state)

    WorkflowRunStatus
        === =========================== =================================================================================================
        ID  Label                       Description
        === =========================== =================================================================================================
        G   REGISTERED                  WorkflowRun has been validated
        B   BOUND                       WorkflowRun has been bound to the database
        R   RUNNING                     WorkflowRun is currently Running
        D   DONE                        WorkflowRun has run to completion
        A   ABORTED                     WorkflowRun encountered problems will binding so it stopped
        S   STOPPED                     WorkflowRun has been stopped probably by keyboard interrupt from user
        E   ERROR                       WorkflowRun has not completed successfully, may have lost contact with services
        C   COLD_RESUME                 WorkflowRun was set to resume once all Tasks were stopped
        H   HOT_RESUME                  WorkflowRun was set to resume while Tasks are still running, they will continue running
        T   TERMINATED                  WorkflowRun was in resume, new WF Run created to pick up remaining Tasks, so this one Terminated
        === =========================== =================================================================================================

    Dag
        Directed Acyclic Graph. The graph of Tasks that will be traversed upon execution of a
        Workflow Run. Made up of Nodes (Tasks with specific node arguments) and Edges (the
        relationship between two Nodes)

    Node
        The object representing a Task within a DAG. It does not gather all of the command and
        execution information that the Task contains, it just keeps track of where it is in a
        DAG, and what attributes from the Task make it unique within the DAG. Often, many Tasks
        will be created from a TaskTemplate, however in order for them to not all run
        identically they vary over some argument like which cause or location they operate
        over, this is what makes a given Node unique.

    Edge
        The relationship between an upstream and a downstream Node.

    TaskTemplate
        The Task Template outlines the structure of a Task to give it more context within the
        DAG and over multiple executions of the DAG. A user defines a command template that
        lays out the pattern that each Task will fill in with arguments. The user categorizes
        the arguments to signify: what makes it unique in the DAG among other Tasks of the
        same Template Type (Node Args), args that indicate new data running through the
        command (Task Args), or args that only affect how the model is run on the Executor,
        but will not affect anything about the inputs to the model or the code that is
        executed (Op Args).

        A Task Template is associated with a given Tool so it can be used over many workflows.
        It also can be versioned.

    Task
        A single executable object in the workflow, a command that will be run. Relate it to a
        Task Template in order to classify it as a type of job within the context of your
        Workflow. Do this by using the Task Template create_task() function.

    TaskAttribute
        Additional attributes of the task that can be tracked. For example, release ID or
        location set version ID. Task attributes are not passed to the job but may be useful
        for profiling or resource prediction work in the Jobmon database. Pass in task
        attributes as a list or dictionary to create_task().

    TaskStatus
        === =========================== =======================================================================================
        ID  Label                       Description
        === =========================== =======================================================================================
        G   REGISTERED                  Task has been bound to the database
        Q   QUEUED_FOR_INSTANTIATION    Task's dependencies have been met, it can be run when the scheduler is ready
        I   INSTANTIATED                Task has had a Task Instance created that will be submitted to the Executor
        R   RUNNING                     Task is running on the chosen Executor
        E   ERROR_RECOVERABLE           Task has errored out but has more attempts so it will be retried
        A   ADJUSTING_RESOURCES         Task has errored with a resource error, the resources will be adjusted before retrying
        F   ERROR_FATAL                 Task has errored out and has used all of the attempts. It cannot be retried
        D   DONE                        Task ran to completion
        === =========================== =======================================================================================

    TaskInstance
        The actual instance of execution of a Task command. The equivalent of a single qsub on
        an SGE Cluster. Jobmon will create TaskInstances from the Tasks that you define. This
        is an actual run of a task. Like calling a function in Python. One Task can have
        multiple task instances if they are retried.

    TaskInstanceStatus
        === =========================== ==============================================================================
        ID  Label                       Description
        === =========================== ==============================================================================
        B   SUBMITTED_TO_BATCH_EXECUTOR Task instance submitted normally.
        D   DONE                        Task instance finishes normally.
        E   ERROR                       Task instance has hit an application error.
        I   INSTANTIATED                Task instance is created.
        R   RUNNING                     Task instance starts running normally.
        U   UNKNOWN_ERROR               Task instance stops reporting that it's alive and jobmon can't figure out why.
        W   NO_EXECUTOR_ID              Task instance submission has hit a bug and did not receive an executor_id.
        Z   RESOURCE_ERROR              Task instance died because of an insufficient resource request.
        K   KILL_SELF                   Task instance has been ordered to kill itself if it is still alive.
        === =========================== ==============================================================================

    Executor
        Where the Tasks will be run. The standard at IHME is to use the SGEExecutor so jobs
        are submitted on the cluster. However Jobs can be run locally using
        MultiprocessingExecutor, or SequentialExecutor. If the user wants to set up the Jobmon
        Workflow and test it without risking actually running the commands, they can use the
        DummyExecutor which imitates job submission.

    Workflow Attributes
        Additional attributes that are being tracked for a given Workflow. They are not required
        to use Jobmon, and workflow_attributes are not passed to your jobs. They are intended to
        track information for a given run and can be utilized for profiling and resource
        prediction.

For developers
**************

You'll want to study to the :doc:`API Reference <api/modules>`.
