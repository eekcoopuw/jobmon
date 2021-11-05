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

    TaskInstance
        The actual instance of execution of a Task command. The equivalent of a single qsub on
        an SGE Cluster. Jobmon will create TaskInstances from the Tasks that you define. This
        is an actual run of a task. Like calling a function in Python. One Task can have
        multiple task instances if they are retried.

    Nodes
        TODO: Fill out

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
