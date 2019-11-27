from jobmon.client import shared_requester
from jobmon.client._logging import ClientLogging as logging
from jobmon.client.requests.requester import Requester
from jobmon.client.tool import Tool
from jobmon.client.workflow import Workflow


logger = logging.getLogger(__name__)


class ResumeStatus(object):
    RESUME = True
    DONT_RESUME = False


class UnknownWorkflow(Workflow):
    """(aka Batch, aka Swarm)
    A Workflow is a framework by which a user may define the relationship
    between tasks and define the relationship between multiple runs of the same
    set of tasks. The great benefit of the Workflow is that it's resumable.
    A Workflow can only be re-loaded if two things are shown to be exact
    matches to a previous Workflow:

    1. WorkflowArgs: It is recommended to pass a meaningful unique identifier
        to workflow_args, to ease resuming. However, if the Workflow is a
        one-off project, you may instantiate the Workflow anonymously, without
        WorkflowArgs. Under the hood, the WorkflowArgs will default to a UUID
        which, as it is randomly generated, will be harder to remember and thus
        harder to resume.

        Workflow args must be hashable. For example, CodCorrect or Como version
        might be passed as Args to the Workflow. For now, the assumption is
        WorkflowArgs is a string.

    2. The tasks added to the workflow. A Workflow is built up by
        using Workflow.add_task(). In order to resume a Workflow, all the same
        tasks must be added with the same dependencies between tasks.
    """
    _tool = Tool()

    def __init__(self,
                 workflow_args: str = None,
                 name: str = "",
                 description: str = "",
                 stderr: str = None,
                 stdout: str = None,
                 project: str = None,
                 reset_running_jobs: bool = True,
                 working_dir: str = None,
                 executor_class: str = 'SGEExecutor',
                 fail_fast: bool = False,
                 requester: Requester = shared_requester,
                 seconds_until_timeout: int = 36000,
                 resume: bool = ResumeStatus.DONT_RESUME,
                 reconciliation_interval: int = None,
                 heartbeat_interval: int = None,
                 report_by_buffer: float = None) -> None:
        """
        Args:
            workflow_args: unique identifier of a workflow
            name: name of the workflow
            description: description of the workflow
            stderr: filepath where stderr should be sent, if run on SGE
            stdout: filepath where stdout should be sent, if run on SGE
            project: SGE project to run under, if run on SGE
            reset_running_jobs: whether or not to reset running jobs
            working_dir: the working dir that a job should be run from,
                if run on SGE
            executor_class: name of one of Jobmon's executors
            fail_fast: whether or not to break out of execution on
                first failure
            seconds_until_timeout: amount of time (in seconds) to wait
                until the whole workflow times out. Submitted jobs will
                continue
            resume: whether the workflow should be resumed or not, if
                it is not and an identical workflow already exists, the
                workflow will error out
            reconciliation_interval: rate at which reconciler reconciles
                jobs to for errors and check state changes, default set to 10
                seconds in client config, but user can reconfigure here
            heartbeat_interval: rate at which worker node reports
                back if it is still alive and running
            report_by_buffer: number of heartbeats we push out the
                report_by_date (default = 3.1) so a job in qw can miss 3
                reconciliations or a running job can miss 3 worker heartbeats,
                and then we will register that it as lost
        """
        self._set_executor(executor_class, stderr=stderr, stdout=stdout,
                           working_dir=working_dir, project=project)

        # run params
        self._reset_running_jobs = reset_running_jobs
        self._fail_fast = fail_fast
        self._seconds_until_timeout = seconds_until_timeout
        self._resume = resume
        self._reconciliation_interval = reconciliation_interval
        self._heartbeat_interval = heartbeat_interval
        self._report_by_buffer = report_by_buffer

        # pass
        super().__init__(
            tool_version_id=self._tool.active_tool_version_id,
            workflow_args=workflow_args,
            name=name,
            description=description,
            requester=requester)

    def _set_executor(self, executor_class, *args, **kwargs) -> None:
        """Set which executor to use to run the tasks.

        Args:
            executor_class (str): string referring to one of the executor
            classes in jobmon.client.swarm.executors
        """
        self.executor_class = executor_class
        if self.executor_class == 'SGEExecutor':
            from jobmon.client.swarm.executors.sge import SGEExecutor
            self.executor = SGEExecutor(*args, **kwargs)
        elif self.executor_class == "SequentialExecutor":
            from jobmon.client.swarm.executors.sequential import \
                SequentialExecutor
            self.executor = SequentialExecutor()
        elif self.executor_class == "DummyExecutor":
            from jobmon.client.swarm.executors.dummy import DummyExecutor
            self.executor = DummyExecutor()
        else:
            raise ValueError(f"{executor_class} is not a valid executor_class")

        if not hasattr(self.executor, "execute"):
            raise AttributeError("Executor must have an execute() method")

    def run(self):
        """Run this workflow"""

        # TODO: override more behavior. need to construct
        # task_instance_state_controller and pass into workflow_run somehow
        # in order to preserve old API
        super().run(self._fail_fast, self._seconds_until_timeout,
                    self._resume, self._reset_running_jobs)
