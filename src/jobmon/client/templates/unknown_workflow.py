"""Unknown Workflow for easy backward compatibility and testing."""
import logging
from typing import Any, List, Optional, Union

from jobmon.client.client_config import ClientConfig
from jobmon.client.distributor.distributor_config import DistributorConfig
from jobmon.client.swarm.workflow_run import WorkflowRun
from jobmon.client.tool import Tool
from jobmon.client.workflow import Workflow
from jobmon.cluster_type.api import import_cluster
from jobmon.requester import Requester


logger = logging.getLogger(__name__)


class ResumeStatus(object):
    """Resume constants."""

    RESUME = True
    DONT_RESUME = False


class UnknownWorkflow(Workflow):
    """Unknown Workflow template.

    (aka Batch, aka Swarm)
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

    _tool: Optional[Tool] = None

    def __init__(self,
                 workflow_args: str = "",
                 name: str = "",
                 description: str = "",
                 stderr: Optional[str] = None,
                 stdout: Optional[str] = None,
                 project: Optional[str] = None,
                 reset_running_jobs: bool = True,
                 working_dir: Optional[str] = None,
                 cluster_name: str = 'buster',
                 fail_fast: bool = False,
                 requester: Optional[Requester] = None,
                 seconds_until_timeout: int = 36000,
                 resume: bool = ResumeStatus.DONT_RESUME,
                 reconciliation_interval: Optional[int] = None,
                 heartbeat_interval: Optional[int] = None,
                 report_by_buffer: Optional[float] = None,
                 workflow_attributes: Union[List, dict] = None,
                 max_concurrently_running: int = 10_000,
                 chunk_size: int = 500) -> None:
        """Initializationof the Unknown Workflow object.

        The Unknown Workflow object was created so that users of older versions
        of Jobmon (before 2.0) are able to update the imports and run their
        scripts as normal. In order to do this the Unknown Workflow associates
        with the 'Unknown' Tool. We recommend however, that users use the
        Workflow object to build out a better classification of what their
        workflows and tasks are doing.

        Args:
            workflow_args: unique identifier of a workflow
            name: name of the workflow
            description: description of the workflow
            stderr: filepath where stderr should be sent, if run on SGE
            stdout: filepath where stdout should be sent, if run on SGE
            project: SGE project to run under, if run on SGE
            reset_running_jobs: whether or not to reset running jobs upon resume
            working_dir: the working dir that a job should be run from,
                if run on SGE
            cluster_name: the name of the cluster the tasks will run on.
            executor_class: name of one of Jobmon's executors
            fail_fast: whether or not to break out of distributor on
                first failure
            requester: the requester used to communicate with central services.
            seconds_until_timeout: amount of time (in seconds) to wait
                until the whole workflow times out. Submitted jobs will
                continue
            resume: whether the workflow should be resumed or not, if
                it is not set to resume and an identical workflow already
                exists, the workflow will error out
            reconciliation_interval: rate at which reconciler reconciles
                jobs to for errors and check state changes, default set to 10
                seconds in client config, but user can reconfigure here
            heartbeat_interval: rate at which worker node reports
                back if it is still alive and running
            report_by_buffer: number of heartbeats we push out the
                report_by_date (default = 3.1) so a job in qw can miss 3
                reconciliations or a running job can miss 3 worker heartbeats,
                and then we will register that it as lost
            workflow_attributes:  attributes that make this workflow different
                from other workflows that the user wants to record.
            max_concurrently_running: How many running jobs to allow in parallel
            chunk_size: size of task and node chunks that are bound in one call to the db
        """
        cfg = DistributorConfig.from_defaults()
        if reconciliation_interval is not None:
            cfg.reconciliation_interval = reconciliation_interval
        if heartbeat_interval is not None:
            cfg.heartbeat_interval = heartbeat_interval
        if report_by_buffer is not None:
            cfg.heartbeat_report_by_buffer = report_by_buffer
        self._execution_config = cfg

        # run params
        self._reset_running_jobs = reset_running_jobs
        self._fail_fast = fail_fast
        self._seconds_until_timeout = seconds_until_timeout
        self._resume = resume

        if requester is None:
            requester_url = ClientConfig.from_defaults().url
            requester = Requester(requester_url)
        self.requester = requester

        if self._tool is None:
            unknown_tool = Tool()
            self._set_tool(unknown_tool)
        super().__init__(
            tool_version_id=self._tool.active_tool_version.id,
            workflow_args=workflow_args,
            name=name,
            description=description,
            requester=self.requester,
            workflow_attributes=workflow_attributes,
            max_concurrently_running=max_concurrently_running,
            chunk_size=chunk_size
        )
        cluster = self._get_cluster_by_name(cluster_name)
        self._set_distributor(cluster_type_name=cluster._cluster_type_name)

    @classmethod
    def _set_tool(cls: Any, tool: Tool) -> None:
        cls._tool = tool

    def _set_distributor(self, cluster_type_name: str) -> None:
        """Set which distributor and parameters to use to run the tasks.

        Args:
            cluster_type_name (str): string referring to one of the executor
            classes in jobmon.client.swarm.executors
        """
        logger.info("Set cluster_type_name to {}".format(cluster_type_name))

        module = import_cluster(cluster_type_name)
        ClusterDistributor = module.get_cluster_distributor_class()
        self._distributor = ClusterDistributor()

        if not hasattr(self._distributor, "submit_to_batch_distributor"):
            raise AttributeError("Distributor must have an submit_to_batch_distributor()"
                                 " method")

    def run(self) -> WorkflowRun:
        """Run this workflow.

        Returns:
            WorkflowRun
        """
        return super().run(self._fail_fast, self._seconds_until_timeout,
                           self._resume, self._reset_running_jobs)
