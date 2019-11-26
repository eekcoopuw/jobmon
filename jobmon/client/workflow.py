from collections import OrderedDict
from http import HTTPStatus as StatusCodes
from typing import Dict, Optional
from typing import Dict
import uuid

from jobmon.client import shared_requester
from jobmon.client._logging import ClientLogging as logging
from jobmon.client.dag import Dag
from jobmon.client.requests.requester import Requester
from jobmon.client.task import Task
from jobmon.client.swarm.workflow.bound_task import BoundTask


logger = logging.getLogger(__name__)


class ResumeStatus(object):
    RESUME = True
    DONT_RESUME = False


class Workflow(object):
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

    def __init__(self,
                 tool_version_id: int,
                 workflow_args: str = None,
                 name: str = "",
                 description: str = "",
                 requester: Requester = shared_requester):
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
        self.tool_version_id = tool_version_id
        self.workflow_args = workflow_args
        self.name = name
        self.description = description
<<<<<<< HEAD
        self.status = "G"
=======
>>>>>>> c1dbbf6f55254fc0b4a942c4fb5530b1bb9f0b4f

        self.requester = requester

        # hash to task object mapping
        self.tasks: OrderedDict = OrderedDict()
<<<<<<< HEAD
=======
        # hash to bound task object mapping
        self.bound_tasks: Dict[int, SwarmTask] = {}
>>>>>>> c1dbbf6f55254fc0b4a942c4fb5530b1bb9f0b4f

        if workflow_args:
            self.workflow_args = workflow_args
        else:
            self.workflow_args = str(uuid.uuid4())
            logger.info("Workflow_args defaulting to uuid {}. To resume this "
                        "workflow, you must re-instantiate Workflow and pass "
                        "this uuid in as the workflow_args. As a uuid is hard "
                        "to remember, we recommend you name your workflows and"
                        " make workflow_args a meaningful unique identifier. "
                        "Then add the same tasks to this workflow"
                        .format(self.workflow_args))

<<<<<<< HEAD
    @property
    def workflow_id(self) -> int:
        if not hasattr(self, "_workflow_id"):
            raise AttributeError(
                "workflow_id cannot be accessed before workflow is bound")
        return self._workflow_id

    @property
    def dag_id(self) -> int:
        if not hasattr(self, "_workflow_id"):
            raise AttributeError(
                "dag_id cannot be accessed before workflow is bound")
        return self._node.dag_id

=======
>>>>>>> c1dbbf6f55254fc0b4a942c4fb5530b1bb9f0b4f
    def run(self,
            fail_fast: bool = False,
            seconds_until_timeout: int = 36000,
            resume: bool = ResumeStatus.DONT_RESUME,
            reset_running_jobs: bool = True):
        pass
<<<<<<< HEAD

    def add_task(self, task: Task):
        """Add a task to the workflow to be executed.
           Set semantics - add tasks once only, based on hash name. Also
           creates the job. If is_no has no task_id the creates task_id and
           writes it onto object.
        """
        logger.debug(f"Adding Task {task}")
        if task.hash in self.tasks:
            raise ValueError(f"A task with hash {task.hash} already exists. "
                             f"All tasks in a workflow must have unique "
                             f"commands. Your command was: {task.command}")
        self.tasks[hash(task)] = task
        logger.debug(f"Task {task.hash} added")

        return task

    def add_tasks(self, tasks: Task):
        """Add a list of task to the workflow to be executed"""
        for task in tasks:
            self.add_task(task)

    def _create_bound_task(self, task: Task):
        bound_task = BoundTask()
        return bound_task

    def bind(self):
        # create DAG
        dag = Dag()

        # bind nodes and add to DAG
        for _, task in self.tasks.values():
            node = task.node
            node.bind()
            dag.add_node(task.node)

        # bind DAG/add edges to db
        dag.bind()

        # get workflow_id
        workflow_id = self._get_workflow_id()
        if workflow_id is None:
            workflow_id = self._add_workflow()
        self._workflow_id = workflow_id

        # add tasks to workflow
        for _, task in self.tasks.values():
            task.workflow_id = self.workflow_id
            task.bind()

    def _get_workflow_id(self) -> Optional[int]:
        return_code, response = self.requester.send_request(
            app_route='/workflow',
            message={
            },
            request_type='get'
        )
        if return_code != StatusCodes.OK:
            raise ValueError(f'Unexpected status code {return_code} from GET '
                             f'request through route /workflow. Expected code '
                             f'200. Response content: {response}')
        return response['workflow_id']

    def _add_workflow(self) -> int:
        app_route = f'/workflow'
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={
            },
            request_type='post'
        )
        if return_code != StatusCodes.OK:
            raise ValueError(f'Unexpected status code {return_code} from PUT '
                             f'request through route {app_route}. Expected '
                             f'code 200. Response content: {response}')
        return response["workflow_id"]

    def __hash__(self) -> int:
        pass
=======
>>>>>>> c1dbbf6f55254fc0b4a942c4fb5530b1bb9f0b4f
