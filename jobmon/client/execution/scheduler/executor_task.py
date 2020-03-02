from __future__ import annotations

from typing import Optional

from jobmon.client import shared_requester
from jobmon.client import ClientLogging as logging
from jobmon.client.requests.requester import Requester
from jobmon.client.execution.strategies.base import ExecutorParameters
from jobmon.serializers import SerializeExecutorTask

logger = logging.getLogger(__name__)


class ExecutorTask:

    # this API should always match what's returned by
    # serializers.SerializeExecutorTask
    def __init__(self,
                 task_id: int,
                 workflow_id: int,
                 node_id: int,
                 task_args_hash: int,
                 name: str,
                 command: str,
                 status: str,
                 executor_parameters: ExecutorParameters,
                 requester: Requester,
                 last_nodename: Optional[str] = None,
                 last_process_group_id: Optional[int] = None):
        """
        This is a Task object used on the RESTful API client side
        when constructing task instances.

        Args:
            task_id: job_id associated with this task
            workflow_id: workflow_id associated with this task
            node_id: node_id assocaiated with this task
            task_args_hash: hash of command for this task
            name: name associated with this task
            command: what command to run when executing
            status: job status  associated with this task
            executor_parameters: Executor parameters class associated with the
                current executor for this task
            last_nodename: where this task last executed
            last_process_group_id: what was the linux process group id of the
                last instance of this task
            requester: requester for communicating with central services
        """
        self.task_id = task_id
        self.workflow_id = workflow_id
        self.node_id = node_id
        self.task_args_hash = task_args_hash
        self.name = name
        self.command = command
        self.status = status
        self.last_nodename = last_nodename
        self.last_process_group_id = last_process_group_id

        self.executor_parameters = executor_parameters

        self.requester = requester

    @classmethod
    def from_wire(cls, wire_tuple: tuple, executor_class: str,
                  requester: Requester = shared_requester
                  ) -> ExecutorTask:
        """construct instance from wire format the JQS gives

        Args:
            wire_tuple (tuple): tuple representing the wire format for this
                task. format = serializers.SerializeExecutorTask.to_wire()
            executor_class (str): which executor class this task instance is
                being run on
            requester (Requester, shared_requester): requester for
                communicating with central services
        """

        # convert wire tuple into dictionary of kwargs
        kwargs = SerializeExecutorTask.kwargs_from_wire(wire_tuple)

        # instantiate job
        logger.info("Instantiate task tid {}".format(kwargs["task_id"]))
        executor_task = cls(
            task_id=kwargs["task_id"],
            workflow_id=kwargs["workflow_id"],
            node_id=kwargs["node_id"],
            task_args_hash=kwargs["task_args_hash"],
            name=kwargs["name"],
            command=kwargs["command"],
            status=kwargs["status"],
            last_nodename=kwargs["last_nodename"],
            last_process_group_id=kwargs["last_process_group_id"],
            executor_parameters=ExecutorParameters(
                executor_class=executor_class,
                num_cores=kwargs["num_cores"],
                queue=kwargs["queue"],
                max_runtime_seconds=kwargs["max_runtime_seconds"],
                j_resource=kwargs["j_resource"],
                m_mem_free=kwargs["m_mem_free"],
                context_args=kwargs["context_args"],
                resource_scales=kwargs["resource_scales"],
                hard_limits=kwargs["hard_limits"]),
            requester=requester)
        return executor_task
