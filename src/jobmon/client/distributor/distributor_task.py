"""Task object used by distributor to create Task Instances from."""
from __future__ import annotations

import logging

# from jobmon.client.distributor.strategies.base import ExecutorParameters
from jobmon.requester import Requester
from jobmon.serializers import SerializeTask


logger = logging.getLogger(__name__)


class DistributorTask:
    """Task object used by distributor to create Task Instances from."""

    # this API should always match what's returned by
    # serializers.SerializeTask
    def __init__(self, task_id: int, workflow_id: int, node_id: int, task_args_hash: int,
                 name: str, command: str, status: str, queue_id: int,
                 requested_resources: dict, requester: Requester):
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
            queue_id: the queue we are submitting to
            requested_resources: the distributor resources for this task
            requester: requester for communicating with central services
        """
        self.task_id = task_id
        self.workflow_id = workflow_id
        self.node_id = node_id
        self.task_args_hash = task_args_hash
        self.name = name
        self.command = command
        self.status = status
        self.queue_id = queue_id
        self.requested_resources = requested_resources

        self.requester = requester

    @classmethod
    def from_wire(cls, wire_tuple: tuple, executor_class: str, requester: Requester
                  ) -> DistributorTask:
        """Construct instance from wire format the JQS gives

        Args:
            wire_tuple (tuple): tuple representing the wire format for this
                task. format = serializers.SerializeTask.to_wire()
            executor_class (str): which executor class this task instance is
                being run on
            requester (Requester, shared_requester): requester for
                communicating with central services
        """
        # convert wire tuple into dictionary of kwargs
        kwargs = SerializeTask.kwargs_from_wire(wire_tuple)

        # instantiate job
        logger.debug("Instantiate task tid {}".format(kwargs["task_id"]))
        executor_task = cls(
            task_id=kwargs["task_id"],
            workflow_id=kwargs["workflow_id"],
            node_id=kwargs["node_id"],
            task_args_hash=kwargs["task_args_hash"],
            name=kwargs["name"],
            command=kwargs["command"],
            status=kwargs["status"],
            queue_id=kwargs["queue_id"],
            requested_resources=kwargs["requested_resources"],
            requester=requester
        )
        return executor_task