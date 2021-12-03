"""Distributes and monitors state of Task Instances."""
from __future__ import annotations

import logging
import multiprocessing as mp
import sys
import threading
import time
from types import TracebackType
from typing import Dict, List, Optional, Type

import tblib.pickling_support

from jobmon.client.client_logging import ClientLogging
from jobmon.client.distributor.distributor_task import DistributorTask
from jobmon.client.distributor.distributor_task_instance import DistributorTaskInstance
from jobmon.client.distributor.distributor_workflow_run import DistributorWorkflowRun

from jobmon.cluster_type.base import ClusterDistributor
from jobmon.constants import TaskInstanceStatus, WorkflowRunStatus
from jobmon.exceptions import (
    InvalidResponse,
    RemoteExitInfoNotAvailable,
    ResumeSet,
    WorkflowRunStateError,
)
from jobmon.requester import http_request_ok, Requester
from jobmon.serializers import SerializeClusterType

ClientLogging().attach(__name__)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
tblib.pickling_support.install()


class ExceptionWrapper(object):
    """Handle exceptions."""

    def __init__(self, ee: Exception) -> None:
        """Initialization of execution wrapper."""
        self.ee = ee
        self.type: Optional[Type[BaseException]]
        self.value: Optional[BaseException]
        self.tb: Optional[TracebackType]
        self.type, self.value, self.tb = sys.exc_info()

    def re_raise(self) -> None:
        """Raise errors and add their traceback."""
        raise self.ee.with_traceback(self.tb)


class DistributorService:

    """
    This class has a bidirectional flow of information.

    It polls from the database and pushes new requests onto clusters.

    It pushes heartbeats from the cluster into the database.

    Is responsible for coordinating between instances and the correct cluster.


    """

    def __init__(
        self,
        workflow_id: int,
        workflow_run_id: int,
        distributor: ClusterDistributor,
        requester: Requester,
        workflow_run_heartbeat_interval: int = 30,
        task_instance_heartbeat_interval: int = 90,
        heartbeat_report_by_buffer: float = 3.1,
        queued_tasks_bulk_query_size: int = 100,
        distributor_poll_interval: int = 10,
        worker_node_entry_point: Optional[str] = None,
    ):
        # APIs
        self.requester = requester
        self.distributor = distributor

        # state tracking
        self.workflow_run = DistributorWorkflowRun(workflow_id, workflow_run_id, requester)

        # config
        self._workflow_run_heartbeat_interval = workflow_run_heartbeat_interval
        self._task_instance_heartbeat_interval = task_instance_heartbeat_interval
        self._heartbeat_report_by_buffer = heartbeat_report_by_buffer
        self._queued_tasks_bulk_query_size = queued_tasks_bulk_query_size
        self._distributor_poll_interval = distributor_poll_interval
        self._worker_node_entry_point = worker_node_entry_point

    def launch_queued_tasks(self):
        """
        pulls work from the db and pushes it to the distributor
        """
        tasks = self.get_queued_tasks()
        for task in tasks:
            self.workflow_run.register_task_instance(task)

