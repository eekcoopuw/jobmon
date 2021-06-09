"""Task Instance object from the distributor's perspective."""
from __future__ import annotations

import logging
import time
from http import HTTPStatus as StatusCodes
from typing import Optional

from jobmon.cluster_type.api import import_cluster
from jobmon.cluster_type.base import ClusterDistributor
from jobmon.constants import TaskInstanceStatus
from jobmon.exceptions import InvalidResponse, RemoteExitInfoNotAvailable
from jobmon.requester import Requester, http_request_ok
from jobmon.serializers import SerializeDistributorTaskInstance


logger = logging.getLogger(__name__)


class DistributorTaskInstance:
    """Object used for communicating with JSM from the distributor node.

    Args:
        task_instance_id (int): a task_instance_id
        distributor (ClusterDistributor): an instance of an ClusterDistributor or a subclass
        distributor_id (int, optional): the distributor_id associated with this
            task_instance
        requester (Requester, optional): a requester to communicate with
            the JSM. default is shared requester
    """

    def __init__(self, task_instance_id: int, workflow_run_id: int,
                 distributor: ClusterDistributor, requester: Requester,
                 distributor_id: Optional[int] = None):

        self.task_instance_id = task_instance_id
        self.workflow_run_id = workflow_run_id
        self.distributor_id = distributor_id

        self.report_by_date: float

        self.error_state = ""
        self.error_msg = ""

        # interfaces to the distributor and server
        self.distributor = distributor

        self.requester = requester

    @classmethod
    def from_wire(cls, wire_tuple: tuple, distributor: ClusterDistributor, requester: Requester
                  ) -> DistributorTaskInstance:
        """Create an instance from json that the JQS returns.

        Args:
            wire_tuple: tuple representing the wire format for this
                task. format = serializers.SerializeDistributorTask.to_wire()
            distributor: which distributor this task instance is
                being run on
            requester: requester for communicating with central services

        Returns:
            DistributorTaskInstance
        """
        kwargs = SerializeDistributorTaskInstance.kwargs_from_wire(wire_tuple)
        ti = cls(task_instance_id=kwargs["task_instance_id"],
                 workflow_run_id=kwargs["workflow_run_id"],
                 distributor=distributor,
                 distributor_id=kwargs["distributor_id"],
                 requester=requester)
        return ti

    @classmethod
    def register_task_instance(cls, task_id: int, workflow_run_id: int, distributor: ClusterDistributor,
                               requester: Requester) -> DistributorTaskInstance:
        """Register a new task instance for an existing task_id.

        Args:
            task_id (int): the task_id to register this instance with
            distributor (ClusterDistributor): which distributor to place this task on
            requester: requester for communicating with central services
        """
        app_route = '/distributor/task_instance'
        return_code, response = requester.send_request(
            app_route=app_route,
            message={'task_id': task_id,
                     'workflow_run_id': workflow_run_id,
                     'distributor_type': distributor.__class__.__name__},
            request_type='post',
            logger=logger
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from POST '
                f'request through route {app_route}. Expected '
                f'code 200. Response content: {response}')

        return cls.from_wire(response['task_instance'], distributor=distributor, requester=requester)

    def register_no_distributor_id(self, distributor_id: int) -> None:
        """Register that submission failed with the central service

        Args:
            distributor_id: placeholder distributor id. generall -9999
        """
        self.distributor_id = distributor_id

        app_route = (
            f'/distributor/task_instance/{self.task_instance_id}/log_no_distributor_id')
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={'distributor_id': distributor_id},
            request_type='post',
            logger=logger
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from POST '
                f'request through route {app_route}. Expected '
                f'code 200. Response content: {response}')

    def register_submission_to_batch_distributor(self, distributor_id: int,
                                              next_report_increment: float) -> None:
        """Register the submission of a new task instance to batch distributor.

        Args:
            distributor_id (int): distributor id created by distributor for this task
                instance
            next_report_increment: how many seconds to wait for
                report or status update before considering the task lost
        """
        self.distributor_id = distributor_id

        app_route = f'/distributor/task_instance/{self.task_instance_id}/log_distributor_id'
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={'distributor_id': str(distributor_id),
                     'next_report_increment': next_report_increment},
            request_type='post',
            logger=logger
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from POST '
                f'request through route {app_route}. Expected '
                f'code 200. Response content: {response}')

        self.report_by_date = time.time() + next_report_increment

    def log_error(self) -> None:
        """Log an error from the distributor loops."""
        if self.distributor_id is None:
            raise ValueError("distributor_id cannot be None during log_error")
        distributor_id = self.distributor_id
        logger.debug(f"log_error for distributor_id {distributor_id}")
        if not self.error_state:
            raise ValueError("cannot log error if self.error_state isn't set")

        if self.error_state == TaskInstanceStatus.UNKNOWN_ERROR:
            app_route = f"/distributor/task_instance/{self.task_instance_id}/log_unknown_error"
        else:
            app_route = f"/distributor/task_instance/{self.task_instance_id}/log_known_error"

        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={
                'error_state': self.error_state,
                'error_message': self.error_msg,
                'distributor_id': distributor_id,
            },
            request_type='post',
            logger=logger
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from POST '
                f'request through route {app_route}. Expected '
                f'code 200. Response content: {response}')

    def infer_error(self) -> None:
        """Infer error by checking the distributor remote exit info."""
        # infer error state if we don't know it already
        if self.distributor_id is None:
            raise ValueError("distributor_id cannot be None during log_error")
        distributor_id = self.distributor_id

        try:
            error_state, error_msg = self.distributor.get_remote_exit_info(distributor_id)
        except RemoteExitInfoNotAvailable:
            error_state = TaskInstanceStatus.UNKNOWN_ERROR
            error_msg = (f"Unknown error caused task_instance_id {self.task_instance_id} "
                         "to be lost")
        self.error_state = error_state
        self.error_msg = error_msg

    def dummy_distributor_task_instance_run_and_done(self) -> None:
        """For all instances other than the DummyExecutor, the worker node task instance should
        be the one logging the done status. Since the DummyExecutor doesn't actually run
        anything, the task_instance_distributor needs to mark it done so that distributor can
        proceed through the DAG.
        """
        if self.distributor.__class__.__name__ != "DummyExecutor":
            logger.error("Cannot directly log a task instance done unless using the Dummy "
                         "Executor")
        logger.debug("Moving the job to running, then done so that dependencies can proceed to"
                     " mock a successful dag traversal process")
        run_app_route = f'/worker/task_instance/{self.task_instance_id}/log_running'
        run_message = {'process_group_id': '0', 'next_report_increment': 60}
        return_code, response = self.requester.send_request(
            app_route=run_app_route,
            message=run_message,
            request_type='post',
            logger=logger
        )
        if return_code != StatusCodes.OK:
            raise InvalidResponse(f'Unexpected status code {return_code} from POST '
                                  f'request through route {run_app_route}. Expected '
                                  f'code 200. Response content: {response}')
        done_app_route = f"/worker/task_instance/{self.task_instance_id}/log_done"
        done_message = {'nodename': 'DummyNode', 'distributor_id': self.distributor_id}
        return_code, response = self.requester.send_request(
            app_route=done_app_route,
            message=done_message,
            request_type='post',
            logger=logger
        )
        if return_code != StatusCodes.OK:
            raise InvalidResponse(f'Unexpected status code {return_code} from POST '
                                  f'request through route {done_app_route}. Expected '
                                  f'code 200. Response content: {response}')
