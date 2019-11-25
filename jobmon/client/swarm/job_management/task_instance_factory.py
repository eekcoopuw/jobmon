from builtins import str
from http import HTTPStatus as StatusCodes
import threading
from time import sleep
import traceback
from typing import Optional, List
import _thread

from jobmon.client import shared_requester, client_config
from jobmon.client.requester import Requester
from jobmon.client.swarm.executors import Executor
from jobmon.client.swarm.job_management.executor_task import ExecutorTask
from jobmon.client.swarm.job_management.executor_task_instance import (
    ExecutorTaskInstance)
from jobmon.client.swarm.executors.sequential import SequentialExecutor
from jobmon.models.attributes.constants import qsub_attribute
from jobmon.client.client_logging import ClientLogging as logging

logger = logging.getLogger(__name__)


class TaskInstanceFactory(object):

    def __init__(self,
                 workflow_id: int,
                 workflow_run_id: int,
                 executor: Optional[Executor] = None,
                 n_queued_tasks: int = 1000,
                 stop_event: Optional[threading.Event] = None,
                 requester: Requester = shared_requester):
        """The TaskInstanceFactory is in charge of queueing tasks and creating
        task_instances, in order to get the tasks from merely Task objects to
        running code.

        Args:
            workflow_id: the id of the workflow associated with tasks
            workflow_run_id: the id for the workflow run associated with these
                task instances
            executor: executor to use w/ this factory. SequentialExecutor,
                DummyExecutor or SGEExecutor. Default SequentialExecutor
            n_queued_tasks: number of queued tasks to return and send to be
                instantiated. default 1000
            stop_event: Object of type threading.Event
            requester: requester for communicating with central services
        """
        self.workflow_id = workflow_id
        self.workflow_run_id = workflow_run_id
        self.requester = requester
        self.n_queued_tasks = n_queued_tasks
        self.report_by_buffer = client_config.report_by_buffer
        self.heartbeat_interval = client_config.heartbeat_interval

        # At this level, default to using a Sequential Executor if None is
        # provided. End-users shouldn't be interacting at this level (they
        # should be using Workflows), so this state will typically
        # only be invoked in testing.
        if executor:
            self.set_executor(executor)
        else:
            se = SequentialExecutor()
            self.set_executor(se)

        if not stop_event:
            self._stop_event = threading.Event()
        else:
            self._stop_event = stop_event

    def instantiate_queued_tasks_periodically(self, poll_interval: int = 3
                                              ) -> None:
        """Running in a thread, this function allows the JobInstanceFactory to
        periodically get all tasks that are ready and queue them for
        instantiation

        Args:
            poll_interval (int, optional): how often you want this function to
                poll for newly ready tasks
        """
        logger.info("Polling for and instantiating queued tasks at {}s "
                    "intervals".format(poll_interval))
        while True and not self._stop_event.is_set():
            try:
                logger.debug("Queuing at interval {}s".format(poll_interval))
                self.instantiate_queued_tasks()
                sleep(poll_interval)
            except Exception as e:
                msg = "About to raise Keyboard Interrupt signal {}".format(e)
                logger.error(msg)
                stack = traceback.format_exc()
                # Also write to stdout because this is a serious problem
                print(msg, stack)
                # Also send to server
                msg = (
                    f"Error in {self.__class__.__name__}, {str(self)} "
                    f"in instantiate_queued_tasks_periodically: \n{stack}")
                shared_requester.send_request(
                    app_route="/error_logger",
                    message={"traceback": msg},
                    request_type="post")
                _thread.interrupt_main()
                self._stop_event.set()
                raise

    def instantiate_queued_tasks(self) -> List[int]:
        """Pull all tasks that are ready, create task instances for them, and
        thereby run them
        """
        logger.debug("TIF: Instantiating Queued Tasks")
        tasks = self._get_tasks_queued_for_instantiation()
        logger.debug("TIF: Found {} Queued Tasks".format(len(tasks)))
        task_instance_ids = []
        for task in tasks:
            task_instance = self._create_task_instance(task)
            if task_instance:
                task_instance_ids.append(task_instance.id)

        logger.debug("TIF: Returning {} Instantiated Tasks".format(
            len(task_instance_ids)))
        return task_instance_ids

    def set_executor(self, executor: Executor) -> None:
        """
        Sets the executor that will be used for all jobs queued downstream
        of the set event.

        Args:
            executor (callable): Any callable that takes a Job and returns
                either None or an Int. If Int is returned, this is assumed
                to be the TaskInstances executor_id, and will be registered
                with the JobStateManager as such.
        """
        # TODO: Add some validation that the passed object is callable and
        # and follows the args/returns requirements of an executor. Potentially
        # resuscitate the Executor abstract base class.
        self.executor = executor

    def _create_task_instance(self, task: ExecutorTask
                              ) -> Optional[ExecutorTaskInstance]:
        """
        Creates a TaskInstance based on the parameters of Task and tells the
        JobStateManager to react accordingly.

        Args:
            task (ExecutorTask): A Task that we want to execute
        """
        try:
            task_instance = ExecutorTaskInstance.register_task_instance(
                task.task_id, self.executor)
        except Exception as e:
            logger.error(e)
            stack = traceback.format_exc()
            msg = (
                f"Error while creating task instances {self.__class__.__name__}"
                f", {str(self)} while submitting tid {task.task_id}: \n{stack}")
            shared_requester.send_request(
                app_route="/error_logger",
                message={"traceback": stack},
                request_type="post")
            # we can't do anything more at this point so must return None
            return None

        logger.debug("Executing {}".format(task.command))

        # TODO: unify qsub IDS to be meaningful across executor types

        command = task_instance.executor.build_wrapped_command(
            command=task.command,
            job_instance_id=task_instance.task_instance_id,
            last_nodename=task.last_nodename,
            last_process_group_id=task.last_process_group_id)
        # The following call will always return a value.
        # It catches exceptions internally and returns ERROR_SGE_JID
        logger.debug(
            "Using the following parameters in execution: "
            f"{task.executor_parameters}")
        executor_id = task_instance.executor.execute(
            command=command,
            name=task.name,
            executor_parameters=task.executor_parameters)
        if executor_id == qsub_attribute.NO_EXEC_ID:
            if executor_id == qsub_attribute.NO_EXEC_ID:
                logger.debug(f"Received {qsub_attribute.NO_EXEC_ID} meaning "
                             f"the task did not qsub properly, moving "
                             f"to 'W' state")
                task_instance.register_no_exec_id(executor_id=executor_id)
        elif executor_id == qsub_attribute.UNPARSABLE:
            logger.debug(f"Got response from qsub but did not contain a "
                         f"valid executor_id. Using ({executor_id}), and "
                         f"moving to 'W' state")
            task_instance.register_no_exec_id(executor_id=executor_id)
        elif executor_id:
            task_instance.register_submission_to_batch_executor(
                executor_id, self.heartbeat_interval * self.report_by_buffer)
        else:
            msg = ("Did not receive an executor_id in _create_task_instance")
            logger.error(msg)
            shared_requester.send_request(
                app_route="/error_logger",
                message={"traceback": msg},
                request_type="post")

        return task_instance

    def _get_tasks_queued_for_instantiation(self) -> List[ExecutorTask]:
        app_route = f"/workflow/{self.workflow_id}/queued_tasks/{self.n_queued_tasks}"
        rc, response = self.requester.send_request(
            app_route=app_route,
            message={},
            request_type='get')
        if rc != StatusCodes.OK:
            logger.error(f"error in {app_route}")
            tasks: List[ExecutorTask] = []
        else:
            tasks = [
                ExecutorTask.from_wire(t, self.executor.__class__.__name__)
                for t in response['task_dcts']]

        return tasks
