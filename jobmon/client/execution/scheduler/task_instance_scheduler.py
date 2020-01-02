from http import HTTPStatus as StatusCodes
# from multiprocessing import Process, Event
import threading
import time
from typing import Optional, List

from jobmon.client import ClientLogging as logging
from jobmon.client.execution.strategies.base import Executor
from jobmon.client.execution.scheduler.execution_config import ExecutionConfig
from jobmon.client.execution.scheduler.executor_task import ExecutorTask
from jobmon.client.execution.scheduler.executor_task_instance import \
    ExecutorTaskInstance
from jobmon.client.requests.requester import Requester
from jobmon.client.requests.connection_config import ConnectionConfig
from jobmon.exceptions import InvalidResponse
from jobmon.models.attributes.constants import qsub_attribute


logger = logging.getLogger(__name__)


class TaskInstanceScheduler:

    def __init__(self, workflow_id: int, workflow_run_id: int,
                 executor: Executor,
                 config: ExecutionConfig = ExecutionConfig.from_defaults(),
                 requester: Optional[Requester] = None):

        #
        self.workflow_id = workflow_id
        self.workflow_run_id = workflow_run_id
        self.executor = executor
        self.config = config
        if requester is None:
            requester = Requester(ConnectionConfig.from_defaults().url, logger)
        self.requester = requester

        logger.info(f"scheduler communicating at {self.requester.url}")

        # reconciliation loop is always running
        self._stop_event = threading.Event()
        self.reconciliation_proc = threading.Thread(
            target=self._reconcile_forever,
            args=(config.reconciliation_interval,))
        self.reconciliation_proc.daemon = True

    def start(self):
        if not self.executor.started:
            self.executor.start(self.config.jobmon_command)
        self.reconciliation_proc.start()

    def stop(self):
        self._stop_event.set()
        self.executor.stop()

    def run_scheduler(self):
        # start up the executor if need be. start reconciliation proc
        self.start()

        # instantiation loop
        keep_scheduling = True
        while keep_scheduling:
            try:
                self.instantiate_queued_tasks()
            except KeyboardInterrupt:
                confirm = input("Are you sure you want to exit (y/n): ")
                confirm = confirm.lower().strip()
                if confirm == "y":
                    keep_scheduling = False
                    self.stop()
                else:
                    print("Continuing to schedule...")

            if keep_scheduling:
                time.sleep(3)

    def reconcile(self):
        self._log_executor_report_by()
        self._account_for_lost_task_instances()

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
                task_instance_ids.append(task_instance.task_instance_id)

        logger.debug("TIF: Returning {} Instantiated tasks".format(
            len(task_instance_ids)))
        return task_instance_ids

    def _reconcile_forever(self, reconciliation_interval=90):
        while True:
            self.reconcile()

            # check if the stop event is set during idle time
            if self._stop_event.wait(timeout=reconciliation_interval):
                break

    def _create_task_instance(self, task: ExecutorTask
                              ) -> Optional[ExecutorTaskInstance]:
        """
        Creates a TaskInstance based on the parameters of Task and tells the
        TaskStateManager to react accordingly.

        Args:
            task (ExecutorTask): A Task that we want to execute
        """
        try:
            task_instance = ExecutorTaskInstance.register_task_instance(
                task.task_id, self.workflow_run_id, self.executor,
                self.requester)
        except InvalidResponse:
            # we can't do anything more at this point so must return None.
            # error is already logged if invalid response
            return None
        except Exception as e:
            # we can't do anything more at this point so must return None
            logger.error(e)
            return None

        logger.debug("Executing {}".format(task.command))

        # TODO: unify qsub IDS to be meaningful across executor types
        # TODO: remove last_nodename and last_process_group_id
        command = task_instance.executor.build_wrapped_command(
            command=task.command,
            task_instance_id=task_instance.task_instance_id,
            heartbeat_interval=self.config.heartbeat_interval,
            report_by_buffer=self.config.report_by_buffer,
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
            logger.debug(f"Received {executor_id} meaning "
                         f"the task did not qsub properly, moving "
                         f"to 'W' state")
            task_instance.register_no_executor_id(executor_id=executor_id)
        elif executor_id == qsub_attribute.UNPARSABLE:
            logger.debug(f"Got response from qsub but did not contain a "
                         f"valid executor_id. Using ({executor_id}), and "
                         f"moving to 'W' state")
            task_instance.register_no_executor_id(executor_id=executor_id)
        elif executor_id:
            task_instance.register_submission_to_batch_executor(
                executor_id,
                self.config.heartbeat_interval * self.config.report_by_buffer)
        else:
            msg = ("Did not receive an executor_id in _create_task_instance")
            logger.error(msg)

        return task_instance

    def _get_tasks_queued_for_instantiation(self) -> List[ExecutorTask]:
        app_route = f"/workflow/{self.workflow_id}/queued_tasks/1000"
        rc, response = self.requester.send_request(
            app_route=app_route,
            message={"n_queued": self.config.n_queued},
            request_type='get')
        if rc != StatusCodes.OK:
            logger.error(f"error in {app_route}. Received response code {rc}. "
                         f"Response was {response}")
            tasks: List[ExecutorTask] = []
        else:
            tasks = [
                ExecutorTask.from_wire(j, self.executor.__class__.__name__,
                                       self.requester)
                for j in response['task_dcts']]

        return tasks

    def _log_executor_report_by(self) -> None:
        next_report_increment = (
            self.config.heartbeat_interval * self.config.report_by_buffer)
        try:
            actual = self.executor.get_actual_submitted_or_running()
        except NotImplementedError:
            logger.warning(
                f"{self.executor.__class__.__name__} does not implement "
                "reconciliation methods. If a job instance does not "
                "register a heartbeat from a worker process in "
                f"{next_report_increment}s the job instance will be "
                "moved to error state.")
            actual = []

        if actual:
            app_route = (
                f'/workflow_run/{self.workflow_run_id}/log_executor_report_by')
            rc, response = self.requester.send_request(
                app_route=app_route,
                message={'executor_ids': actual,
                         'next_report_increment': next_report_increment},
                request_type='post')
            if rc != StatusCodes.OK:
                logger.error(f"error in {app_route}. Received response code "
                             f"{rc}. Response was {response}")

    def _account_for_lost_task_instances(self) -> None:
        app_route = (
            f'/workflow_run/{self.workflow_run_id}/'
            'get_suspicious_task_instances')
        rc, response = self.requester.send_request(
            app_route=app_route,
            message={},
            request_type='get')
        if rc != StatusCodes.OK:
            lost_task_instances: List[ExecutorTaskInstance] = []
            logger.error(f"error in {app_route}. Received response code "
                         f"{rc}. Response was {response}")
        else:
            lost_task_instances = [
                ExecutorTaskInstance.from_wire(ti, self.executor,
                                               self.requester)
                for ti in response["task_instances"]
            ]
        for executor_job_instance in lost_task_instances:
            executor_job_instance.log_error()
