from http import HTTPStatus as StatusCodes
import logging
import threading
import time
import traceback
from typing import Optional, List

from jobmon.requester import Requester, shared_requester

# TODO, split configs?
from jobmon.client_config import client_config

from jobmon.models.attributes.constants import qsub_attribute

from jobmon.execution.strategies import Executor
from jobmon.execution.scheduler.executor_job import ExecutorJob
from jobmon.execution.scheduler.executor_job_instance import \
    ExecutorJobInstance


logger = logging.getLogger(__name__)


class JobInstanceScheduler:

    def __init__(self, executor: Executor,
                 requester: Requester = shared_requester):
        self.executor = executor
        self.requester = requester
        logger.info(f"scheduler communicating at {self.requester.url}")

        # reconciliation loop is always running
        self._stop_event = threading.Event()
        self.reconciliation_proc = threading.Thread(
            target=self._reconcile_forever,
            args=(client_config.reconciliation_interval,))
        self.reconciliation_proc.daemon = True

    def start(self):
        # TODO: add an is started attribute to executor
        self.executor.start()
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
                self.instantiate_queued_jobs()
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
        self._account_for_lost_job_instances()

    def instantiate_queued_jobs(self) -> List[int]:
        """Pull all jobs that are ready, create job instances for them, and
        thereby run them
        """
        logger.debug("JIF: Instantiating Queued Jobs")
        jobs = self._get_jobs_queued_for_instantiation()
        logger.debug("JIF: Found {} Queued Jobs".format(len(jobs)))
        job_instance_ids = []
        for job in jobs:
            job_instance = self._create_job_instance(job)
            if job_instance:
                job_instance_ids.append(job_instance.job_instance_id)

        logger.debug("JIF: Returning {} Instantiated Jobs".format(
            len(job_instance_ids)))
        return job_instance_ids

    def _reconcile_forever(self, reconciliation_interval=90):
        while True:
            self.reconcile()

            # check if the stop event is set during idle time
            if self._stop_event.wait(timeout=reconciliation_interval):
                break

    def _create_job_instance(self, job: ExecutorJob
                             ) -> Optional[ExecutorJobInstance]:
        """
        Creates a JobInstance based on the parameters of Job and tells the
        JobStateManager to react accordingly.

        Args:
            job (ExecutorJob): A Job that we want to execute
        """
        try:
            job_instance = ExecutorJobInstance.register_job_instance(
                job.job_id, self.executor)
        except Exception as e:
            logger.error(e)
            stack = traceback.format_exc()
            msg = (
                f"Error while creating job instances {self.__class__.__name__}"
                f", {str(self)} while submitting jid {job.job_id}: \n{stack}")
            shared_requester.send_request(
                app_route="/error_logger",
                message={"traceback": stack},
                request_type="post")
            # we can't do anything more at this point so must return None
            return None

        logger.debug("Executing {}".format(job.command))

        # TODO: unify qsub IDS to be meaningful across executor types

        command = job_instance.executor.build_wrapped_command(
            command=job.command,
            job_instance_id=job_instance.job_instance_id,
            last_nodename=job.last_nodename,
            last_process_group_id=job.last_process_group_id)
        # The following call will always return a value.
        # It catches exceptions internally and returns ERROR_SGE_JID
        logger.debug(
            "Using the following parameters in execution: "
            f"{job.executor_parameters}")
        executor_id = job_instance.executor.execute(
            command=command,
            name=job.name,
            executor_parameters=job.executor_parameters)
        if executor_id == qsub_attribute.NO_EXEC_ID:
            if executor_id == qsub_attribute.NO_EXEC_ID:
                logger.debug(f"Received {qsub_attribute.NO_EXEC_ID} meaning "
                             f"the job did not qsub properly, moving "
                             f"to 'W' state")
                job_instance.register_no_exec_id(executor_id=executor_id)
        elif executor_id == qsub_attribute.UNPARSABLE:
            logger.debug(f"Got response from qsub but did not contain a "
                         f"valid executor_id. Using ({executor_id}), and "
                         f"moving to 'W' state")
            job_instance.register_no_exec_id(executor_id=executor_id)
        elif executor_id:
            job_instance.register_submission_to_batch_executor(
                executor_id,
                (
                    client_config.heartbeat_interval *
                    client_config.report_by_buffer))
        else:
            msg = ("Did not receive an executor_id in _create_job_instance")
            logger.error(msg)
            shared_requester.send_request(
                app_route="/error_logger",
                message={"traceback": msg},
                request_type="post")

        return job_instance

    def _get_jobs_queued_for_instantiation(self) -> List[ExecutorJob]:
        app_route = f"/queued_jobs/1000"
        rc, response = self.requester.send_request(
            app_route=app_route,
            message={},
            request_type='get')
        if rc != StatusCodes.OK:
            logger.error(f"error in {app_route}")
            jobs: List[ExecutorJob] = []
        else:
            jobs = [
                ExecutorJob.from_wire(j, self.executor.__class__.__name__)
                for j in response['job_dcts']]

        return jobs

    def _log_executor_report_by(self) -> None:
        next_report_increment = (
            client_config.heartbeat_interval *
            client_config.report_by_buffer)
        try:
            # TODO, this method needs to be user agnostic
            # qstat for pending jobs or running jobs
            actual = self.executor.get_actual_submitted_or_running()
        except NotImplementedError:
            logger.warning(
                f"{self.executor.__class__.__name__} does not implement "
                "reconciliation methods. If a job instance does not "
                "register a heartbeat from a worker process in "
                f"{next_report_increment}s the job instance will be "
                "moved to error state.")
            actual = []
        rc, response = self.requester.send_request(
            app_route=f'/log_executor_report_by',
            message={'executor_ids': actual,
                     'next_report_increment': next_report_increment},
            request_type='post')

    def _account_for_lost_job_instances(self) -> None:
        rc, response = self.requester.send_request(
            app_route='/get_suspicious_job_instances',
            message={},
            request_type='get')
        if rc != StatusCodes.OK:
            lost_job_instances: List[ExecutorJobInstance] = []
        else:
            lost_job_instances = [
                ExecutorJobInstance.from_wire(ji, self.executor)
                for ji in response["job_instances"]
            ]
        for executor_job_instance in lost_job_instances:
            executor_job_instance.log_error()
