from http import HTTPStatus as StatusCodes
import threading
from time import sleep
from typing import Optional, List
import _thread

from jobmon.client import shared_requester, client_config
from jobmon.client.requester import Requester
from jobmon.client.swarm.executors import Executor
from jobmon.client.swarm.job_management.executor_job import ExecutorJob
from jobmon.client.swarm.job_management.executor_job_instance import (
    ExecutorJobInstance)
from jobmon.client.swarm.executors.sequential import SequentialExecutor
from jobmon.models.attributes.constants import qsub_attribute
from jobmon.client.client_logging import ClientLogging as logging

logger = logging.getLogger(__name__)


class JobInstanceFactory(object):

    def __init__(self,
                 dag_id: int,
                 executor: Optional[Executor] = None,
                 n_queued_jobs: int = 1000,
                 stop_event: Optional[threading.Event] = None,
                 requester: Requester = shared_requester):
        """The JobInstanceFactory is in charge of queueing jobs and creating
        job_instances, in order to get the jobs from merely Task objects to
        running code.

        Args:
            dag_id: the id for the dag to run
            executor: executor to use w/ this factory. SequentialExecutor,
                DummyExecutor or SGEExecutor. Default SequentialExecutor
            n_queued_jobs: number of queued jobs to return and send to be
                instantiated. default 1000
            stop_event: Object of type threading.Event
            requester: requester for communicating with central services
        """

        self.dag_id = dag_id
        self.requester = requester
        self.n_queued_jobs = n_queued_jobs
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

    def instantiate_queued_jobs_periodically(self, poll_interval: int = 3
                                             ) -> None:
        """Running in a thread, this function allows the JobInstanceFactory to
        periodically get all jobs that are ready and queue them for
        instantiation

        Args:
            poll_interval (int, optional): how often you want this function to
                poll for newly ready jobs
        """
        logger.info(
            f"Polling for and instantiating queued jobs at {poll_interval}s "
            "intervals")
        while True and not self._stop_event.is_set():
            try:
                logger.info(f"Queuing jobs for dag_id {self.dag_id} at"
                            f" interval: {poll_interval}s")
                self.instantiate_queued_jobs()
                sleep(poll_interval)
            except Exception as e:
                msg = f"About to raise Keyboard Interrupt signal {e}"
                logger.error(msg)
                _thread.interrupt_main()
                self._stop_event.set()
                raise

    def instantiate_queued_jobs(self) -> List[int]:
        """Pull all jobs that are ready, create job instances for them, and
        thereby run them
        """
        jobs = self._get_jobs_queued_for_instantiation()
        logger.info(f"Found {len(jobs)} jobs queued for instantiation")
        job_instance_ids = []
        for job in jobs:
            job_instance = self._create_job_instance(job)
            if job_instance:
                job_instance_ids.append(job_instance.job_instance_id)
        logger.debug(f"Returning {len(job_instance_ids)} Instantiated Jobs")
        return job_instance_ids

    def set_executor(self, executor: Executor) -> None:
        """
        Sets the executor that will be used for all jobs queued downstream
        of the set event.

        Args:
            executor (callable): Any callable that takes a Job and returns
                either None or an Int. If Int is returned, this is assumed
                to be the JobInstances executor_id, and will be registered
                with the JobStateManager as such.
        """
        # TODO: Add some validation that the passed object is callable and
        # and follows the args/returns requirements of an executor. Potentially
        # resuscitate the Executor abstract base class.
        self.executor = executor

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
            msg = (
                f"Error while creating job instances for dag_id {self.dag_id}"
                f". Submitting jid {job.job_id} caused the following error:"
                f"\n{e}")
            logger.error(msg)
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
                executor_id, self.heartbeat_interval * self.report_by_buffer)
        else:
            msg = ("Did not receive an executor_id in _create_job_instance")
            logger.error(msg)
            shared_requester.send_request(
                app_route="/error_logger",
                message={"traceback": msg},
                request_type="post")

        return job_instance

    def _get_jobs_queued_for_instantiation(self) -> List[ExecutorJob]:
        app_route = f"/dag/{self.dag_id}/queued_jobs/{self.n_queued_jobs}"
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
