from builtins import str
import _thread
import logging
from time import sleep
import threading
import traceback

from jobmon.client import shared_requester, client_config
from jobmon.client.swarm.job_management.executor_job import ExecutorJob
from jobmon.client.swarm.job_management.executor_job_instance import (
    ExecutorJobInstance)
from jobmon.client.swarm.executors.sequential import SequentialExecutor
from jobmon.models.attributes.constants import qsub_attribute

logger = logging.getLogger(__name__)


class JobInstanceFactory(object):

    def __init__(self, dag_id, executor=None, interrupt_on_error=True,
                 n_queued_jobs=1000, stop_event=None,
                 requester=shared_requester):
        """The JobInstanceFactory is in charge of queueing jobs and creating
        job_instances, in order to get the jobs from merely Task objects to
        running code.

        Args:
            dag_id (int): the id for the dag to run
            executor (obj, default SequentialExecutor): obj of type
            SequentialExecutor, DummyExecutor or SGEExecutor
            interrupt_on_error (bool, default True): whether or not to
                interrupt the thread if there's an error
            n_queued_jobs (int): number of queued jobs to return and send to
                be instantiated
            stop_event (obj, default None): Object of type threading.Event
        """
        self.dag_id = dag_id
        self.requester = requester
        self.interrupt_on_error = interrupt_on_error
        self.n_queued_jobs = n_queued_jobs
        self.next_report_increment = client_config.heartbeat_interval * \
            client_config.report_by_buffer

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

    def instantiate_queued_jobs_periodically(self, poll_interval=3):
        """Running in a thread, this function allows the JobInstanceFactory to
        periodically get all jobs that are ready and queue them for
        instantiation

        Args:
            poll_interval (int): how often you want this function to poll for
            newly ready jobs
        """
        logger.info("Polling for and instantiating queued jobs at {}s "
                    "intervals".format(poll_interval))
        while True and not self._stop_event.is_set():
            try:
                logger.debug("Queuing at interval {}s".format(poll_interval))
                self.instantiate_queued_jobs()
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
                    f"in instantiate_queued_jobs_periodically: \n{stack}")
                shared_requester.send_request(
                    app_route="/error_logger",
                    message={"traceback": msg},
                    request_type="post")
                if self.interrupt_on_error:
                    _thread.interrupt_main()
                    self._stop_event.set()
                else:
                    raise

    def instantiate_queued_jobs(self):
        """Pull all jobs that are ready, create job instances for them, and
        thereby run them
        """
        logger.debug("JIF: Instantiating Queued Jobs")
        jobs = self._get_jobs_queued_for_instantiation()
        logger.debug("JIF: Found {} Queued Jobs".format(len(jobs)))
        job_instance_ids = []
        for job in jobs:
            job_instance_id, _ = self._create_job_instance(job)
            job_instance_ids.append(job_instance_id)

        logger.debug("JIF: Returning {} Instantiated Jobs".format(
            len(job_instance_ids)))
        return job_instance_ids

    def set_executor(self, executor):
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

    def _create_job_instance(self, job: ExecutorJob):
        """
        Creates a JobInstance based on the parameters of Job and tells the
        JobStateManager to react accordingly.

        Args:
            job (Job): A Job that we want to execute
        """
        # try:
        job_instance = ExecutorJobInstance.register_job_instance(
            job, self.executor)
        # except Exception as e:
        #     logger.error(e)
        #     stack = traceback.format_exc()
        #     msg = (
        #         f"Error while creating job instances {self.__class__.__name__}"
        #         f", {str(self)} while submitting jid {job.job_id}: \n{stack}")
        #     shared_requester.send_request(
        #         app_route="/error_logger",
        #         message={"traceback": stack},
        #         request_type="post")
        #     # we can't do anything more at this point so must return Nones
        #     return (None, None)

        logger.debug("Executing {}".format(job.command))

        # TODO: consider pushing the execute command down into
        # ExecutorJobInstance

        # TODO: unify qsub IDS to be meaningful across executor types

        # The following call will always return a value.
        # It catches exceptions internally and returns ERROR_SGE_JID
        print(job.command)
        executor_id = self.executor.execute(
            job, job_instance_id=job_instance.job_instance_id)
        print(executor_id)
        if executor_id == qsub_attribute.NO_EXEC_ID:
            if executor_id == qsub_attribute.NO_EXEC_ID:
                logger.debug(f"Received {qsub_attribute.NO_EXEC_ID} meaning "
                             f"the job did not qsub properly, moving "
                             f"to 'W' state")
                job_instance.register_no_exec_id(
                    executor_id=qsub_attribute.NO_EXEC_ID)
        elif executor_id == qsub_attribute.UNPARSABLE:
            logger.debug(f"Got response from qsub but did not contain a "
                         f"valid job id "
                         f"({qsub_attribute.UNPARSABLE}), "
                         f"moving to 'W' state")
            job_instance.register_no_exec_id(
                executor_id=qsub_attribute.UNPARSABLE)
        elif executor_id:
            job_instance.register_submission_to_batch_executor(
                executor_id, self.next_report_increment)
        else:
            msg = ("Did not receive an executor_id in _create_job_instance")
            logger.error(msg)
            shared_requester.send_request(
                app_route="/error_logger",
                message={"traceback": msg},
                request_type="post")

        return job_instance, executor_id

    def _get_jobs_queued_for_instantiation(self):
        try:
            rc, response = self.requester.send_request(
                app_route=(
                    f'/dag/{self.dag_id}/queued_jobs/{self.n_queued_jobs}'),
                message={},
                request_type='get')
            jobs = [ExecutorJob.from_wire(j) for j in response['job_dcts']]

        except TypeError:
            # Ignore, it indicates that there are no jobs queued
            jobs = []

        return jobs
