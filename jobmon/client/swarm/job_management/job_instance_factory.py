from builtins import str
import _thread
import logging
import threading
from time import sleep

from jobmon.client import shared_requester, client_config
from jobmon.client.swarm.executors.sequential import SequentialExecutor
from jobmon.models.job import Job
from jobmon.models.job_status import JobStatus
from jobmon.models.job_instance import JobInstance

logger = logging.getLogger(__name__)


class JobInstanceFactory(object):

    def __init__(self, dag_id, executor=None, interrupt_on_error=True,
                 n_queued_jobs=1000, stop_event=None,
                 requester=shared_requester,
                 report_by_transitition_buffer=client_config.heartbeat_interval
                 ):
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
            report_by_transitition_buffer
                (int, default client_config.heartbeat_interval): How long to
                wait for a job instance to report after it is moved into
                SUBMITTED_TO_BATCH_EXECUTOR state
        """
        self.dag_id = dag_id
        self.requester = requester
        self.interrupt_on_error = interrupt_on_error
        self.n_queued_jobs = n_queued_jobs
        self.report_by_transitition_buffer = report_by_transitition_buffer

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
                # Also write to stdout because this is a serious problem
                print(msg)
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

    def _create_job_instance(self, job):
        """
        Creates a JobInstance based on the parameters of Job and tells the
        JobStateManager to react accordingly.

        Args:
            job (Job): A Job that we want to execute
        """
        try:
            job_instance = JobInstance(job=job)
            executor_class = self.executor.__class__
            job_instance.register(self.requester, executor_class.__name__)
        except Exception as e:
            logger.error(e)
        logger.debug("Executing {}".format(job.command))
        executor_id = self.executor.execute(job_instance=job_instance)
        if executor_id:
            job_instance.assign_executor_id(self.requester, executor_id,
                                            self.report_by_transitition_buffer)
        return job_instance, executor_id

    def _get_jobs_queued_for_instantiation(self):
        try:
            rc, response = self.requester.send_request(
                app_route=f'/dag/{self.dag_id}/queued_jobs/{self.n_queued_jobs}',
                message={},
                request_type='get')
            jobs = [Job.from_wire(j) for j in response['job_dcts']]

        except TypeError:
            # Ignore, it indicates that there are no jobs queued
            jobs = []

        return jobs

    def _register_job_instance(self, job, executor_type):
        rc, response = self.requester.send_request(
            app_route='/job_instance',
            message={'job_id': str(job.job_id),
                     'executor_type': executor_type},
            request_type='post')
        job_instance_id = response['job_instance_id']
        return job_instance_id

    def _register_submission_to_batch_executor(self, job_instance_id,
                                               executor_id):
        self.requester.send_request(
            app_route=('/job_instance/{}/log_executor_id'
                       .format(job_instance_id)),
            message={'executor_id': str(executor_id)},
            request_type='put')
