from builtins import str
import logging
import threading
import _thread
from time import sleep

from jobmon.config import config
from jobmon.client.executors.sequential import SequentialExecutor
from jobmon.models.job import Job
from jobmon.models.job_instance import JobInstance
from jobmon.client.requester import Requester


logger = logging.getLogger(__name__)


class JobInstanceFactory(object):

    def __init__(self, dag_id, executor=None, interrupt_on_error=True,
                 stop_event=None):
        self.dag_id = dag_id
        self.jsm_req = Requester(config.jsm_port)
        self.jqs_req = Requester(config.jqs_port)
        self.interrupt_on_error = interrupt_on_error

        # At this level, default to using a Sequential Executor if None is
        # provided. End-users shouldn't be interacting at this level (they
        # should be using Workflows or TaskDags), so this state will typically
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

    def instantiate_queued_jobs_periodically(self, poll_interval=1):
        logger.info("Polling for and instantiating queued jobs at {}s "
                    "intervals".format(poll_interval))
        while True and not self._stop_event.is_set():
            try:
                logger.debug("Queuing at interval {}s".format(poll_interval))
                self.instantiate_queued_jobs()
                sleep(poll_interval)
            except Exception as e:
                logger.error(e)
                if self.interrupt_on_error:
                    _thread.interrupt_main()
                    self._stop_event.set()
                else:
                    raise

    def instantiate_queued_jobs(self):
        jobs = self._get_jobs_queued_for_instantiation()
        job_instance_ids = []
        for job in jobs:
            job_instance_id, _ = self._create_job_instance(job)
            job_instance_ids.append(job_instance_id)
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
            job_instance.register(self.jsm_req, executor_class.__name__)
        except Exception as e:
            logger.error(e)
        logger.debug("Executing {}".format(job.command))
        executor_id = self.executor.execute(job_instance)
        if executor_id:
            job_instance.assign_executor_id(self.jsm_req, executor_id)
        return job_instance, executor_id

    def _get_jobs_queued_for_instantiation(self):
        try:
            rc, response = self.jqs_req.send_request(
                app_route='/get_queued',
                message={'dag_id': str(self.dag_id)},
                request_type='get')
            jobs = [Job.from_wire(j) for j in response['job_dcts']]
        except TypeError:
            # Ignore if there are no jobs queued
            jobs = []
        return jobs

    def _register_job_instance(self, job, executor_type):
        rc, response = self.jsm_req.send_request(
            app_route='/add_job_instance',
            message={'job_id': str(job.job_id),
                     'executor_type': executor_type},
            request_type='post')
        job_instance_id = response['job_instance_id']
        return job_instance_id

    def _register_submission_to_batch_executor(self, job_instance_id,
                                               executor_id):
        self.jsm_req.send_request(
            app_route='/log_executor_id',
            message={'job_instance_id': str(job_instance_id),
                     'executor_id': str(executor_id)},
            request_type='post')
