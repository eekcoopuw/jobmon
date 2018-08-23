import logging
import threading
import _thread
from time import sleep

from zmq.error import ZMQError

from jobmon.config import config
from jobmon.executors.sequential import SequentialExecutor
from jobmon.models import Job, JobInstance
from jobmon.requester import Requester


logger = logging.getLogger(__name__)


class JobInstanceFactory(object):

    def __init__(self, dag_id, executor=None, interrupt_on_error=True,
                 stop_event=None):
        self.dag_id = dag_id
        self.jsm_req = Requester(config.jm_rep_conn)
        self.jqs_req = Requester(config.jqs_rep_conn)
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
            except ZMQError as e:
                # Tests rely on some funky usage of various REQ/REP pairs
                # across threads, so interrupting here can be problematic...

                # ... since this interrupt is primarily in reponse to potential
                # SGE failures anyways, I'm just going to warn for now on ZMQ
                # errors and save the interrupts for everything else
                logger.warning(e)
            except Exception as e:
                handlers = logger.handlers
                formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
                for h in handlers:
                    h.setFormatter(formatter)
                logger.error("About to throw Keyboard Intterupt {error}".format(error=e))
                print("About to throw Keyboard Interrupt. Error is:  {error}".format(error=e))
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
            rc, jobs = self.jqs_req.send_request({
                'action': 'get_queued_for_instantiation',
                'kwargs': {'dag_id': self.dag_id}
            })
            jobs = [Job.from_wire(j) for j in jobs]
        except TypeError:
            # Ignore if there are no jobs queued
            jobs = []
        return jobs
