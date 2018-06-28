import logging
import _thread
from time import sleep

from zmq.error import ZMQError

from jobmon.config import config
from jobmon.command_context import build_qsub, build_wrapped_command
from jobmon.models import Job
from jobmon.requester import Requester


logger = logging.getLogger(__name__)


def execute_sequentially(job, job_instance_id):
    import subprocess
    try:
        cmd = build_wrapped_command(job, job_instance_id)
        logger.debug(cmd)
        subprocess.check_output(cmd, shell=True)
    except Exception as e:
        logger.error(e)
    return None


def execute_sge(job, job_instance_id, stderr=None, stdout=None, project=None):
    from jobmon import sge
    try:
        import subprocess
        qsub_cmd = build_qsub(job, job_instance_id, stderr, stdout, project)
        resp = subprocess.check_output(qsub_cmd, shell=True)
        idx = resp.split().index(b'job')
        sge_jid = int(resp.split()[idx + 1])

        # TODO: FIX THIS ... DRMAA QSUB METHOD IS FAILING FOR SOME REASON,
        # NEED TO INVESTIGATE THE JOBTYPE ASSUMPTIONS. RESORTING TO
        # BASIC COMMAND-LINE QSUB FOR NOW
        # sge_jid = sge.qsub(cmd, jobname=job.name, jobtype='plain')

        return sge_jid
    except Exception as e:
        logger.error(e)
        return -99999


def execute_batch_dummy(job, job_instance_id):
    import random
    # qsub
    job_instance_id = random.randint(1, 1e7)
    return job_instance_id


class JobInstanceFactory(object):

    def __init__(self, dag_id, executor=None, interrupt_on_error=True):
        self.dag_id = dag_id
        self.jsm_req = Requester(config.jm_port)
        self.jqs_req = Requester(config.jqs_port)
        self.interrupt_on_error = interrupt_on_error

        if executor:
            self.set_executor(executor)
        else:
            self.set_executor(execute_sequentially)

    def instantiate_queued_jobs_periodically(self, poll_interval=1):
        logger.info("Polling for and instantiating queued jobs at {}s "
                    "intervals".format(poll_interval))
        while True:
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
                logger.error(e)
                if self.interrupt_on_error:
                    _thread.interrupt_main()
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
            job_instance_id = self._register_job_instance(
                job, self.executor.__name__)
        except Exception as e:
            logger.error(e)
        logger.debug("Executing {}".format(job.command))
        executor_id = self.executor(job, job_instance_id)
        if executor_id:
            self._register_submission_to_batch_executor(job_instance_id,
                                                        executor_id)
        return job_instance_id, executor_id

    def _get_jobs_queued_for_instantiation(self):
        try:
            rc, response = self.jqs_req.send_request(
                app_route='/get_queued_for_instantiation',
                message={'dag_id': self.dag_id},
                request_type='get')
            jobs = [Job.from_wire(j) for j in response['job_dcts']]
        except TypeError:
            # Ignore if there are no jobs queued
            jobs = []
        return jobs

    def _register_job_instance(self, job, executor_type):
        rc, response = self.jsm_req.send_request(
            app_route='/add_job_instance',
            message={'job_id': job.job_id,
                     'executor_type': executor_type},
            request_type='post')
        job_instance_id = response['job_instance_id']
        return job_instance_id

    def _register_submission_to_batch_executor(self, job_instance_id,
                                               executor_id):
        self.jsm_req.send_request(
            app_route='/log_executor_id',
            message={'job_instance_id': job_instance_id,
                     'executor_id': executor_id},
            request_type='post')
