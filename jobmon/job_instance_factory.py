import logging
from time import sleep

from jobmon import config
from jobmon.command_context import build_wrapped_command
from jobmon.database import Session
from jobmon.models import Job, JobStatus
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


def execute_sge(job, job_instance_id):
    from jobmon import sge
    try:
        # cmd = build_wrapped_command(job, job_instance_id)
        cmd = job.command
        sge_jid = sge.qsub(cmd, jobtype='plain', jobname=job.name,
                           stderr="/homes/tomflem", stdout="/homes/tomflem")
        return sge_jid
    except Exception as e:
        logger.error(e)
        return None


def execute_batch_dummy(job, job_instance_id):
    import random
    # qsub
    job_instance_id = random.randint(1, 1e7)
    return job_instance_id


class JobInstanceFactory(object):

    def __init__(self, dag_id, executor=None):
        self.dag_id = dag_id
        self.requester = Requester(config.jm_rep_conn)

        if executor:
            self.set_executor(executor)
        else:
            self.set_executor(execute_sequentially)

    def instantiate_queued_jobs_periodically(self, poll_interval=1):
        logger.info("Polling for and instantiating queued jobs at {}s "
                    "intervals".format(poll_interval))
        while True:
            logger.debug("Queuing at interval {}s".format(poll_interval))
            self.instantiate_queued_jobs()
            sleep(poll_interval)

    def instantiate_queued_jobs(self):
        session = Session()
        jobs = self._get_jobs_queued_for_instantiation(session)
        job_instance_ids = []
        for job in jobs:
            job_instance_id, _ = self._create_job_instance(job)
            job_instance_ids.append(job_instance_id)
        session.close()
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
            logger.info("Got here")
            job_instance_id = self._register_job_instance(
                job, self.executor.__name__)
        except Exception as e:
            logger.info("Got here too")
            logger.error(e)
        logger.info("Got here three")
        logger.debug("Executing {}".format(job.command))
        executor_id = self.executor(job, job_instance_id)
        if executor_id:
            self._register_submission_to_batch_executor(job_instance_id,
                                                        executor_id)
        return job_instance_id, executor_id

    def _get_jobs_queued_for_instantiation(self, session):
        jobs = session.query(Job).filter_by(
            status=JobStatus.QUEUED_FOR_INSTANTIATION,
            dag_id=self.dag_id).all()
        return jobs

    def _register_job_instance(self, job, executor_type):
        rc, job_instance_id = self.requester.send_request({
            'action': 'add_job_instance',
            'kwargs': {'job_id': job.job_id,
                       'executor_type': executor_type}
        })
        return job_instance_id

    def _register_submission_to_batch_executor(self, job_instance_id,
                                               executor_id):
        self.requester.send_request({
            'action': 'log_executor_id',
            'kwargs': {'job_instance_id': job_instance_id,
                       'executor_id': executor_id}
        })
