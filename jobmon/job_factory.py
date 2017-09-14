import logging
import time

from jobmon import config
from jobmon import requester
from jobmon.database import Session
from jobmon.models import Job, JobStatus

logger = logging.getLogger(__name__)


class JobFactory(object):

    def __init__(self, dag_id):
        self.dag_id = dag_id
        self.requester = requester.Requester(config.jm_conn_obj)

    def create_job(self, runfile, jobname, parameters=None):
        rc, job_id = self.requester.send_request({
            'action': 'add_job',
            'kwargs': {'dag_id': self.dag_id,
                       'name': jobname,
                       'runfile': runfile,
                       'job_args': parameters}
        })
        return job_id

    def queue_job(self, job_id):
        rc = self.requester.send_request({
            'action': 'queue_job',
            'kwargs': {'job_id': job_id}
        })
        return rc

    def block_till_done_or_error(self, poll_interval=10):
        logger.info("Blocking, poll interval = {}".format(poll_interval))

        while True:
            session = Session()
            active_jobs = self._get_instantiated_not_done_not_fatal(session)
            session.close()
            if len(active_jobs) == 0:
                break
            logger.debug("{} active jobs. Waiting {} seconds...".format(
                len(active_jobs), poll_interval))
            time.sleep(poll_interval)

    def _get_done(self, session):
        jobs = session.query(Job).filter(
            Job.status == JobStatus.DONE,
            Job.dag_id == self.dag_id).all()
        return jobs

    def _get_fatal(self, session):
        jobs = session.query(Job).filter(
            Job.status == JobStatus.ERROR_FATAL,
            Job.dag_id == self.dag_id).all()
        return jobs

    def _get_instantiated_not_done_not_fatal(self, session):
        jobs = session.query(Job).filter(
            Job.status != JobStatus.REGISTERED,
            Job.status != JobStatus.DONE,
            Job.status != JobStatus.ERROR_FATAL,
            Job.dag_id == self.dag_id).all()
        return jobs
