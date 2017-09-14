from jobmon import config
from jobmon.models import Job, JobStatus
from jobmon.database import Session
from jobmon.requester import Requester


class JobInstanceFactory(object):

    def __init__(self, dag_id):
        self.dag_id = dag_id
        self.requester = Requester(config.jm_conn_obj)

    def flush_jobs_queued_for_instantiation(self):
        session = Session()
        jobs = self._get_jobs_queued_for_instantiation(session)
        for job in jobs:
            self._create_job_instance(job)
        session.close()

    def _create_job_instance(self, job):
        # qsub
        import random
        job_instance_id = random.randint(1, 1e7)
        self._register_job_instance(job, job_instance_id)

    def _get_jobs_queued_for_instantiation(self, session):
        jobs = session.query(Job).filter_by(
            status=JobStatus.QUEUED_FOR_INSTANTIATION,
            dag_id=self.dag_id).all()
        return jobs

    def _register_job_instance(self, job, job_instance_id):
        self.requester.send_request({
            'action': 'add_job_instance',
            'kwargs': {'job_id': job.job_id,
                       'job_instance_id': job_instance_id}
        })
