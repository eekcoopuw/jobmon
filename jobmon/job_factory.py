import logging

from jobmon import requester
from jobmon.config import config

logger = logging.getLogger(__name__)


class JobFactory(object):

    def __init__(self, dag_id):
        self.dag_id = dag_id
        self.requester = requester.Requester(config.jm_rep_conn)

    def create_job(self, command, jobname, slots=1, mem_free=2, max_attempts=1,
                   project=None, max_runtime=None):
        rc, job_id = self.requester.send_request({
            'action': 'add_job',
            'kwargs': {'dag_id': self.dag_id,
                       'name': jobname,
                       'command': command,
                       'slots': slots,
                       'mem_free': mem_free,
                       'project': project,
                       'max_attempts': max_attempts,
                       'max_runtime': max_runtime}
        })
        return job_id

    def queue_job(self, job_id):
        rc = self.requester.send_request({
            'action': 'queue_job',
            'kwargs': {'job_id': job_id}
        })
        return rc
