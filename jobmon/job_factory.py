import json
import logging

from jobmon import requester
from jobmon.config import config

logger = logging.getLogger(__name__)


class JobFactory(object):

    def __init__(self, dag_id):
        self.dag_id = dag_id
        self.requester = requester.Requester(config.jm_rep_conn)

    def create_job(self, command, jobname, job_hash, slots=1, mem_free=2,
                   max_attempts=1, max_runtime=None, project=None,
                   context_args=None):
        """
        Create a job entry in the database.

        Args:
            command (str): the command to run
            jobname (str): name of the job
            job_hash (str): hash of the job
            slots (int): Number of slots to request from SGE
            mem_free (int): Number of GB of memory to request from SGE
            max_attmpets (int): Maximum # of attempts before sending the job to
                ERROR_FATAL state
            max_runtime (int): Maximum runtime of a single job_instance before
                killing and marking that instance as failed
            project (str): name of the project to run the jobs under
            context_args (dict): Additional arguments to be sent to the command
                builders
        """
        if not context_args:
            context_args = json.dumps({})
        else:
            context_args = json.dumps(context_args)
        rc, job_id = self.requester.send_request({
            'action': 'add_job',
            'kwargs': {'dag_id': self.dag_id,
                       'name': jobname,
                       'job_hash': job_hash,
                       'command': command,
                       'context_args': context_args,
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
