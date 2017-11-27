import logging
import os

from jobmon.requester import Requester

if os.getenv("SGE_CLUSTER_NAME"):
    from jobmon import sge


logger = logging.getLogger(__name__)


class JobInstanceIntercom(object):

    def __init__(self, job_instance_id, jm_rep_cc=None):
        self.job_instance_id = job_instance_id
        self.requester = Requester(jm_rep_cc)

    def log_done(self):
        return self.requester.send_request({
            'action': 'log_done',
            'kwargs': {'job_instance_id': self.job_instance_id}
        })

    def log_error(self, error_message):
        return self.requester.send_request({
            'action': 'log_error',
            'kwargs': {'job_instance_id': self.job_instance_id,
                       'error_message': error_message}
        })

    def log_job_stats(self):
        if os.getenv("JOB_ID"):
            job_id = os.getenv("JOB_ID")
            self.usage = sge.qstat_usage(
                job_id)
            dbukeys = ['usage_str', 'wallclock', 'maxvmem', 'cpu', 'io']
            kwargs = {k: self.usage[k] for k in dbukeys
                      if k in self.usage.keys()}
            msg = {
                'action': 'log_usage',
                'args': [self.job_instance_id],
                'kwargs': kwargs}
            return self.requester.send_request(msg)
        else:
            return False

    def log_running(self):
        return self.requester.send_request({
            'action': 'log_running',
            'kwargs': {'job_instance_id': self.job_instance_id}
        })
