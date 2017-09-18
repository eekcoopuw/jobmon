import logging
from jobmon import config
from jobmon.requester import Requester


logger = logging.getLogger(__name__)


class JobInstanceIntercom(object):

    def __init__(self, job_instance_id):
        self.job_instance_id = job_instance_id
        self.requester = Requester(config.jm_rep_conn)

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

    def log_running(self):
        return self.requester.send_request({
            'action': 'log_running',
            'kwargs': {'job_instance_id': self.job_instance_id}
        })
