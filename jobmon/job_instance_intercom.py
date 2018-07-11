import logging
import os
import socket

from jobmon.requester import Requester
from jobmon.config import config

if os.getenv("SGE_CLUSTER_NAME"):
    from jobmon import sge


logger = logging.getLogger(__name__)


class JobInstanceIntercom(object):

    def __init__(self, job_instance_id, process_group_id, hostname):
        self.job_instance_id = job_instance_id
        self.process_group_id = process_group_id
        self.hostname = hostname
        self.requester = Requester(config.jsm_port, host=hostname)
        with open('/homes/cpinho/forked_jobmon/jii.txt', 'w') as f:
            f.write("in jii, requester made")
        logger.debug("Instantiated JobInstanceIntercom")

    def log_done(self):
        return self.requester.send_request(
            app_route='/log_done',
            message={'job_instance_id': str(self.job_instance_id)},
            request_type='post')

    def log_error(self, error_message):
        return self.requester.send_request(
            app_route='/log_error',
            message={'job_instance_id': str(self.job_instance_id),
                     'error_message': error_message},
            request_type='post')

    def log_job_stats(self, job_id):
        if job_id:
            self.usage = sge.qstat_usage([job_id])[int(job_id)]
            dbukeys = ['usage_str', 'wallclock', 'maxvmem', 'cpu',
                       'io']
            msg = {k: self.usage[k] for k in dbukeys
                   if k in self.usage.keys()}
            msg.update({
                'job_instance_id': [self.job_instance_id]})
            return self.requester.send_request(app_route='/log_job_stats',
                                               message=msg,
                                               request_type='post')
        else:
            logger.debug("In log_job_stats: job_id is None")
            return False

    def log_running(self):
        return self.requester.send_request(
            app_route='/log_running',
            message={'job_instance_id': str(self.job_instance_id),
                     'nodename': socket.gethostname(),
                     'process_group_id': str(self.process_group_id)},
            request_type='post')
