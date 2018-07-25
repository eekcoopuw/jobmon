import logging
import socket

from jobmon.requester import Requester
from jobmon.config import config


logger = logging.getLogger(__name__)


class JobInstanceIntercom(object):

    def __init__(self, job_instance_id, executor_class, process_group_id,
                 hostname):
        self.job_instance_id = job_instance_id
        self.process_group_id = process_group_id
        self.hostname = hostname
        self.requester = Requester(config.jsm_port, host=hostname)
        self.executor_class = executor_class
        self.executor = executor_class()
        logger.debug("Instantiated JobInstanceIntercom")

    def log_done(self):
        rc, _ = self.requester.send_request(
            app_route='/log_done',
            message={'job_instance_id': str(self.job_instance_id)},
            request_type='post')
        return rc

    def log_error(self, error_message):
        rc, _ = self.requester.send_request(
            app_route='/log_error',
            message={'job_instance_id': str(self.job_instance_id),
                     'error_message': error_message},
            request_type='post')
        return rc

    def log_job_stats(self):
        try:
            usage = self.executor.get_usage_stats()
            dbukeys = ['usage_str', 'wallclock', 'maxvmem', 'cpu', 'io']
            kwargs = {k: usage[k] for k in dbukeys if k in usage.keys()}
            msg = {
                'action': 'log_usage',
                'args': [self.job_instance_id],
                'kwargs': kwargs}
            return self.requester.send_request(msg)
        except NotImplementedError:
            logger.warning("Usage stats not available for {} "
                           "executors".format(self.executor_class))

    def log_running(self):
        rc, _ = self.requester.send_request(
            app_route='/log_running',
            message={'job_instance_id': str(self.job_instance_id),
                     'nodename': socket.gethostname(),
                     'process_group_id': str(self.process_group_id)},
            request_type='post')
        return rc
