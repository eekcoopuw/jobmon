import logging
import socket

from jobmon.client.requester import Requester
from jobmon.client.the_client_config import get_the_client_config


logger = logging.getLogger(__name__)


class JobInstanceIntercom(object):

    def __init__(self, job_instance_id, executor_class, process_group_id,
                 hostname):
        self.job_instance_id = job_instance_id
        self.process_group_id = process_group_id
        self.hostname = hostname
        self.requester = Requester(get_the_client_config(), 'jsm')
        self.executor_class = executor_class
        self.executor = executor_class()
        logger.debug("Instantiated JobInstanceIntercom")

    def log_done(self):
        rc, _ = self.requester.send_request(
            app_route='/job_instance/{}/log_done'.format(self.job_instance_id),
            message={},
            request_type='post')
        return rc

    def log_error(self, error_message):
        rc, _ = self.requester.send_request(
            app_route=('/job_instance/{}/log_error'
                       .format(self.job_instance_id)),
            message={'error_message': error_message},
            request_type='post')
        return rc

    def log_job_stats(self):
        try:
            usage = self.executor.get_usage_stats()
            dbukeys = ['usage_str', 'wallclock', 'maxvmem', 'cpu', 'io']
            msg = {k: usage[k] for k in dbukeys if k in usage.keys()}
            rc, _ = self.requester.send_request(
                app_route=('/job_instance/{}/log_usage'
                           .format(self.job_instance_id)),
                message=msg,
                request_type='post')
            return rc
        except NotImplementedError:
            logger.warning("Usage stats not available for {} "
                           "executors".format(self.executor_class))

    def log_running(self):
        rc, _ = self.requester.send_request(
            app_route=('/job_instance/{}/log_running'
                       .format(self.job_instance_id)),
            message={'nodename': socket.gethostname(),
                     'process_group_id': str(self.process_group_id)},
            request_type='post')
        return rc
