import logging
import socket

from jobmon.requester import Requester


logger = logging.getLogger(__name__)


class JobInstanceIntercom(object):

    def __init__(self, job_instance_id, executor_class, process_group_id,
                 jm_rep_cc=None):
        self.job_instance_id = job_instance_id
        self.process_group_id = process_group_id
        self.requester = Requester(jm_rep_cc)
        self.executor_class = executor_class
        self.executor = executor_class()
        logger.debug("Instantiated JobInstanceIntercom")

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
        try:
            usage = self.executor.get_usage_stats()
            dbukeys = ['usage_str', 'wallclock', 'maxrss', 'cpu', 'io']
            kwargs = {k: usage[k] for k in dbukeys if k in usage.keys()}
            msg = {
                'action': 'log_usage',
                'args': [self.job_instance_id],
                'kwargs': kwargs}
            with open("/homes/mm7148/jobmon/simple-dag-log.txt", 'w') as f:
                f.write("args passed through {kwargs}".format(kwargs=kwargs))
            return self.requester.send_request(msg)
        except NotImplementedError:
            logger.warning("Usage stats not available for {} "
                           "executors".format(self.executor_class))

    def log_running(self):
        return self.requester.send_request({
            'action': 'log_running',
            'kwargs': {'job_instance_id': self.job_instance_id,
                       'nodename': socket.gethostname(),
                       'process_group_id': self.process_group_id}
        })
