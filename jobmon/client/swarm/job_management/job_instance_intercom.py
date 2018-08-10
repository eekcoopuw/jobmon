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
        with open("/homes/cpinho/forked_jobmon/jii.txt", "w") as f:
            f.write("made it to JII")

    def log_done(self):
        rc, _ = self.requester.send_request(
            app_route='/log_done',
            message={'job_instance_id': str(self.job_instance_id)},
            request_type='post')
        return rc

    def log_error(self, error_message):
        with open("/homes/cpinho/forked_jobmon/jii.txt", "a") as f:
            f.write("made it to log_error")
        rc, response = self.requester.send_request(
            app_route='/log_error',
            message={'job_instance_id': str(self.job_instance_id),
                     'error_message': error_message},
            request_type='post')
        with open("/homes/cpinho/forked_jobmon/jii.txt", "a") as f:
            f.write("log_error rc is {} and response is {}"
                    .format(rc, response))
        return rc

    def log_job_stats(self):
        with open("/homes/cpinho/forked_jobmon/jii.txt", "a") as f:
            f.write("made it to log_job_stats")
        try:
            usage = self.executor.get_usage_stats()
            dbukeys = ['usage_str', 'wallclock', 'maxvmem', 'cpu', 'io']
            msg = {k: usage[k] for k in dbukeys if k in usage.keys()}
            msg.update({
                'job_instance_id': [self.job_instance_id]})
            rc, _ = self.requester.send_request(app_route='/log_usage',
                                                message=msg,
                                                request_type='post')
            return rc
        except NotImplementedError:
            logger.warning("Usage stats not available for {} "
                           "executors".format(self.executor_class))
            with open("/homes/cpinho/forked_jobmon/jii.txt", "a") as f:
                f.write("caught NotImplemented on log_job_stats")

    def log_running(self):
        with open("/homes/cpinho/forked_jobmon/jii.txt", "a") as f:
            f.write("made it to log_running")
        rc, _ = self.requester.send_request(
            app_route='/log_running',
            message={'job_instance_id': str(self.job_instance_id),
                     'nodename': socket.gethostname(),
                     'process_group_id': str(self.process_group_id)},
            request_type='post')
        with open("/homes/cpinho/forked_jobmon/jii.txt", "a") as f:
            f.write("log_job_stats rc is {}".format(rc))
        return rc
