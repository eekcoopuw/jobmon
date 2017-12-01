import logging
import os

from jobmon.requester import Requester

if os.getenv("SGE_CLUSTER_NAME"):
    from jobmon import sge


# logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
handler = logging.FileHandler(
    '/ihme/scratch/users/cpinho/tests/jobmon/status_check.log')
f = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(f)
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)


class JobInstanceIntercom(object):

    def __init__(self, job_instance_id, jm_rep_cc=None):
        self.job_instance_id = job_instance_id
        self.requester = Requester(jm_rep_cc)
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
        if os.environ["JOB_ID"]:
            logger.debug("In log_job_stats: JOB_ID is {}"
                         .format(os.environ["JOB_ID"]))
            job_id = os.environ["JOB_ID"]
            self.usage = sge.qstat_usage(
                job_id)
            dbukeys = ['usage_str', 'nodename', 'wallclock', 'maxvmem', 'cpu',
                       'io']
            kwargs = {k: self.usage[k] for k in dbukeys
                      if k in self.usage.keys()}
            msg = {
                'action': 'log_usage',
                'args': [self.job_instance_id],
                'kwargs': kwargs}
            logger.debug("In log_job_stats: msg is {}".format(msg))
            return self.requester.send_request(msg)
        else:
            logger.debug("In log_job_stats: couldn't get JOB_ID")
            return False

    def log_running(self):
        return self.requester.send_request({
            'action': 'log_running',
            'kwargs': {'job_instance_id': self.job_instance_id}
        })
