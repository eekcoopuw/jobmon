import logging
import socket

from jobmon.client import shared_requester


logger = logging.getLogger(__name__)


class JobInstanceIntercom(object):

    def __init__(self, job_instance_id, executor_class, process_group_id,
                 hostname, requester=shared_requester):
        """
        The JobInstanceIntercom is a mechanism whereby a running job_instance
        can communicate back to the JobStateManager to log its status, errors,
        usage details, etc.

        Args:
            job_instance_id (int): the id of the job_instance_id that is
            reporting back
            executor_class (str): string representing the type of executor that
            was used for this job instance
            process_group_id (int): linux process_group_id that this
            job_instance is a part of
            hostname (str): hostname where this job_instance is running
        """
        self.job_instance_id = job_instance_id
        self.process_group_id = process_group_id
        self.hostname = hostname
        self.requester = shared_requester
        self.executor_class = executor_class
        self.executor = executor_class()
        logger.debug("Instantiated JobInstanceIntercom")

    def log_done(self):
        """Tell the JobStateManager that this job_instance is done"""
        rc, _ = self.requester.send_request(
            app_route='/job_instance/{}/log_done'.format(self.job_instance_id),
            message={},
            request_type='post')
        return rc

    def log_error(self, error_message):
        """Tell the JobStateManager that this job_instance has errored"""
        rc, _ = self.requester.send_request(
            app_route=('/job_instance/{}/log_error'
                       .format(self.job_instance_id)),
            message={'error_message': error_message},
            request_type='post')
        return rc

    def log_job_stats(self):
        """Tell the JobStateManager all the applicable job_stats for this
        job_instance
        """
        try:
            usage = self.executor.get_usage_stats()
            dbukeys = ['usage_str', 'wallclock', 'maxrss', 'cpu', 'io']
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
        """Tell the JobStateManager that this job_instance is running"""
        rc, _ = self.requester.send_request(
            app_route=('/job_instance/{}/log_running'
                       .format(self.job_instance_id)),
            message={'nodename': socket.getfqdn(),
                     'process_group_id': str(self.process_group_id)},
            request_type='post')
        return rc

    def log_report_by(self):
        """Log the heartbeat to show that the job instance is still alive"""
        rc, _ = self.requester.send_request(
            app_route=(f'/job_instance/{self.job_instance_id}/log_report_by'),
            message={},
            request_type='post')
        return rc
