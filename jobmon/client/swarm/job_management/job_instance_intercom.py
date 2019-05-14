import logging
import socket
import sys
import traceback

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
            executor_class (Executor): object representing the kind of
            executor that was used for this job instance
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

    def log_done(self, executor_id):
        """Tell the JobStateManager that this job_instance is done"""
        message = {'nodename': socket.getfqdn()}
        if executor_id is not None:
            message['executor_id'] = str(executor_id)
        else:
            logger.info("No Job ID was found in the qsub env at this time")
        rc, _ = self.requester.send_request(
            app_route='/job_instance/{}/log_done'.format(self.job_instance_id),
            message=message,
            request_type='post')
        return rc

    def log_error(self, error_message, executor_id, exit_status, scale=0.5):
        """Tell the JobStateManager that this job_instance has errored"""

        # clip at 10k to avoid mysql has gone away errors when posting long
        # messages
        e_len = len(error_message)
        if e_len >= 10000:
            error_message = error_message[-10000:]
            logger.info(f"Error_message is {e_len} which is more than the 10k "
                        "character limit for error messages. Only the final "
                        "10k will be captured by the database.")

        message = {'error_message': error_message,
                   'exit_status': exit_status,
                   'resource_adjustment': scale,
                   'nodename': socket.getfqdn()}

        if executor_id is not None:
            message['executor_id'] = str(executor_id)
        else:
            logger.info("No Job ID was found in the qsub env at this time")
        rc, _ = self.requester.send_request(
            app_route=('/job_instance/{}/log_error'
                       .format(self.job_instance_id)),
            message=message,
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
        except Exception as e:
            # subprocess.CalledProcessError is raised if qstat fails.
            # Not a critical error, keep running and log an error.
            logger.error(f"Usage stats not available due to exception {e}")
            (e_type, e_value, e_traceback) = sys.exc_info()
            logger.error("Traceback {}".
                         format(print(repr(traceback.format_tb(e_traceback)))))

    def log_running(self, next_report_increment, executor_id):
        """Tell the JobStateManager that this job_instance is running, and
        update the report_by_date to be further in the future in case it gets
        reconciled immediately"""
        message = {'nodename': socket.getfqdn(),
                   'process_group_id': str(self.process_group_id),
                   'next_report_increment': next_report_increment}
        logger.debug(f'executor_id is {executor_id}')
        if executor_id is not None:
            message['executor_id'] = str(executor_id)
        else:
            logger.info("No Job ID was found in the qsub env at this time")
        rc, _ = self.requester.send_request(
            app_route=('/job_instance/{}/log_running'
                       .format(self.job_instance_id)),
            message=message,
            request_type='post')
        return rc

    def log_report_by(self, next_report_increment, executor_id):
        """Log the heartbeat to show that the job instance is still alive"""
        message = {"next_report_increment": next_report_increment}
        if executor_id is not None:
            message['executor_id'] = str(executor_id)
        else:
            logger.info("No Job ID was found in the qsub env at this time")
        rc, _ = self.requester.send_request(
            app_route=(f'/job_instance/{self.job_instance_id}/log_report_by'),
            message=message,
            request_type='post')
        return rc
