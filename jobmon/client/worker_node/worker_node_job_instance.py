import logging
import os
import socket
import traceback

from jobmon.client import shared_requester

logger = logging.getLogger(__name__)


class WorkerNodeJobInstance:

    def __init__(self, job_instance_id, executor_id, executor, nodename=None,
                 process_group_id=None, requester=shared_requester):
        """
        The JobInstanceIntercom is a mechanism whereby a running job_instance
        can communicate back to the JobStateManager to log its status, errors,
        usage details, etc.

        Args:
            job_instance_id (int): the id of the job_instance_id that is
                reporting back
            executor (Executor): instance of executor that was used for this
                job instance
            nodename (str): hostname where this job_instance is running
            process_group_id (int): linux process_group_id that this
                job_instance is a part of
        """
        self.job_instance_id = job_instance_id
        self._executor_id = executor_id
        self._nodename = nodename
        self._process_group_id = process_group_id
        self.executor = executor
        self.requester = shared_requester
        logger.debug("Instantiated JobInstanceIntercom")

    @property
    def executor_id(self):
        if self._executor_id is None:
            self._executor_id = os.environ.get('JOB_ID')
        return self._executor_id

    @property
    def nodename(self):
        if self._nodename is None:
            self._nodename = socket.getfqdn()
        return self._nodename

    @property
    def process_group_id(self):
        if self._process_group_id is None:
            self._process_group_id = os.getpid()
        return self._process_group_id

    def log_done(self):
        """Tell the JobStateManager that this job_instance is done"""
        message = {'nodename': self.nodename}
        if self.executor_id is not None:
            message['executor_id'] = str(self.executor_id)
        else:
            logger.info("No Job ID was found in the qsub env at this time")
        rc, _ = self.requester.send_request(
            app_route=f'/job_instance/{self.job_instance_id}/log_done',
            message=message,
            request_type='post')
        return rc

    def log_error(self, error_message, exit_status, scale=0.5):
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
                   'nodename': self.nodename}

        if self.executor_id is not None:
            message['executor_id'] = str(self.executor_id)
        else:
            logger.info("No Job ID was found in the qsub env at this time")
        rc, _ = self.requester.send_request(
            app_route=f'/job_instance/{self.job_instance_id}/log_error',
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
                app_route=f'/job_instance/{self.job_instance_id}/log_usage',
                message=msg,
                request_type='post')
            return rc
        except NotImplementedError:
            logger.warning("Usage stats not available for "
                           f"{self.executor.__class__.__name__} executors")
        except Exception as e:
            # subprocess.CalledProcessError is raised if qstat fails.
            # Not a critical error, keep running and log an error.
            logger.error(f"Usage stats not available due to exception {e}")
            logger.error(f"Traceback {traceback.format_exc()}")

    def log_running(self, next_report_increment):
        """Tell the JobStateManager that this job_instance is running, and
        update the report_by_date to be further in the future in case it gets
        reconciled immediately"""
        message = {'nodename': self.nodename,
                   'process_group_id': str(self.process_group_id),
                   'next_report_increment': next_report_increment}
        logger.debug(f'executor_id is {self.executor_id}')
        if self.executor_id is not None:
            message['executor_id'] = str(self.executor_id)
        else:
            logger.info("No Job ID was found in the qsub env at this time")
        rc, _ = self.requester.send_request(
            app_route=(f'/job_instance/{self.job_instance_id}/log_running'),
            message=message,
            request_type='post')
        return rc

    def log_report_by(self, next_report_increment):
        """Log the heartbeat to show that the job instance is still alive"""
        message = {"next_report_increment": next_report_increment}
        if self.executor_id is not None:
            message['executor_id'] = str(self.executor_id)
        else:
            logger.info("No Job ID was found in the qsub env at this time")
        rc, _ = self.requester.send_request(
            app_route=(f'/job_instance/{self.job_instance_id}/log_report_by'),
            message=message,
            request_type='post')
        return rc
